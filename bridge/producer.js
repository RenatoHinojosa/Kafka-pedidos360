// Productor simulado (mock) de ms-pedidos360-orders. Publica el evento v2 tal como lo hará el servicio real:
// mismos campos, misma key (pedidoId) y mismo header (correlationId).
//   npm run producer                 -> 10 eventos (2 pedidos que recorren los 5 estados)
//   npm run producer -- 15           -> 15 eventos (3 pedidos)
//   npm run producer -- 12 1500      -> 12 eventos, uno cada 1,5 s (para verlos en vivo)
//   npm run producer -- --malo       -> JSON roto + evento sin pedidoId (terminan en la DLT)
//   npm run producer -- --falla      -> evento válido que report no puede procesar (reintentos + DLT)
//   npm run producer -- --total-malo -> PEDIDO_CREADO cuyo total no cuadra con los items (DLT)
//   npm run producer -- --salto      -> P-2001 pasa de CREADO directo a DESPACHADO (DLT de report)
//   npm run producer -- --duplicado  -> el mismo PEDIDO_CREADO 2 veces con el mismo eventId
//   npm run producer -- 15 --key-eventid  -> experimento Parte 2: key = eventId en vez de pedidoId
const crypto = require('crypto');
const { Partitioners } = require('kafkajs');
const { crearKafka } = require('../shared/kafka');
const { crearEvento, validar } = require('../shared/dto');
const { PEDIDOS } = require('../shared/topics');

const args = process.argv.slice(2);
const flag = f => args.includes(f);
const [n = 10, pausa = 0] = args.filter(a => !a.startsWith('--')).map(Number);   // pausa en ms
const KEY_EVENTID = flag('--key-eventid');

const kafka = crearKafka('bridge');
const producer = kafka.producer({
  idempotent: true,        // sin duplicados por red
  maxInFlightRequests: 1,
  createPartitioner: Partitioners.DefaultPartitioner,
});

// Catálogo de prueba (en el proyecto final los productos vienen de catalog)
const CATALOGO = { 'PROD-1': 8990, 'PROD-2': 1990, 'PROD-3': 3490 };
const FLUJO = ['PEDIDO_CREADO', 'PEDIDO_ACEPTADO', 'PEDIDO_EN_PREPARACION', 'PEDIDO_DESPACHADO', 'PEDIDO_ENTREGADO'];

// El CLIENTE crea el pedido; el OPERADOR lo acepta, prepara, despacha y entrega
const CLIENTE = j => ({ usuario: `cliente-${j + 1}`, rol: 'CLIENTE' });
const OPERADOR = { usuario: 'op-ana', rol: 'OPERADOR' };

function pedidoDemo(pedidoId, j) {
  const items = [
    { productoId: 'PROD-1', cantidad: 1 + (j % 2), precio: CATALOGO['PROD-1'] },
    { productoId: j % 3 ? 'PROD-2' : 'PROD-3', cantidad: 1 + (j % 3), precio: CATALOGO[j % 3 ? 'PROD-2' : 'PROD-3'] },
  ];
  const total = items.reduce((s, i) => s + i.cantidad * i.precio, 0);
  return { pedidoId, clienteId: `C-${j + 1}`, total, items, j };
}

// Arma el evento según el estado: items solo en CREADO, actor y origen cambian según quién actúa
function datosEvento(p, tipo) {
  const esCliente = tipo === 'PEDIDO_CREADO';
  return {
    tipo, pedidoId: p.pedidoId, clienteId: p.clienteId, total: p.total,
    items: esCliente ? p.items : undefined,
    actor: esCliente ? CLIENTE(p.j) : OPERADOR,
    origen: esCliente ? 'web' : 'backoffice',
  };
}

async function publicar(datos) {
  const evento = crearEvento(datos);
  const errores = validar(evento);
  if (errores.length) throw new Error(errores.join());
  await enviar(evento);
}

// Envía sin validar: solo para provocar errores en los consumidores
async function enviar(evento, key = KEY_EVENTID ? evento.eventId : evento.pedidoId) {
  const value = typeof evento === 'string' ? evento : JSON.stringify(evento);
  const [r] = await producer.send({
    topic: PEDIDOS,
    acks: -1,                // all
    messages: [{ key, value, headers: { correlationId: crypto.randomUUID() } }],
  });
  console.log(`-> p${r.partition} off${r.baseOffset}  key=${key}  ${evento.tipo || value}`);
}

// Los ids siguen la cantidad de eventos ya publicados: con el tópico vacío parten en P-1001,
// y si vuelves a correr el productor salen pedidos nuevos (no se repite un pedido ya ENTREGADO).
async function primerNumeroLibre() {
  const admin = kafka.admin();
  await admin.connect();
  const offsets = await admin.fetchTopicOffsets(PEDIDOS);
  await admin.disconnect();
  return 1001 + offsets.reduce((s, p) => s + Number(p.high), 0);
}

async function main() {
  await producer.connect();
  const base = await primerNumeroLibre();

  if (flag('--malo')) {
    await enviar('{ esto no es JSON', 'P-ROTO');
    await enviar(crearEvento({ tipo: 'PEDIDO_CREADO', clienteId: 'C-9', total: 1000 }), 'SIN-ID');
  } else if (flag('--falla')) {
    await publicar({ ...datosEvento(pedidoDemo('P-9999', 0), 'PEDIDO_CREADO'), clienteId: 'C-FALLA' });
  } else if (flag('--total-malo')) {
    const p = pedidoDemo(`P-${base}`, 0);
    await enviar(crearEvento({ ...datosEvento(p, 'PEDIDO_CREADO'), total: p.total + 1000 }));
  } else if (flag('--salto')) {
    const p = pedidoDemo('P-2001', 0);
    await publicar(datosEvento(p, 'PEDIDO_CREADO'));
    await publicar(datosEvento(p, 'PEDIDO_DESPACHADO'));   // se salta ACEPTADO y EN_PREPARACION
  } else if (flag('--duplicado')) {
    const evento = crearEvento(datosEvento(pedidoDemo(`P-${base}`, 0), 'PEDIDO_CREADO'));
    await enviar(evento);
    await enviar(evento);    // mismo eventId: como si el productor reintentara tras un corte de red
  } else {
    // k pedidos que avanzan intercalados: P-1001 CREADO, P-1002 CREADO, P-1001 ACEPTADO, ...
    const k = Math.ceil(n / FLUJO.length);   // con 60 eventos -> 12 pedidos distintos
    const pedidos = Array.from({ length: k }, (_, j) => pedidoDemo(`P-${base + j}`, j));
    for (let i = 0; i < n; i++) {
      await publicar(datosEvento(pedidos[i % k], FLUJO[Math.floor(i / k)]));
      if (pausa) await new Promise(r => setTimeout(r, pausa));
    }
  }

  await producer.disconnect();
}

main().catch(e => { console.error(e.message); process.exit(1); });
