// Lógica de negocio de report: el consumidor llama a aplicar(evento), la API llama a obtener().
// No sabe nada de Kafka ni de HTTP.
const { transicionValida } = require('../shared/estados');

const FINALES = ['PEDIDO_ENTREGADO', 'PEDIDO_CANCELADO'];
const IDEMPOTENTE = process.env.IDEMPOTENCIA !== 'off';   // IDEMPOTENCIA=off solo para el experimento

const vistos = new Set();       // eventId ya aplicados (Kafka es at-least-once: pueden llegar repetidos)
const estadoActual = {};        // pedidoId -> último tipo
const creado = {};              // pedidoId -> { fecha, hora, total }
const ventasPorHora = {};       // 'YYYY-MM-DDTHH' -> total
const leadTimes = {};           // pedidoId -> segundos (CREADO -> ENTREGADO)
const unidades = {};            // productoId -> unidades vendidas

function aplicar(e) {
  if (IDEMPOTENTE && vistos.has(e.eventId)) {
    console.log(`   duplicado ignorado ${e.eventId}`);
    return;
  }

  // Parte 2: revisar la transición ANTES de tocar los KPIs.
  // Repetir el mismo estado no es un salto (es un duplicado; de eso se encarga el Set).
  const anterior = estadoActual[e.pedidoId];
  if (anterior !== e.tipo && !transicionValida(anterior, e.tipo)) {
    const err = new Error(`transicion invalida ${anterior || '(ninguno)'} -> ${e.tipo}`);
    err.reintentable = false;
    throw err;
  }

  estadoActual[e.pedidoId] = e.tipo;

  if (e.tipo === 'PEDIDO_CREADO') {
    const hora = e.fecha.slice(0, 13);
    ventasPorHora[hora] = (ventasPorHora[hora] || 0) + e.total;
    creado[e.pedidoId] = { fecha: e.fecha, hora, total: e.total };
    for (const i of e.items) unidades[i.productoId] = (unidades[i.productoId] || 0) + i.cantidad;
  }

  if (e.tipo === 'PEDIDO_CANCELADO' && creado[e.pedidoId]) {
    const c = creado[e.pedidoId];
    ventasPorHora[c.hora] -= c.total;   // se resta en la hora en que se sumó
  }

  if (e.tipo === 'PEDIDO_ENTREGADO' && creado[e.pedidoId]) {
    leadTimes[e.pedidoId] = (new Date(e.fecha) - new Date(creado[e.pedidoId].fecha)) / 1000;
  }

  vistos.add(e.eventId);
}

function obtener() {
  const lt = Object.entries(leadTimes).map(([pedidoId, segundos]) => ({ pedidoId, segundos }));

  const porEstado = {};
  for (const tipo of Object.values(estadoActual)) {
    if (!FINALES.includes(tipo)) porEstado[tipo] = (porEstado[tipo] || 0) + 1;
  }

  return {
    ventasPorHora: Object.entries(ventasPorHora).sort().map(([hora, total]) => ({ hora, total })),
    leadTimePromedioSeg: lt.length ? lt.reduce((s, p) => s + p.segundos, 0) / lt.length : 0,
    leadTimes: lt,
    pedidosPorEstado: Object.entries(porEstado).map(([estado, cantidad]) => ({ estado, cantidad })),
    topProductos: Object.entries(unidades)
      .map(([productoId, u]) => ({ productoId, unidades: u }))
      .sort((a, b) => b.unidades - a.unidades)
      .slice(0, 3),
  };
}

module.exports = { aplicar, obtener };
