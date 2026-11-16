// Prueba la lógica de negocio SIN Kafka:  npm test
const test = require('node:test');
const assert = require('node:assert');
const { crearEvento, validar } = require('../shared/dto');
const { transicionValida } = require('../shared/estados');
const kpis = require('../report/kpis');

const ana = { usuario: 'op-ana', rol: 'OPERADOR' };
const ev = (tipo, extra = {}) => crearEvento({ tipo, pedidoId: 'P-1', total: 19970, actor: ana, origen: 'web', ...extra });
const items = [{ productoId: 'PROD-1', cantidad: 2, precio: 8990 }, { productoId: 'PROD-2', cantidad: 1, precio: 1990 }];

test('Parte 1: validar evento v2', () => {
  assert.deepStrictEqual(validar(ev('PEDIDO_CREADO', { items })), []);
  assert.deepStrictEqual(validar(ev('PEDIDO_EN_PREPARACION')), []);
  assert.match(validar(ev('PEDIDO_CREADO', { items, total: 1 })).join(), /no cuadra/);
  assert.match(validar(ev('PEDIDO_CREADO', { items: [] })).join(), /items vacio/);
  assert.match(validar(ev('PEDIDO_CREADO', { items: [{ ...items[0], cantidad: 0 }], total: 0 })).join(), /cantidad/);
  assert.match(validar(ev('PEDIDO_ACEPTADO', { actor: { usuario: 'x', rol: 'JEFE' } })).join(), /actor/);
  const v1 = ev('PEDIDO_ACEPTADO'); delete v1.version; delete v1.origen;
  assert.strictEqual(validar(v1).length, 2);
});

test('Parte 2: transiciones', () => {
  assert.ok(transicionValida(undefined, 'PEDIDO_CREADO'));
  assert.ok(!transicionValida(undefined, 'PEDIDO_ACEPTADO'));
  assert.ok(transicionValida('PEDIDO_EN_PREPARACION', 'PEDIDO_CANCELADO'));
  assert.ok(!transicionValida('PEDIDO_CREADO', 'PEDIDO_DESPACHADO'));
  assert.ok(!transicionValida('PEDIDO_ENTREGADO', 'PEDIDO_CANCELADO'));
});

test('Parte 3: KPIs, idempotencia y salto', () => {
  const t0 = '2026-11-04T15:00:00.000Z';
  const creado = ev('PEDIDO_CREADO', { items, fecha: t0 });
  kpis.aplicar(creado);
  kpis.aplicar(creado);                                   // duplicado: no cuenta 2 veces
  for (const tipo of ['PEDIDO_ACEPTADO', 'PEDIDO_EN_PREPARACION', 'PEDIDO_DESPACHADO'])
    kpis.aplicar(ev(tipo, { fecha: t0 }));
  kpis.aplicar(ev('PEDIDO_ENTREGADO', { fecha: '2026-11-04T15:00:30.000Z' }));

  kpis.aplicar(ev('PEDIDO_CREADO', { pedidoId: 'P-2', items: [items[1]], total: 1990, fecha: t0 }));
  kpis.aplicar(ev('PEDIDO_CANCELADO', { pedidoId: 'P-2', total: 1990 }));

  kpis.aplicar(ev('PEDIDO_CREADO', { pedidoId: 'P-3', items: [items[1]], total: 1990, fecha: t0 }));
  assert.throws(() => kpis.aplicar(ev('PEDIDO_DESPACHADO', { pedidoId: 'P-3' })),
    e => e.reintentable === false && /PEDIDO_CREADO -> PEDIDO_DESPACHADO/.test(e.message));

  const k = kpis.obtener();
  assert.deepStrictEqual(k.ventasPorHora, [{ hora: '2026-11-04T15', total: 19970 + 1990 }]);  // P-2 restado
  assert.strictEqual(k.leadTimePromedioSeg, 30);
  assert.deepStrictEqual(k.pedidosPorEstado, [{ estado: 'PEDIDO_CREADO', cantidad: 1 }]);    // solo P-3 activo
  assert.deepStrictEqual(k.topProductos[0], { productoId: 'PROD-2', unidades: 3 });
});
