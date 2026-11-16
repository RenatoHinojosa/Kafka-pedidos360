const crypto = require('crypto');

// Parte 1: se agrega PEDIDO_EN_PREPARACION
const TIPOS = ['PEDIDO_CREADO', 'PEDIDO_ACEPTADO', 'PEDIDO_EN_PREPARACION',
  'PEDIDO_DESPACHADO', 'PEDIDO_ENTREGADO', 'PEDIDO_CANCELADO'];

const ROLES = ['CLIENTE', 'OPERADOR', 'ADMIN'];

// Evento v2: además de lo de v1 lleva items (qué), actor (quién) y origen (desde dónde)
function crearEvento(d) {
  return {
    version: 2,
    eventId: d.eventId || crypto.randomUUID(),
    tipo: d.tipo,
    pedidoId: d.pedidoId,
    clienteId: d.clienteId,
    total: d.total,
    items: d.items,          // [{ productoId, cantidad, precio }] (obligatorio en PEDIDO_CREADO)
    actor: d.actor,          // { usuario, rol }
    origen: d.origen,        // 'web', 'backoffice', ...
    fecha: d.fecha || new Date().toISOString(),
  };
}

function validar(e) {
  const err = [];
  if (e.version !== 2) err.push('version 2 requerida');
  if (!e.eventId) err.push('eventId requerido');
  if (!TIPOS.includes(e.tipo)) err.push('tipo invalido');
  if (!e.pedidoId) err.push('pedidoId requerido');
  if (typeof e.total !== 'number') err.push('total');
  if (!e.actor || !e.actor.usuario || !ROLES.includes(e.actor.rol)) err.push('actor invalido (usuario + rol CLIENTE/OPERADOR/ADMIN)');
  if (!e.origen) err.push('origen requerido');

  if (e.tipo === 'PEDIDO_CREADO') {
    if (!Array.isArray(e.items) || e.items.length === 0) err.push('items vacio');
    else {
      if (e.items.some(i => !(i.cantidad > 0))) err.push('cantidad debe ser > 0');
      const suma = e.items.reduce((s, i) => s + i.cantidad * i.precio, 0);
      if (suma !== e.total) err.push(`total ${e.total} no cuadra con items (${suma})`);
    }
  }
  return err;   // [] = valido
}

module.exports = { TIPOS, ROLES, crearEvento, validar };
