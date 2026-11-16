// Parte 2: regla de transiciones del caso ("no se puede despachar sin aceptar")
const SIGUIENTES = {
  undefined: ['PEDIDO_CREADO'],   // (ninguno): el pedido aún no existe
  PEDIDO_CREADO: ['PEDIDO_ACEPTADO', 'PEDIDO_CANCELADO'],
  PEDIDO_ACEPTADO: ['PEDIDO_EN_PREPARACION', 'PEDIDO_CANCELADO'],
  PEDIDO_EN_PREPARACION: ['PEDIDO_DESPACHADO', 'PEDIDO_CANCELADO'],
  PEDIDO_DESPACHADO: ['PEDIDO_ENTREGADO'],
  PEDIDO_ENTREGADO: [],           // estado final
  PEDIDO_CANCELADO: [],           // estado final
};

function transicionValida(anterior, nuevo) {
  return (SIGUIENTES[anterior] || []).includes(nuevo);
}

module.exports = { transicionValida };
