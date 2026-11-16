// Nombres y configuración de los tópicos (la topología del caso Pedidos360)
const PEDIDOS = 'pedidos.eventos';
const TIMELINE = 'audit.timeline';

// Cada consumidor tiene su propia DLT: pedidos.eventos.<groupId>.dlt
const dltDe = groupId => `${PEDIDOS}.${groupId}.dlt`;

const TOPICOS = [
  // 3 carriles para escalar; la key (pedidoId) mantiene el orden de cada pedido
  { topic: PEDIDOS, particiones: 3, retencionDias: 7 },
  // Una DLT por consumidor: se sabe quién falló. Se guarda más tiempo para investigar
  { topic: dltDe('report'), particiones: 3, retencionDias: 14 },
  { topic: dltDe('audit'), particiones: 3, retencionDias: 14 },
  // Historial legible: compact (último por key) + delete (borra lo viejo a los 30 días)
  { topic: TIMELINE, particiones: 3, retencionDias: 30, cleanupPolicy: 'compact,delete' },
];

module.exports = { PEDIDOS, TIMELINE, dltDe, TOPICOS };
