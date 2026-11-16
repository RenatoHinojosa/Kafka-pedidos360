// Servicio audit (groupId = audit)
// Recibe los MISMOS eventos que report, con su propio offset.
// Guarda cada evento en una "tabla" (en memoria), publica un registro legible en audit.timeline
// y expone el historial por HTTP (solo lectura) en el puerto 3002.
const { Partitioners } = require('kafkajs');
const { iniciarConsumidor } = require('../shared/consumidor');
const { crearApiSoloLectura } = require('../shared/http');
const { crearKafka } = require('../shared/kafka');
const { TIMELINE } = require('../shared/topics');

const PORT = Number(process.env.PORT) || 3002;
const tabla = [];              // filas de auditoría (solo lectura / append)
const vistos = new Set();      // eventId ya guardados -> idempotencia

// Publicar en audit.timeline es una decisión de negocio de audit: por eso el productor vive aquí y no en shared/
const timeline = crearKafka('audit-timeline').producer({ createPartitioner: Partitioners.DefaultPartitioner });

const fila = e => ({
  pedidoId: e.pedidoId,
  fecha: e.fecha,
  tipo: e.tipo,
  quien: `${e.actor.usuario} (${e.actor.rol})`,
  desdeDonde: e.origen,
});

async function procesar(evento) {
  if (vistos.has(evento.eventId)) return;   // duplicado: se ignora

  // quién / qué / cuándo / desde dónde. Se publica antes de marcarlo como visto:
  // si Kafka falla, el reintento vuelve a intentarlo en vez de saltárselo.
  const f = fila(evento);
  await timeline.send({
    topic: TIMELINE,
    messages: [{
      key: evento.pedidoId,
      value: JSON.stringify({ pedidoId: f.pedidoId, quien: f.quien, que: f.tipo,
        cuando: f.fecha, desdeDonde: f.desdeDonde, eventId: evento.eventId }),
    }],
  });

  vistos.add(evento.eventId);
  tabla.push(evento);

  const linea = tabla
    .filter(e => e.pedidoId === evento.pedidoId)
    .map(e => `${e.fecha.slice(11, 19)} ${e.tipo} [${e.actor.usuario}]`);
  console.log(`   timeline ${evento.pedidoId}: ${linea.join(' -> ')}`);
}

// Parte 5: endpoints de solo lectura
const app = crearApiSoloLectura();

// GET /api/audit?usuario=op-ana&tipo=PEDIDO_ACEPTADO&desde=2026-11-04T00:00&hasta=2026-11-05
app.get('/api/audit', (req, res) => {
  const { usuario, tipo, desde, hasta } = req.query;
  res.json(tabla
    .filter(e => !usuario || e.actor.usuario === usuario)
    .filter(e => !tipo || e.tipo === tipo)
    .filter(e => !desde || new Date(e.fecha) >= new Date(desde))
    .filter(e => !hasta || new Date(e.fecha) <= new Date(hasta))
    .map(fila));
});

// GET /api/audit/P-1001 -> timeline del pedido ordenado por fecha
app.get('/api/audit/:pedidoId', (req, res) => {
  res.json(tabla
    .filter(e => e.pedidoId === req.params.pedidoId)
    .sort((a, b) => a.fecha.localeCompare(b.fecha))
    .map(fila));
});

app.listen(PORT, () => console.log(`[audit] API en http://localhost:${PORT}/api/audit`));

timeline.connect()
  .then(() => iniciarConsumidor({ clientId: 'audit-service', groupId: 'audit', procesar }))
  .catch(e => { console.error(e); process.exit(1); });
