// Servicio report: consumidor de Kafka + API de solo lectura en el mismo proceso.
//   npm run report
//   GROUP_ID=report-v2 PORT=3011 npm run report      (Parte 3: replay con un grupo nuevo)
//   PROCESO_MS=500 PORT=3011 npm run report          (Parte 6: consumidor lento, 2ª instancia)
const { iniciarConsumidor } = require('../shared/consumidor');
const { crearApiSoloLectura } = require('../shared/http');
const kpis = require('./kpis');

const GROUP_ID = process.env.GROUP_ID || 'report';
const PORT = Number(process.env.PORT) || 0;
const PROCESO_MS = Number(process.env.PROCESO_MS) || 0;

async function procesar(evento) {
  // Falla simulada para mostrar reintentos + DLT (npm run producer -- --falla)
  if (evento.clienteId === 'C-FALLA') throw new Error('error simulado en report');
  // Parte 6: simula una consulta lenta a la base de datos
  if (PROCESO_MS) await new Promise(r => setTimeout(r, PROCESO_MS));

  kpis.aplicar(evento);
  const k = kpis.obtener();
  console.log('   ventas por hora:', k.ventasPorHora, '| por estado:', k.pedidosPorEstado,
    '| top:', k.topProductos, '| lead time prom:', k.leadTimePromedioSeg.toFixed(1), 's');
}

// Parte 5: los endpoints solo leen lo que ya calculó kpis (nada de lógica nueva aquí)
const app = crearApiSoloLectura();
app.get('/api/report/ventas-por-hora', (req, res) => res.json(kpis.obtener().ventasPorHora));
app.get('/api/report/lead-time', (req, res) => {
  const k = kpis.obtener();
  res.json({ promedioSeg: k.leadTimePromedioSeg, pedidos: k.leadTimes });
});
app.get('/api/report/top-productos', (req, res) => res.json(kpis.obtener().topProductos));
app.get('/api/report/estados', (req, res) => res.json(kpis.obtener().pedidosPorEstado));
const server = app.listen(PORT, () => {
  const puertoReal = server.address().port;
  console.log(`[${GROUP_ID}] API en http://localhost:${puertoReal}/api/report`);
});

iniciarConsumidor({
  clientId: 'report-service',
  groupId: GROUP_ID,
  procesar,
  fromBeginning: process.env.FROM_BEGINNING !== 'false',
}).catch(e => { console.error(e); process.exit(1); });
