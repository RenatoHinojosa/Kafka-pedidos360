
const net = require('net');

const NODOS = [
  { nombre: 'zookeeper-1', puerto: 2181 },
  { nombre: 'zookeeper-2', puerto: 2182 },
  { nombre: 'zookeeper-3', puerto: 2183 },
];

function srvr(puerto) {
  return new Promise((resolve) => {
    let salida = '';

    const s = net.connect(
      { host: 'localhost', port: puerto },
      () => s.write('srvr')
    );

    s.setTimeout(1500, () => {
      s.destroy();
      resolve(null);
    });

    s.on('data', (d) => {
      salida += d;
    });

    s.on('end', () => resolve(salida));
    s.on('error', () => resolve(null));
  });
}

function parsear(texto) {
  if (!texto) {
    return { modo: 'CAÍDO / sin respuesta' };
  }

  if (texto.includes('not currently serving')) {
    return { modo: 'SIN QUÓRUM' };
  }

  const campo = (k) =>
    (texto.match(new RegExp(`${k}: (.*)`)) || [])[1];

  return {
    modo: (campo('Mode') || '?').toUpperCase(),
    zxid: campo('Zxid'),
    znodes: campo('Node count'),
    conexiones: campo('Connections'),
  };
}

async function ciclo() {
  const filas = await Promise.all(
    NODOS.map(async (n) => ({
      nodo: n.nombre,
      ...parsear(await srvr(n.puerto)),
    }))
  );

  const vivos = filas.filter(
    (f) => f.modo === 'LEADER' || f.modo === 'FOLLOWER'
  ).length;

  console.clear();

  console.log(
    `Ensamble ZooKeeper · ${new Date().toLocaleTimeString()}`
  );

  console.table(filas);

  console.log(
    `Nodos sirviendo: ${vivos}/3 · Quórum necesario: 2 · ${
      vivos >= 2 ? 'HAY QUÓRUM' : 'SIN QUÓRUM'
    }`
  );

  console.log(
    '\nzxid = identificador de transacción: epoch + contador.'
  );
}

async function iniciar() {
  await ciclo();
  setInterval(ciclo, 2000);
}

iniciar();
