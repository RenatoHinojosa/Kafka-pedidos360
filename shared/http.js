// Parte 5: API HTTP de solo lectura (infraestructura, igual que consumidor.js).
// Cada servicio agrega sus rutas GET; aquí van CORS y el 405 para todo lo que no sea lectura.
const express = require('express');
const cors = require('cors');

const ORIGEN_FRONTEND = process.env.FRONTEND_ORIGIN || 'http://localhost:4200';

function crearApiSoloLectura() {
  const app = express();
  app.use(cors({ origin: ORIGEN_FRONTEND }));   // sin esto el navegador bloquea las llamadas
  app.use((req, res, next) => {
    if (['GET', 'HEAD', 'OPTIONS'].includes(req.method)) return next();
    res.set('Allow', 'GET').status(405).json({ error: 'Method Not Allowed' });
  });
  return app;
}

module.exports = { crearApiSoloLectura };
