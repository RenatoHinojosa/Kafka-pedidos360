# Pedidos360 + Kafka (DSY1107 · EA3)

Ejercicio EA3 · Pedidos360 v2: evento v2 (items, actor, origen, EN_PREPARACION), regla de transiciones,
KPIs completos con idempotencia y replay, DLT por consumidor, `audit.timeline` y 6 endpoints de solo lectura.

```
bridge/producer.js ──► pedidos.eventos (3 particiones, 7 días, key = pedidoId)
   (mock de orders)        ├──► report  (groupId = report) ──► GET :3001/api/report/*   error ─► pedidos.eventos.report.dlt
                           ├──► report-v2 (solo experimento de replay, Parte 3)
                           └──► audit   (groupId = audit)  ──► GET :3002/api/audit/*    error ─► pedidos.eventos.audit.dlt
                                     └──► audit.timeline (compact,delete · 30 días)
```

## Estructura

```
kafka/
├─ docker-compose.yml     <- ZooKeeper + broker + Kafka UI
├─ shared/
│   ├─ kafka.js           <- cliente (KAFKA_BROKERS)
│   ├─ topics.js          <- nombres/config de tópicos
│   ├─ dto.js             <- evento v2 + validación
│   ├─ estados.js         <- regla de transiciones
│   ├─ consumidor.js      <- parseo, validación, reintentos y DLT por grupo (infraestructura)
│   └─ http.js            <- API de solo lectura: CORS + 405 (infraestructura)
├─ bridge/producer.js     <- mock de ms-pedidos360-orders
├─ report/kpis.js         <- KPIs: aplicar(evento) / obtener()
├─ report/consumer.js     <- consumidor + endpoints /api/report/* (puerto 3001)
├─ audit/consumer.js      <- tabla + audit.timeline + endpoints /api/audit/* (puerto 3002)
├─ frontend-prueba/       <- página simple por si no tienen el frontend Angular a mano
├─ test/logica.test.js    <- npm test (lógica sin Kafka)
└─ admin/
    ├─ setup.js           <- crea los tópicos de topics.js
    └─ server.js          <- API para tópicos y grupos
```

## Paso a paso

Requisitos: Docker Desktop y Node 18+.

```bash
npm install
docker compose up -d        # o: npm run up
docker compose ps           # los 3 contenedores en "running"
npm run topics              # crea los 4 tópicos del caso
```

Abre Kafka UI en http://localhost:8080.

Luego, cada uno en su propia terminal:

```bash
npm run report              # terminal 1
npm run audit               # terminal 2
npm run producer            # terminal 3: publica 10 eventos
npm run producer -- 12 1500 # 12 eventos, uno cada 1,5 s (ideal para la demo en vivo)
```

Opciones del productor: `--malo`, `--falla`, `--total-malo`, `--salto`, `--duplicado`, `--key-eventid`.
Variables de report: `PORT` (3001), `GROUP_ID` (report), `FROM_BEGINNING` (true), `PROCESO_MS` (0), `IDEMPOTENCIA=off`.

Mensajes esperados (no son errores del ejemplo):
- `The group coordinator is not available`: solo la primera vez, justo después de levantar Kafka; se resuelve solo.
- `Limiting retries for the idempotent producer...`: aviso de KafkaJS por usar `idempotent: true` junto con `retries: 8`.

## Experimentos (Paso 6)

| # | Haz esto | Deberías ver |
|---|----------|--------------|
| 1 | `npm run producer` (10 eventos, 3 pedidoId) | En Kafka UI › Topics › pedidos.eventos › Messages: el mismo pedidoId siempre en la misma partición |
| 2 | Abre `npm run report` en **2 terminales** | Cada una muestra `particiones asignadas: [...]` distintas. Cierra una (Ctrl+C): la otra toma todas (rebalanceo) |
| 3 | `npm run audit` | Recibe TODOS los eventos aunque report ya los haya leído (otro grupo, otro offset) |
| 4 | Detén report, `npm run producer -- 20`, revisa el lag | Lag = 20 (Kafka UI › Consumers, o el admin). Al levantar report, baja a 0 |
| 5 | `npm run producer -- --malo` | JSON roto y evento sin pedidoId aparecen en `pedidos.eventos.report.dlt` y `pedidos.eventos.audit.dlt` con los headers `motivo` y `consumidor`; el resto sigue normal |
| 6 | `npm run producer -- --falla` | report reintenta 3 veces (1s, 2s, 4s) y lo manda a su DLT; audit lo procesa sin problema |

Ver el lag desde el broker:

```bash
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group report
```

## Servicio administrador

```bash
npm run admin               # http://localhost:3000
```

```bash
curl localhost:3000/admin/topics
curl -X POST localhost:3000/admin/topics -H 'Content-Type: application/json' \
     -d '{"topic":"pedidos.prueba","particiones":2,"retencionDias":1}'
curl -X PUT localhost:3000/admin/topics/pedidos.prueba -H 'Content-Type: application/json' \
     -d '{"retencionDias":3}'
curl -X DELETE localhost:3000/admin/topics/pedidos.prueba
curl localhost:3000/admin/groups
curl localhost:3000/admin/groups/report/lag
```

## Volver a empezar

```bash
docker compose down         # borra los contenedores (y los eventos)
```

Para la EC2 solo cambia la variable: `KAFKA_BROKERS=<ip-privada>:9092 npm run report`.
