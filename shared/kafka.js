const { Kafka, logLevel } = require('kafkajs');

const brokers = (
  process.env.KAFKA_BROKERS ||
  'localhost:9092,localhost:9093,localhost:9094'
).split(',');

function crearKafka(clientId) {
  return new Kafka({
    clientId,
    brokers,
    logLevel: logLevel.WARN,
    retry: {
      initialRetryTime: 300,
      retries: 8,
    },
  });
}

module.exports = { crearKafka };
