
function definicionTopico({
  topic,
  particiones,
  retencionDias,
  cleanupPolicy = 'delete'
}) {
  return {
    topic,
    numPartitions: particiones,
    replicationFactor: 3,
    configEntries: [
      { name: 'cleanup.policy', value: cleanupPolicy },
      {
        name: 'retention.ms',
        value: String(retencionDias * 86400000)
      },
      { name: 'min.insync.replicas', value: '2' }
    ],
  };
}

module.exports = { definicionTopico };
