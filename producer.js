const { Kafka, logLevel, Partitioners } = require('kafkajs')

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: ['localhost:9092'],
  clientId: 'example-consumer',
})

const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner })

const run = async (mensagem) => {
  await producer.connect()
  await producer.send({
    topic: 'topic-test',
    messages: [
      {
        value: mensagem
      },
    ],
  })
  await producer.disconnect()
}

for (let i = 1; i < 5; i++) {
  setTimeout(() => {
    const mensagem = JSON.stringify({ mensagem: i, valor: 1 });
    run(mensagem).catch(e => console.error(`[example/consumer] ${e.message}`, e))
  }, i * 1000)
}
