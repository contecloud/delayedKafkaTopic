const { Kafka, logLevel, Partitioners } = require('kafkajs')

const kafka = new Kafka({
    logLevel: logLevel.INFO,
    brokers: ['localhost:9092'],
    clientId: 'example-consumer',
  })

const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner })

const jsonMessage = JSON.stringify({ valor: 1 })

const run = async () => {
    await producer.connect()
    await producer.send({
      topic: 'topic-test',
      messages: [
        { 
            value: jsonMessage
        },
      ],
    })
    await producer.disconnect()
}

run().catch(e => console.error(`[example/consumer] ${e.message}`, e))
