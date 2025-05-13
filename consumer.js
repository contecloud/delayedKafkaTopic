const { Kafka, logLevel, Partitioners } = require('kafkajs')

const kafka = new Kafka({
    logLevel: logLevel.INFO,
    brokers: ['localhost:9092'],
    clientId: 'example-consumer',
})

const topic = 'topic-test'
const delayedTopic = 'delayed-topic'
const consumer = kafka.consumer({ groupId: 'test-group' })
const consumerDelayed = kafka.consumer({ groupId: 'delayed-group' })
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner })

const sendMessage = async (message, topic) => {
    console.log(`Enviando para a fila ${topic}`);
    await producer.connect()
    await producer.send({
        topic,
        messages: [
            { value: message }
        ]
    })
    await producer.disconnect()
}

const runPrincipalTopic = async () => {
    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: true })
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const jsonMessage = JSON.parse(message.value.toString())
            console.log(new Date(), jsonMessage)
            if (jsonMessage.valor <= 3) {
                jsonMessage.valor++
                const message = JSON.stringify(jsonMessage)
                sendMessage(message, delayedTopic)
            }
        },
    })
}

const runDelayedTopic = async () => {
    await consumerDelayed.connect()
    await consumerDelayed.subscribe({ topic: delayedTopic, fromBeginning: true })
    await consumerDelayed.run({
        eachBatch: async ({ batch }) => {
            consumerDelayed.pause([{ topic: delayedTopic }])
            batch.messages.forEach((message) => {
                sendMessage(message.value, topic)
            })
            setTimeout(() => consumerDelayed.resume([{ topic: delayedTopic }]), 5000);
        },
    })
}

runPrincipalTopic().catch(e => console.error(`[example/consumer] ${e.message}`, e))
runDelayedTopic().catch(e => console.error(`[example/consumer] ${e.message}`, e))