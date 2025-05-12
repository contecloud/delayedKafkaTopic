const { Kafka, logLevel, Partitioners } = require('kafkajs')

const kafka = new Kafka({
    logLevel: logLevel.INFO,
    brokers: ['localhost:9092'],
    clientId: 'example-consumer',
})

const topic = 'topic-test'
const consumer = kafka.consumer({ groupId: 'test-group' })
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner })

const sendMessage = (message, partition, consumer) => {
    console.log("Enviando novamente para a fila");

    return producer.send({
        topic,
        messages: [
            { value: message }
        ]
    })

    consumer.commitOffsets([
        { topic: 'topic-test', partition, offset: message.offset }
    ])
}

const run = async () => {
    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: true })
    await consumer.run({
        autoCommit: false,
        eachMessage: async ({ topic, partition, message }) => {
            const jsonMessage = JSON.parse(message.value.toString())
            console.log(new Date(), jsonMessage)
            if (jsonMessage.valor <= 3) {
                jsonMessage.valor++
                const message = JSON.stringify(jsonMessage)
                await producer.connect()
                setTimeout(sendMessage(message, partition, consumer), 5000)
            }
        },
    })
}

run().catch(e => console.error(`[example/consumer] ${e.message}`, e))