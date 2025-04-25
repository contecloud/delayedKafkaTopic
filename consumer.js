const { Kafka, logLevel, Partitioners } = require('kafkajs')

const kafka = new Kafka({
    logLevel: logLevel.INFO,
    brokers: ['localhost:9092'],
    clientId: 'example-consumer',
})

const topic = 'topic-test'
const consumer = kafka.consumer({ groupId: 'test-group' })
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner })

const sendMessage = (message) => {
    console.log("Enviando novamente para a fila");
    
    return producer.send({
        topic,
        messages: [
            { value: message }
        ]
    })
}

const run = async () => {
    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: true })
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const jsonMessage = JSON.parse(message.value.toString())
            console.log(jsonMessage)
            if (jsonMessage.valor <= 4) {
                jsonMessage.valor++
                const message = JSON.stringify(jsonMessage)
                await producer.connect()
                setTimeout(sendMessage, 5000, message)
            }
        },
    })
}

run().catch(e => console.error(`[example/consumer] ${e.message}`, e))