const { Kafka, Partitioners } = require("kafkajs")

process.env.KAFKAJS_NO_PARTITIONER_WARNING = "1"

const TOPIC = "logs-topic"
const BROKER = "localhost:9092"

const kafka = new Kafka({
  clientId: "log-producer",
  brokers: [BROKER],
  retry: {
    initialRetryTime: 3000,
    retries: 20,
    maxRetryTime: 30000,
    factor: 1.5
  }
})

const admin = kafka.admin()
const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner
})

const services = ["user-service", "order-service", "payment-service"]
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

async function ensureTopicExists() {
  await admin.connect()
  const topics = await admin.listTopics()
  if (!topics.includes(TOPIC)) {
    console.log(`Creating topic: ${TOPIC}`)
    await admin.createTopics({
      topics: [{ topic: TOPIC, numPartitions: 1, replicationFactor: 1 }]
    })
    console.log(`Topic ${TOPIC} created.`)
  } else {
    console.log(`Topic ${TOPIC} already exists.`)
  }
  await admin.disconnect()
}

async function produceLogs() {
  console.log("[Producer] Connecting to Kafka at", BROKER)

  await ensureTopicExists()
  await producer.connect()
  console.log("[Producer] Connected. Sending logs every 2s...")

  setInterval(async () => {
    const log = {
      timestamp: new Date().toISOString(),
      service: services[Math.floor(Math.random() * services.length)],
      level: ["INFO", "WARN", "ERROR"][Math.floor(Math.random() * 3)],
      message: "Random log event"
    }
    try {
      await producer.send({
        topic: TOPIC,
        messages: [{ value: JSON.stringify(log) }]
      })
      console.log("[Producer] Sent:", log.service, "|", log.level, "|", log.timestamp)
    } catch (err) {
      console.error("[Producer] Send failed:", err.message)
    }
  }, 2000)
}

produceLogs().catch((err) => {
  console.error("[Producer] Fatal error:", err.message)
  process.exit(1)
})