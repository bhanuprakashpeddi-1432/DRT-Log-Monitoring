const { Kafka } = require("kafkajs")
const { Client } = require("@elastic/elasticsearch")

process.env.KAFKAJS_NO_PARTITIONER_WARNING = "1"

const TOPIC = "logs-topic"
const ES_INDEX = "logs-index"
const BROKER = "localhost:9092"
const ES_NODE = "http://localhost:9200"

const kafka = new Kafka({
  clientId: "log-consumer",
  brokers: [BROKER],
  retry: {
    initialRetryTime: 3000,
    retries: 20,
    maxRetryTime: 30000,
    factor: 1.5
  }
})

const admin = kafka.admin()
const consumer = kafka.consumer({ groupId: "log-group" })
const es = new Client({ node: ES_NODE })

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

async function waitForElasticsearch() {
  console.log("[Consumer] Waiting for Elasticsearch to be ready...")
  for (let i = 0; i < 20; i++) {
    try {
      const health = await es.cluster.health({ wait_for_status: "yellow", timeout: "5s" })
      console.log("[Consumer] Elasticsearch is ready. Status:", health.status)
      return
    } catch (err) {
      console.log(`[Consumer] ES not ready yet (attempt ${i + 1}/20), retrying in 5s...`)
      await sleep(5000)
    }
  }
  throw new Error("Elasticsearch never became ready")
}

async function ensureTopicExists() {
  await admin.connect()
  const topics = await admin.listTopics()
  if (!topics.includes(TOPIC)) {
    console.log(`[Consumer] Creating topic: ${TOPIC}`)
    await admin.createTopics({
      topics: [{ topic: TOPIC, numPartitions: 1, replicationFactor: 1 }]
    })
  } else {
    console.log(`[Consumer] Topic ${TOPIC} already exists.`)
  }
  await admin.disconnect()
}

async function run() {
  console.log("[Consumer] Connecting to Kafka at", BROKER)

  await ensureTopicExists()
  await waitForElasticsearch()

  await consumer.connect()
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
  console.log("[Consumer] Subscribed to", TOPIC, "- waiting for messages...")

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const log = JSON.parse(message.value.toString())
        await es.index({ index: ES_INDEX, document: log })
        console.log("[Consumer] Indexed:", log.service, "|", log.level, "|", log.timestamp)
      } catch (err) {
        console.error("[Consumer] Error processing message:", err.message)
      }
    }
  })
}

run().catch((err) => {
  console.error("[Consumer] Fatal error:", err.message)
  process.exit(1)
})