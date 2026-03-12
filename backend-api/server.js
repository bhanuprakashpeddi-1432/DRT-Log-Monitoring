const express = require("express")
const cors = require("cors")
const { Client } = require("@elastic/elasticsearch")

const app = express()

app.use(cors())
app.use(express.json())

const es = new Client({
  node: "http://localhost:9200"
})

app.get("/logs", async (req,res)=>{

  const result = await es.search({
    index: "logs-index",
    query: {
      match_all: {}
    }
  })

  res.json(result.hits.hits.map(hit => hit._source))

})

app.get("/logs/service/:service", async (req,res)=>{

  const service = req.params.service

  const result = await es.search({
    index: "logs-index",
    query: {
      match: { service }
    }
  })

  res.json(result.hits.hits.map(hit => hit._source))
})

app.listen(5000, ()=>{
  console.log("API running on port 5000")
})