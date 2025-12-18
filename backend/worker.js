const amqp = require("amqplib");
const { Client } = require("@elastic/elasticsearch");

const es = new Client({
  node: "http://localhost:9200",
});

async function ensureIndex() {
  const exists = await es.indices.exists({ index: "news" });

  if (!exists) {
    await es.indices.create({
      index: "news",
      mappings: {
        properties: {
          title: { type: "text" },
          content: { type: "text" },
          author: { type: "text" },
          source: { type: "text" },
          created_at: { type: "date" },
        },
      },
    });

    console.log(">>> Created ES Index 'news'");
  }
}

async function startWorker() {
  await ensureIndex();

  const connection = await amqp.connect("amqp://localhost:5672");
  const channel = await connection.createChannel();

  await channel.assertQueue("news_queue", {
    durable: true,
  });

  console.log(">>> News Q waiting for new entries...");

  channel.consume(
    "news_queue",
    async (msg) => {
      if (!msg) return;

      const news = JSON.parse(msg.content.toString());

      console.log(">>> processing news", news.id);

      try {
        await es.index({
          index: "news",
          id: news.id,
          document: {
            title: news.title,
            content: news.content,
            author: news.author,
            source: news.source,
            created_at: new Date(),
          },
        });

        console.log(`>>> ${news.id} indexed in ES`);
        channel.ack(msg);
      } catch (error) {
        console.error(error);
        // channel.ack(msg);
      }
    },
    {
      noAck: false,
    }
  );
}

startWorker().catch(console.error);
