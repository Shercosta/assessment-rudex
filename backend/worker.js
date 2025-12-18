const amqp = require("amqplib");

async function startWorker() {
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

      channel.ack(msg);
    },
    {
      noAck: false,
    }
  );
}

startWorker().catch(console.error);
