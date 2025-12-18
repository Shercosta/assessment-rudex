const amqp = require("amqplib");

let channel;

async function connectRMQ(retries = 10, delay = 3000) {
  const url = process.env.RABBITMQ_URL || "amqp://rabbitmq:5672";

  while (retries > 0) {
    try {
      const connection = await amqp.connect(url);
      console.log("✅ RabbitMQ connected");
      return connection;
    } catch (err) {
      retries--;
      console.log(`⏳ Waiting for RabbitMQ... ${retries} attempts left`);
      await new Promise((r) => setTimeout(r, delay));
    }
  }

  throw new Error("❌ Could not connect to RabbitMQ after retries");
}

async function connectRabbit() {
  const connection = await connectRMQ();
  channel = await connection.createChannel();

  // news queue
  await channel.assertQueue("news_queue", {
    durable: true,
  });

  console.log(">>> RMQ Connected");
}

function getChannel() {
  if (!channel) {
    throw new Error("RMQ not connected");
  } else {
    return channel;
  }
}

module.exports = {
  connectRabbit,
  getChannel,
};
