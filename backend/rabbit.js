const amqp = require("amqplib");

let channel;

async function connectRabbit() {
  const connection = await amqp.connect("amqp://localhost:5672");
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
