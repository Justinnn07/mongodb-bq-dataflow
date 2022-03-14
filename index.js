const mongoose = require("mongoose");
const { PubSub } = require("@google-cloud/pubsub");

// pubsub client
const pubsubClient = new PubSub({
  projectId: "vigilant-yeti-343907",
  keyFilename: "./vigilant-yeti-343907-13ab28347e73.json",
});

// connect to mongoose
mongoose
  .connect("mongodb+srv://admin:admin@cluster0.lsbxu.mongodb.net/sample_mflix")
  .then(() => console.log("successfully connected"));

const topicName = "mongo-bq-test";

async function createTopic() {
  // Creates a new topic
  await pubsubClient.createTopic(topicName);
  console.log(`Topic ${topicName} created.`);
}

async function doesTopicExist() {
  const topics = await pubsubClient.getTopics();
  const topicExists = topics.find((topic) => topic.name === topicName);
  return topics && topicExists;
}

if (!doesTopicExist()) {
  createTopic();
}

async function publishMessage(data) {
  try {
    const messageId = await pubsubClient.topic(topicName).publishJSON(data);
    console.log(`Message ${messageId} published`);
  } catch (error) {
    console.error(`Received error while publishing: ${error.message}`);
    process.exitCode = 1;
  }
}

const db = mongoose.connection;

db.on("error", console.error.bind(console, "Connection Error:"));

db.once("open", () => {
  const taskCollection = db.collection("comments");
  const changeStream = taskCollection.watch();

  changeStream.on("change", async (change) => {
    console.log(change.operationType);
    if (change.operationType === "insert") {
      let data = change.fullDocument;
      delete data._id;
      await publishMessage(data);
    }
  });
});
