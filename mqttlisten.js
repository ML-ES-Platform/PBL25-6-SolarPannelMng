const mqtt = require('mqtt');
const { MongoClient } = require('mongodb');

const host = '';
const port = 8883;
const protocol = 'mqtts';

const options = {
  username: '',
  password: '',
  protocol,
  host,
  port,
  clean: true,
  connectTimeout: 4000,
  rejectUnauthorized: true
};

const mongoUri = 'mongodb://localhost:27017'; 
const dbName = 'solar';
const collectionName = 'solar_data';

const client = mqtt.connect(options);
const mongoClient = new MongoClient(mongoUri);


let db, collection;
mongoClient.connect().then(() => {
  db = mongoClient.db(dbName);
  collection = db.collection(collectionName);
  console.log('Connected to MongoDB');
}).catch((err) => {
  console.error('MongoDB connection error:', err);
});

const topic = 'data';

client.on('connect', () => {
  console.log('Connected to Solar Panel MQTT');

  client.subscribe(topic, (err) => {
    if (err) {
      console.error('Subscription error:', err);
    } else {
      console.log(`Subscribed to topic: ${topic}`);
    }
  });
});

client.on('message', async (topic, message) => {
  try {
    const parsedData = JSON.parse(message.toString());

    if ('validity_check' in parsedData) {
      console.log('Skipping message due to presence of validity_check field.');
      return; 
    }

    const document = {
      timestamp: new Date(),
      data: parsedData
    };

    const result = await collection.insertOne(document);
    console.log('Saved message with ID:', result.insertedId);
  } catch (err) {
    console.error('Error handling message:', err);
  }
});


client.on('error', (err) => {
  console.error('MQTT connection error:', err);
});