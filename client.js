const fs = require('fs');
const readline = require('readline');
const axios = require('axios');

const filePath = 'events.jsonl';
const liveEventUrl = 'http://localhost:8000/liveEvent'; 

const headers = {
        'Content-Type': 'application/json',
        'Authorization': 'secret'
}

async function postEvent(data) {
  try {
    const response = await axios.post(liveEventUrl, data, { headers });
    console.log('event posted:', response.data);
  } catch (error) {
    console.error('error while post event:', error.message);
  }
}

const readLine = readline.createInterface({
  input: fs.createReadStream(filePath),
  crlfDelay: Infinity,
});

readLine.on('line', async (line) => {
  await postEvent(line);
});

readLine.on('close', () => {
  console.log('post events completed');
});