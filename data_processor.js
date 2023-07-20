const fs = require('fs');
const readline = require('readline');
const {
    Pool
} = require('pg');


const serverFile = 'events-server.jsonl';
const inProcessFile = 'events-in-process.jsonl';

const pool = new Pool({
    user: 'postgres',
    host: 'localhost',
    database: 'postgres',
    password: 'sa',
    port: 5432,
});


const data = {};

function processEvent(event) {
    let userData = data[event.userId];
    if (!userData) {
        userData = {
            userId: event.userId,
            value: 0
        }
        data[event.userId] = userData;
    }
    if (event.name == 'add_revenue') {
        userData.value += event.value;
    } else {
        userData.value -= event.value;
    }
}

async function saveData(data) {
    const client = await pool.connect();
    for (const userData of Object.values(data)) {
        if (userData.value) {
            
            const query = `INSERT INTO users_revenue (user_id, revenue) VALUES ('${userData.userId}', ${userData.value})
            ON CONFLICT (user_id)
            DO UPDATE SET revenue = users_revenue.revenue + ${userData.value}`
            try {
                await client.query(query);
            } catch (error) {
                console.error('Error in update', error.message);
                throw error;
            }
        }
    }
    client.release();
}

fs.rename(serverFile, inProcessFile, (err) => {
    if (err) {
        console.error('Error renaming file:', err);
    } else {
        console.log('File renamed successfully.');
    }
});

const readLine = readline.createInterface({
    input: fs.createReadStream(inProcessFile),
    crlfDelay: Infinity,
});

readLine.on('line', async (line) => {
    processEvent(JSON.parse(line));
});

readLine.on('close', async () => {
    await saveData(data);
    console.log('process events completed');
});