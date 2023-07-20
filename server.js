const express = require('express');
const bodyParser = require("body-parser");
const { Pool } = require('pg');
const fs = require('fs');
const app = express();
const router = express.Router();

app.use(bodyParser.urlencoded({
    extended: false
}));
app.use(bodyParser.json());

router.post('/liveEvent', (request, response) => {

    console.log(request.body);
    if (request.headers.authorization != 'secret') {
        console.log('Authentication Failed');
        response.set('WWW-Authenticate', 'Basic realm="401"');
        response.status(401).send('Authentication required.');

        return;
    }

    saveEventInFile(request.body);
    response.json({});

});

router.get('/userEvents/:userId', async(request, response) => {

    console.log(JSON.stringify(request.params));
    results = await getEventsForUser(request.params.userId);
    response.json(results);
});

app.use("/", router);
app.listen(8000, function () {
    console.log(`server is listening`);
});



function saveEventInFile(event) {
    const filePath = 'events-server.jsonl';
    fs.appendFile(filePath, JSON.stringify(event) + '\n', (err) => {
        if (err) {
            console.error('Error appending event to file:', err);
        } else {
            console.log('event appended to file successfully.');
        }
    });

}

const pool = new Pool({
    user: 'postgres',
    host: 'localhost',
    database: 'postgres',
    password: 'sa',
    port: 5432, 
  });

async function getEventsForUser(userId) {
    try {
        const client = await pool.connect();
        const result = await client.query(`SELECT * FROM users_revenue WHERE user_id = '${userId}'`);
        client.release();
        return result.rows;
      } catch (error) {
        console.error('Error reading table from DB:', error.message);
        throw error;
      }

}
