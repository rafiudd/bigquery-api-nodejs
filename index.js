const express = require('express');

const db = require('./command');
const port = 8080;

const app = express();

app.get('/', (request, response) => {
    response.json({code : 200, message: 'server running'})
});

app.get('/api/post', db.getFromBigQuery);
app.get('/api/get',db.getFromPostgres);
// app.delete('/api/delete', db.deleteData);
app.post('/api/table/create', db.createTable);
app.post('/api/table/drop', db.dropTable);

app.listen(port, () => {
    console.log('server running on ' + port);
});