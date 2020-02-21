const { Pool, Client } = require('pg');
const { BigQuery } = require('@google-cloud/bigquery');
const bigquery = new BigQuery();

const pool = new Pool({
  user: "user",
  host: "localhost",
  database: "sample_analytics",
  password: "12345",
  port: "5432"
});

async function getFromBigQuery(req,res) {
    const query = `SELECT * FROM \`bigquery-public-data.google_analytics_sample.ga_sessions_20170801\`limit 10`;
    const options = {
        query: query,
        location: 'US',
    };
    
    const [job] = await bigquery.createQueryJob(options);
    console.log(`Job ${job.id} started.`);
    
    const [rows] = await job.getQueryResults();
    rows.forEach(row => console.log(row));

    let array = [];
    for (let i = 0; i < rows.length; i++) {
        let data = rows[i];
        let model = {
            visitId: data.visitId,
            date: data.date,
            totals: data.totals,
            device: data.device,
            geoNetwork: data.geoNetwork
        }
        array.push(model);
    }

    for(let a = 0; a < array.length; a++) {
        let values = Object.values(array[a]);
        
        let queryInsert = 'INSERT INTO sample_analytics (visitId,date,totals,device,geoNetwork) VALUES ($1,$2,$3,$4,$5)';
        pool.query(queryInsert, values).then(result => {
          return res.json ({ code : 200, success: true, message: "Success Insert Data From Bigquery" })
        }).catch(error => {
          return res.json ({ code : 500, success: true, message: "Ooopsss!, Something is wrong", data: error })
        })
    }
}

async function getFromPostgres(req,res) {
    try {
        const query = 'SELECT * FROM sample_analytics'

        const result = await pool.query(query);
        return res.json ({ code : 201, success: true, message: "Success Get Data", data: result.rows })
    } catch (error) {
        return res.json ({ code : 500, success: true, message: "Ooopsss!, Something is wrong", data: error })
    }
}

async function createTable(req,res) {
    try {
        const query = `CREATE TABLE sample_analytics (
            visitId integer,
            date timestamp,
            totals json,
            device json,
            geoNetwork json
        )`
    
        const result = await pool.query(query)
        return res.json ({ code : 201, success: true, message: "Success Create Table", data: result })
    } catch (error) {
        return res.json ({ code : 500, success: true, message: "Ooopsss!, Something is wrong", data: error })
    }
    
}

async function dropTable(req,res){
    try {
        let query = `DROP TABLE sample_analytics`
        const result = await pool.query(query);

        return res.json ({ code : 201, success: true, message: "Success Drop Table", data: result })
    } catch (error) {
        return res.json ({ code : 500, success: true, message: "Ooopsss!, Something is wrong", data: error })
    }    
}

module.exports = {
  getFromBigQuery,
  getFromPostgres,
  createTable,
  dropTable
}