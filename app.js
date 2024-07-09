

const express = require('express');
const cors = require('cors');
const { Client } = require('@elastic/elasticsearch');
const bodyParser = require('body-parser');

const app = express();
const port = 8000;

app.use(cors());
app.use(bodyParser.json());

const esClient = new Client({
        node : `https://0ab57394385c4e0aa34417359c694dc9.us-central1.gcp.cloud.es.io`,
        auth: {
          username: 'elastic',
          password: 'ZHoicwrP7j0qnqsyakOGu9w1'
        }
    });

    app.post('/push-data', async (req, res) => {
        const data = req.body;
    
        if (!Array.isArray(data)) {
            return res.status(400).send({ error: 'Invalid data format. Expected an array.' });
        }
    
        try {
            // Check for existing machineIds in Elasticsearch
            const existingMachineIds = await checkExistingMachineIds(data.map(doc => doc.machineId));
            
            // Filter out documents with existing machineIds
            const newData = data.filter(doc => !existingMachineIds.includes(doc.machineId));
    
            // If all documents are duplicates, return error
            if (newData.length === 0) {
                return res.status(400).send({ error: 'All documents contain duplicate machineIds.' });
            }
    
            // Prepare bulk operation for new documents
            const bulkOps = newData.flatMap(doc => [{ index: { _index: 'malware_data' } }, doc]);
            
            // Execute bulk insertion
            const response = await esClient.bulk({
                refresh: true,
                body: bulkOps
            });
    
            if (response.errors) {
                const erroredDocuments = response.items
                    .filter(item => item.index && item.index.error)
                    .map(item => item.index._id); // Extract IDs of failed documents
                console.error('Errored documents:', erroredDocuments);
                
                // Return error with details of failed documents
                return res.status(500).send({
                    error: 'Some documents failed to index.',
                    details: {
                        erroredDocuments,
                        duplicateMachineIds: existingMachineIds
                    }
                });
            }
    
            res.status(200).send({ message: 'Documents indexed successfully', response, existingMachineIds: existingMachineIds });
        } catch (error) {
            console.error(error);
            res.status(500).send({ error: 'Failed to push data to Elasticsearch' });
        }
    });
    
    async function checkExistingMachineIds(machineIds) {
        try {
            const  body  = await esClient.search({
                index: 'malware_data',
                body: {
                    query: {
                        terms: {
                            machineId: machineIds
                        }
                    }
                }
            });
            // console.log('body', body.hits.hits);
            const existingIds = body.hits.hits.map(hit => hit._source.machineId);
            return existingIds;
        } catch (error) {
            console.error('Error checking existing machineIds:', error);
            return []; // Return empty array if error occurs
        }
    }



app.get('/get-data', async (req, res) => {
    try {
        const response = await esClient.search({
            index: 'malware_data',
            body: {
                query: {
                    match_all: {}
                }
            }
        });
        res.status(200).send(response.hits.hits);
    } catch (error) {
        console.error(error);
        res.status(500).send({ error: 'Failed to retrieve data from Elasticsearch' });
    }
});

app.listen(port, () => {
    console.log(`Server is running on http://localhost:${port}`);
});




