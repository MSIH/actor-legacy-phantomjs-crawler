const _ = require('underscore');
const Apify = require('apify');
const PhantomCrawler = require('./phantom_crawler');
const inputSchema = require('../INPUT_SCHEMA.json');

const { log } = Apify.utils;

Apify.main(async () => {
    const input = await Apify.getInput();
    if (!input) throw new Error('The input was not provided');

    if (input.verboseLog) {
        log.setLevel(log.LEVELS.DEBUG);
    }

    // WORKAROUND: The legacy Apify Crawler product used to enforce default values for the following fields,
    // even if the user passed null value via API. Since passing null value for actors doesn't enforce
    // the default value from input schema, we need to do it here explicitly, in order to provide consistent behavior
    // (and avoid hanging actor runs!)
    // Additionally, we do this for "maxPageRetryCount", otherwise on page error the crawler might run infinitely.
    ['timeout', 'resourceTimeout', 'maxCrawledPagesPerSlave', 'randomWaitBetweenRequests', 'pageLoadTimeout', 'pageFunctionTimeout', 'maxParallelRequests', 'maxPageRetryCount'].forEach((key) => {
        if (typeof input[key] !== 'number') input[key] = inputSchema.properties[key].default;
    });
    
    // change the inputs values to millseconds from Seconds
    // to make UI more human readable, inputSchema was changed to seconds
    ['resourceTimeout', 'randomWaitBetweenRequests', 'pageLoadTimeout', 'pageFunctionTimeout'].forEach((key) => {
        input[key] = input[key] * 1000;
        log.info( key + ": " + input[key]);
    });
    

    // Set up finish webhook
    if (input.finishWebhookUrl) {
        const webhook = await Apify.addWebhook({
            requestUrl: input.finishWebhookUrl,
            eventTypes: [
                'ACTOR.RUN.SUCCEEDED',
                'ACTOR.RUN.FAILED',
                'ACTOR.RUN.ABORTED',
                'ACTOR.RUN.TIMED_OUT',
            ],

            // This is to ensure that on actor restart, the webhook will not be added again
            idempotencyKey: `finish-webhook-${process.env.APIFY_ACTOR_RUN_ID}`,

            // Note that ACTOR_TASK_ID might be undefined if not running in an actor task,
            // other fields can undefined when running this locally
            payloadTemplate: `{
    "actorId": ${JSON.stringify(process.env.APIFY_ACTOR_ID || null)},
    "taskId": ${JSON.stringify(process.env.APIFY_ACTOR_TASK_ID || null)},
    "runId": ${JSON.stringify(process.env.APIFY_ACTOR_RUN_ID || null)},
    "datasetId": ${JSON.stringify(process.env.APIFY_DEFAULT_DATASET_ID || null)},
    "data": ${JSON.stringify(input.finishWebhookData || null)}
}`,
        });
        log.info('Added finish webhook', { webhook: _.pick(webhook, 'id', 'idempotencyKey', 'requestUrl') });
    }

    const requestQueue = await Apify.openRequestQueue();

    const dataset = await Apify.openDataset();

    const crawler = new PhantomCrawler({
        input,
        requestQueue,
        dataset,
    });

    await crawler.run();   

    
    
    async function loadResults(datasetId, process, offset){  
    const limit = 10000;
    if(!offset){offset = 0;}
    const newItems = await Apify.client.datasets.getItems({
        datasetId, 
        offset,
        limit
    });
    if(newItems && (newItems.length || (newItems.items && newItems.items.length))){
        if(newItems.length){await process(newItems);}
        else if(newItems.items && newItems.items.length){await process(newItems.items);}
        await loadResults(datasetId, process, offset + limit);
        }
    };
        
    // create named dataset with Actor Run ID
    const datasetName = (process.env.APIFY_ACTOR_RUN_ID || null);
    log.info("datasetName: " + datasetName);  
    const namedDataset = await Apify.openDataset(datasetName);   
    
    const datasetId = dataset.datasetId;
    // load the data from datasetId and save into namedDataset
    await loadResults(datasetId, async (items) => {
            await namedDataset.pushData(items);
        });
      
    if (datasetId) {
        log.info(`Crawler finished.

Full results in JSON format:
https://api.apify.com/v2/datasets/${datasetId}/items?format=json

Simplified results in JSON format:
https://api.apify.com/v2/datasets/${datasetId}/items?format=json&simplified=1`);
    } else {
        log.info('Crawler finished.');
    }
    
       // Send mail
    const subject = 'Task Number: ' + process.env.APIFY_ACTOR_TASK_ID + 'Run Number: ' + process.env.APIFY_ACTOR_RUN_ID;
     const result = await Apify.call('apify/send-mail', {
        to: 'bschneider@msih.com',
        subject: subject,
        text: 'Completed - https://my.apify.com/tasks/'+process.env.APIFY_ACTOR_RUN_ID+'/runs'
    });
    console.log(result);

});
