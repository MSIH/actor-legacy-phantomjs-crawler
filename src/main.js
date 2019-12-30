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
    ['timeout', 'maxCrawledPagesPerSlave', 'randomWaitBetweenRequests', 'pageLoadTimeout', 'pageFunctionTimeout', 'maxParallelRequests', 'maxPageRetryCount'].forEach((key) => {
        if (typeof input[key] !== 'number') input[key] = inputSchema.properties[key].default;
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
    
    // create named dataset
    const datasetName = (process.env.APIFY_ACTOR_RUN_ID || null);
    log.info(datasetName);
    const dataset = await Apify.openDataset(datasetName);

    const crawler = new PhantomCrawler({
        input,
        requestQueue,
        dataset,
    });

    await crawler.run();   

    const datasetId = dataset.datasetId;
    if (datasetId) {
        log.info(`Crawler finished.

Full results in JSON format:
https://api.apify.com/v2/datasets/${datasetId}/items?format=json

Simplified results in JSON format:
https://api.apify.com/v2/datasets/${datasetId}/items?format=json&simplified=1`);
    } else {
        log.info('Crawler finished.');
    }

});
