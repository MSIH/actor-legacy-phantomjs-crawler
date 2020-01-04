const _ = require('underscore');
const Apify = require('apify');
const PhantomCrawler = require('./phantom_crawler');
const inputSchema = require('../INPUT_SCHEMA.json');

const {
    log
} = Apify.utils;

Apify.main(async () => {
    const input = await Apify.getInput();
    if (!input) throw new Error('The input was not provided');

    log.debug("input.verboseLog: " + input.verboseLog);
    if (!input.verboseLog) {
        log.setLevel(log.LEVELS.INFO);
    } else {
        log.setLevel(log.LEVELS.DEBUG);
    }

    log.debug("log.getLevel(): " + log.getLevel());

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
        log.debug(key + ": " + input[key]);
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
        log.debug('Added finish webhook', {
            webhook: _.pick(webhook, 'id', 'idempotencyKey', 'requestUrl')
        });
    }

    const requestQueue = await Apify.openRequestQueue();
    const dataset = await Apify.openDataset();
    const crawler = new PhantomCrawler({
        input,
        requestQueue,
        dataset,
    });

    await crawler.run();

    // create named dataset with Task Name Actor Run ID
    log.debug('Obtaining task...');
    const task = await Apify.client.tasks.getTask({
        taskId: process.env.APIFY_ACTOR_TASK_ID
    });
    log.debug(`Task Name ${task.name}...`);

    const run = await Apify.client.acts.getRun({
        actId: process.env.APIFY_ACTOR_ID,
        runId: process.env.APIFY_ACTOR_RUN_ID
    });
    log.debug(`CompletionStatus JSON.stringify(${run})...`);
    log.debug(`CompletionStatus ${run.status}...`);


    const runID = process.env.APIFY_ACTOR_RUN_ID || "";

    if (input.datasetName) {
        datasetTitle = input.datasetName + "---" + runID;
    } else {
        datasetTitle = "---" + runID;
    }

    const datasetName = task.name + "---" + datasetTitle;

    const datasetId = dataset.datasetId;

    if (datasetId) {


        log.info(`Crawler finished.

Full results in JSON format:
https://api.apify.com/v2/datasets/${datasetId}/items?format=json

Simplified results in JSON format:
https://api.apify.com/v2/datasets/${datasetId}/items?format=json&simplified=1`);

        // load the data from datasetId and save into namedDataset
        log.info("Create Database: " + datasetName);
        const namedDataset = await Apify.openDataset(datasetName);
        await loadResults(datasetId, async (items) => {
            await namedDataset.pushData(items);
        });
    } else {
        log.info('Crawler finished.');
    }

    async function loadResults(datasetId, process, offset) {
        const limit = 10000;
        if (!offset) {
            offset = 0;
        }
        const newItems = await Apify.client.datasets.getItems({
            datasetId,
            offset,
            limit
        });
        if (newItems && (newItems.length || (newItems.items && newItems.items.length))) {
            if (newItems.length) {
                await process(newItems);
            } else if (newItems.items && newItems.items.length) {
                await process(newItems.items);
            }
            await loadResults(datasetId, process, offset + limit);
        }
    };

    // Send email notification
    if (input.sendEmailNotification) {

        // create webhooks
        const webhooks = await Apify.addWebhook({
            // run web hook on all events, except create
            eventTypes: [
                'ACTOR.RUN.SUCCEEDED',
                'ACTOR.RUN.FAILED',
                'ACTOR.RUN.ABORTED',
                'ACTOR.RUN.TIMED_OUT',
            ],
            // URL of web hook
            requestUrl: 'https://api.apify.com/v2/acts/barry8schneider~task-notification/runs?token=' + token.slice(1, -1),

            // web hook uses standard template
            payloadTemplate: `
 {
    "userId": {{userId}},
    "createdAt": {{createdAt}},
    "eventType": {{eventType}},
    "eventData": {{eventData}},
    "resource": {{resource}}

}`,
            // This is to ensure that on actor restart, the webhook will not be added again
            idempotencyKey: process.env.APIFY_ACTOR_RUN_ID,
        });
    }

});
