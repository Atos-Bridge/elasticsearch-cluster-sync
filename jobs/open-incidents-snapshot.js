import { parentPort, workerData } from "node:worker_threads";
import { measureTime, connect, sha256Check, Logger } from "../lib/Helpers.js";
import process from "node:process";
import _ from "lodash";
import * as fs from "node:fs";
const elapsed = measureTime();

let QUERY_CHANGED = true;
const logger = Logger(parentPort);

const queryFile =
  process.cwd() + `/filters/${workerData.job_type}.${workerData.job_name}.json`;

const getTargetClientVersion = () =>
  workerData.ELASTICSEARCH.TARGET.ES_CLIENT_VERSION;

const getSourceClientVersion = () =>
  workerData.ELASTICSEARCH.SOURCE.ES_CLIENT_VERSION;

const resultPath = ({ instance } = {}) => {
  const esVersion = workerData.ELASTICSEARCH[instance].ES_CLIENT_VERSION;
  switch (esVersion) {
    case "es7":
      return "body.";
    case "es8":
      return "";
  }
};
const buildBody = ({ payload, instance } = {}) => {
  const esVersion = workerData.ELASTICSEARCH[instance].ES_CLIENT_VERSION;
  switch (esVersion) {
    case "es7":
      return { body: payload };
    case "es8":
      return payload;
  }
};

const store = async (client, docs, execution_time) => {
  logger.info(
    `Go to store ${docs.length} document(s)... to ${workerData.ELASTICSEARCH["TARGET"].INDEX}`,
  );

  const datasource = docs.map(({ _id, _source }) => ({
    ..._source,
    "@timestamp": execution_time,
  }));

  const errors = [];
  logger.info("Storing ....");
  const result = await client.helpers.bulk({
    datasource,
    onDrop(doc) {
      errors.push(doc);
    },
    onDocument(doc) {
      return {
        index: {
          _index: workerData.ELASTICSEARCH["TARGET"].INDEX,
        },
      };
    },
  });

  logger.info(
    `Total: ${result.total}, successful: ${result.successful}, failed: ${result.failed}, aborted: ${result.aborted} `,
  );
  if (errors.length > 0) {
    logger.info(errors);
  }
  return {
    result,
    errors,
  };
};

const sync = async () => {
  await sha256Check(queryFile);
  const execution_time = new Date().toISOString();
  let query = {
    match_all: {},
  };
  if (fs.existsSync(queryFile)) {
    query = JSON.parse(fs.readFileSync(queryFile, "utf-8"));
  } else {
    throw new Error(`Filter file: ${queryFile} not found.`);
  }
  logger.info(`start sync...`);
  const source = await connect("SOURCE", workerData, logger);
  const target = await connect("TARGET", workerData, logger);

  const bulkSize = workerData.BULK_SIZE;
  const size = workerData.HITS_SIZE;

  try {
    if (QUERY_CHANGED) {
      logger.info("First time use of query ....");
    }
    logger.info(`Query: ${JSON.stringify(query, null, 4)}`);

    const body = buildBody({
      instance: "TARGET",
      payload: { query },
    });

    const scroll = source.helpers.scrollSearch({
      index: workerData.ELASTICSEARCH["SOURCE"].INDEX,
      size,
      rest_total_hits_as_int: true,
      sort: !_.isNil(workerData.ELASTICSEARCH["SOURCE"].TIMEFIELD)
        ? `${workerData.ELASTICSEARCH["SOURCE"].TIMEFIELD}:asc`
        : undefined,
      ...body,
    });

    let toStore = [];
    logger.info("Start fetching data ....");
    logger.info("Result size:" + size);
    logger.info("Result bulkSize:" + bulkSize);
    let scrollResult;
    let fetched = 0;

    for await (scrollResult of scroll) {
      const {
        body: { hits },
      } = scrollResult;
      fetched += scrollResult.documents.length;

      toStore = toStore.concat(hits.hits);
      logger.info(
        `${fetched} documents fetched: ${fetched}/${hits.total}, cache: ${toStore.length}`,
      );

      if (toStore.length >= bulkSize) {
        logger.info("Store bucket...." + toStore.length);

        await store(target, toStore, execution_time);
        toStore = [];
      }

      if (fetched >= hits.total && toStore.length > 0) {
        logger.info("Store last bucket...." + toStore.length);
        await store(target, toStore, execution_time);
        toStore = [];
      }
    }
    return fetched;
  } catch (e) {
    logger.error(e);
    throw e;
  }
};
elapsed.start();
const fetched = await sync();
// wait for a promise to finish
const elapsedTime = elapsed.end("s");

logger.info(
  `Finished, ${fetched} docs fetched, elapsed time: ${elapsedTime.elapsed}${elapsedTime.unit}`,
);

process.exit(0);
