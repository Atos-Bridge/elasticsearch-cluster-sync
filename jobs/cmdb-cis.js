import { parentPort, workerData } from "node:worker_threads";
import process from "node:process";
import Logger from "../lib/Logger.js";
import { Client } from "@elastic/elasticsearch";
import _ from "lodash";
import * as fs from "node:fs";

import Transform from "../transform/cmdb-cis.js";

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const logger = Logger({ appName: "cmdb-cis-sync" });
const filterFile = process.cwd() + "/filters/cmdb-cis.json";
let filters = [];

if (fs.existsSync(filterFile))
  filters = JSON.parse(fs.readFileSync(filterFile, "utf-8"));

const connect = async (instance = "SOURCE") => {
  try {
    const client = new Client({
      node: workerData.ELASTICSEARCH[instance].URL,
      auth: {
        username: workerData.ELASTICSEARCH[instance].USERNAME,
        password: workerData.ELASTICSEARCH[instance].PASSWORD,
      },
      tls: {
        rejectUnauthorized: false,
      },
    });

    return client;
  } catch (e) {
    logger.error(
      "Cannot connect to: " + workerData.ELASTICSEARCH[instance].URL
    );
    logger.error(e.message);
    throw e;
  }
};

const store = async (client, docs) => {
  logger.info(
    `Go to store ${docs.length} document(s)... to ${workerData.ELASTICSEARCH["TARGET"].INDEX}`
  );
  const datasource = transform(
    docs.map(({ _id, _source }) => ({
      ..._source,
      _id,
    }))
  );

  const errors = [];
  logger.info("Storing ....");
  const result = await client.helpers.bulk({
    datasource,
    onDrop(doc) {
      errors.push(doc);
    },
    onDocument(doc) {
      const _id = _.get(doc, "_id");
      if (_id) delete doc._id;
      return [
        {
          update: {
            _index: workerData.ELASTICSEARCH["TARGET"].INDEX,
            _id: _id,
          },
        },
        { doc_as_upsert: true },
      ];
    },
  });
  logger.info("Storing .... done");

  logger.info(
    `Total: ${result.total}, successful: ${result.successful}, failed: ${result.failed}, aborted: ${result.aborted} `
  );
  if (errors.length > 0) {
    logger.info(errors);
  }
  return {
    result,
    errors,
  };
};

const getLastTimestamp = async (client, index, timeField) => {
  const result = await client.search(
    {
      index,
      size: 0,
      aggregations: {
        lastTimeStamp: {
          max: {
            field: timeField,
          },
        },
      },
    },
    {
      ignore: [404],
    }
  );

  return _.get(result, "aggregations.lastTimeStamp");
};

const sync = async () => {
  const source = await connect("SOURCE");
  const target = await connect("TARGET");

  const bulkSize = workerData.BULKSIZE;
  const size = workerData.HITSSIZE;
  try {
    const lastCheckPoint = await getLastTimestamp(
      target,
      workerData.ELASTICSEARCH["TARGET"].INDEX,
      workerData.TIMEFIELD
    );

    let query = {
      bool: {
        filter: [],
      },
    };

    if (lastCheckPoint.value) {
      logger.info(`Last Checkpoint: ${lastCheckPoint.value_as_string}`);
      query.bool.filter.push({
        range: {
          [workerData.TIMEFIELD]: {
            gte: lastCheckPoint.value + 1,
          },
        },
      });
    }

    query.bool.filter = query.bool.filter.concat(filters);

    logger.info(`Query: ${JSON.stringify(query, null, 4)}`);

    const scroll = source.helpers.scrollSearch({
      index: workerData.ELASTICSEARCH["SOURCE"].INDEX,
      size,
      rest_total_hits_as_int: true,
      sort: [
        {
          "@timestamp": {
            order: "asc",
          },
        },
      ],
      query,
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
        statuCode,
      } = scrollResult;
      fetched += scrollResult.documents.length;

      toStore = toStore.concat(hits.hits);
      logger.info(
        `${fetched} documents fetched: ${fetched}/${hits.total}, cache: ${toStore.length}`
      );

      if (toStore.length >= bulkSize) {
        await store(target, toStore);
        toStore = [];
      }

      if (fetched >= hits.total && toStore.length > 0) {
        logger.info("Store last bucket...." + toStore.length);
        await store(target, toStore);
        toStore = [];
      }
    }
    logger.info(`${fetched} docs fetched`);
  } catch (e) {
    logger.info(e.message);
    throw e;
  }
};

const transform = (data) => {
  const rules = Transform();
  const transformed = [];
  for (const doc of data) {
    const transformedDoc = {
      _id: doc._id,
    };
    for (const field of rules.fields) {
      const { source_attr, computed = null, destinations = [] } = field;

      const value = computed ? computed(doc) : _.get(doc, source_attr);
      destinations.map((dest) => {
        const { attr, converter } = dest;
        let destValue = value;
        if (typeof converter == "function") destValue = converter(destValue);
        _.set(transformedDoc, attr, destValue);
      });
    }
    transformed.push(transformedDoc);
  }
  logger.debug(JSON.stringify(transformed[0]));
  return transformed;
};

await sync();

// wait for a promise to finish

// signal to parent that the job is done
if (parentPort) parentPort.postMessage("done");
else process.exit(0);
