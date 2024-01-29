import { createRequire } from "module";
const require = createRequire(import.meta.url);
import { parentPort, workerData } from "node:worker_threads";
import process from "node:process";
import { measureTime, connect, sha256Check } from "../lib/Helpers.js";
import * as es8 from "es8";
import * as es7 from "es7";
import _ from "lodash";
import * as fs from "node:fs";

const elapsed = measureTime();

import Transform from "../transform/cmdb-cis.js";

let INIT = true;
const logger = {
  info: (message) =>
    parentPort.postMessage({
      message,
      level: "info",
    }),
  debug: (message) =>
    parentPort.postMessage({
      message,
      level: "debug",
    }),
  warning: (message) =>
    parentPort.postMessage({
      message,
      level: "warning",
    }),
  error: (message) =>
    parentPort.postMessage({
      message,
      level: "error",
    }),
};
const JSON_stringify = (obj) =>
  parentPort.postMessage({
    message: JSON.stringify(obj, null, 4),
    level: "debug",
  });

const filterFile = process.cwd() + "/filters/cmdb-cis.json";
let filters = [];

if (fs.existsSync(filterFile)) {
  filters = JSON.parse(fs.readFileSync(filterFile, "utf-8"));
}

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
const store = async (client, docs) => {
  parentPort.postMessage({
    message: `Go to store ${docs.length} document(s)... to ${workerData.ELASTICSEARCH["TARGET"].INDEX}`,
  });

  const datasource = transform(
    docs.map(({ _id, _source }) => ({
      ..._source,
      _id,
    })),
  );

  const errors = [];
  parentPort.postMessage({
    message: "Storing ....",
  });
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

  parentPort.postMessage({
    message: `Done, Total: ${result.total}, successful: ${result.successful}, failed: ${result.failed}, aborted: ${result.aborted} `,
  });
  if (errors.length > 0) {
    parentPort.postMessage({
      message: JSON.stringify(errors),
      level: "error",
    });
  }
  return {
    result,
    errors,
  };
};

const getLastTimestamp = async (client, index, timeField) => {
  const body = buildBody({
    instance: "TARGET",
    payload: {
      aggs: {
        lastTimeStamp: {
          max: {
            field: timeField,
          },
        },
      },
    },
  });

  JSON_stringify(body);
  try {
    const result = await client.search(
      {
        index,
        size: 0,
        ...body,
      },
      {
        ignore: [404],
      },
    );
    console.log(result);
    return _.get(
      result,
      `${resultPath({ instance: "TARGET" })}aggregations.lastTimeStamp`,
    );
  } catch (error) {
    throw error;
  }
};

const sync = async () => {
  INIT = await sha256Check(filterFile);
  parentPort.postMessage({
    message: `${INIT ? "init sync ... create checkpoint" : "use checkpoint"}`,
  });
  const source = await connect("SOURCE", workerData, logger);
  const target = await connect("TARGET", workerData, logger);

  const bulkSize = workerData.BULK_SIZE;
  const size = workerData.HITS_SIZE;
  const reInit = workerData.REINIT || false;
  try {
    const lastCheckPoint = await getLastTimestamp(
      target,
      workerData.ELASTICSEARCH["TARGET"].INDEX,
      workerData.ELASTICSEARCH["TARGET"].TIME_FIELD,
    );

    let query = {
      bool: {
        filter: [],
      },
    };

    if (lastCheckPoint.value && !INIT) {
      parentPort.postMessage({
        message: `Last Checkpoint: ${lastCheckPoint.value_as_string}`,
      });
      query.bool.filter.push({
        range: {
          [workerData.ELASTICSEARCH["SOURCE"].TIME_FIELD]: {
            gte: lastCheckPoint.value + 1,
          },
        },
      });
    }

    query.bool.filter = query.bool.filter.concat(filters);

    parentPort.postMessage({
      message: `Query: ${JSON.stringify(query, null, 4)}`,
      level: "info",
    });

    const body = buildBody({
      instance: "TARGET",
      payload: { query },
    });

    const scroll = source.helpers.scrollSearch({
      index: workerData.ELASTICSEARCH["SOURCE"].INDEX,
      size,
      rest_total_hits_as_int: true,
      sort: `${workerData.ELASTICSEARCH["SOURCE"].TIME_FIELD}:asc`,
      ...body,
    });

    let toStore = [];
    parentPort.postMessage({
      message: `Start fetching data! Result used size: ${size}, used bulk size: ${bulkSize}`,
      level: "info",
    });
    let scrollResult;
    let fetched = 0;

    for await (scrollResult of scroll) {
      const {
        body: { hits },
        statusCode,
      } = scrollResult;
      fetched += scrollResult.documents.length;

      toStore = toStore.concat(hits.hits);
      parentPort.postMessage({
        message: `${fetched} documents fetched: ${fetched}/${hits.total}, cache: ${toStore.length}, ${statusCode}`,
        level: "info",
      });

      if (toStore.length >= bulkSize) {
        await store(target, toStore);
        toStore = [];
      }

      if (fetched >= hits.total && toStore.length > 0) {
        parentPort.postMessage({
          message: "Store last bucket...." + toStore.length,
        });
        await store(target, toStore);
        toStore = [];
      }
    }
    return fetched;
  } catch (e) {
    parentPort.postMessage({
      message: e.message,
      level: "error",
    });
    throw e;
  }
};

const transform = (data) => {
  const rules = Transform({ Package: workerData.Package });

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
        if (typeof converter == "function")
          destValue = converter(destValue, doc);
        _.set(transformedDoc, attr, destValue);
      });
    }

    transformed.push(transformedDoc);
  }
  parentPort.postMessage({
    message: JSON.stringify(transformed[0]),
    level: "debug",
  });
  return transformed;
};

elapsed.start();
await sync();
const elapsedTime = elapsed.end("s");

parentPort.postMessage({
  message: `Finished, elapsed time: ${elapsedTime.elapsed}${elapsedTime.unit}`,
  level: "info",
});
// wait for a promise to finish

process.exit(0);
