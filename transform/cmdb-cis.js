import * as _ from "lodash-es";

import Package from `${process.cwd()}/package.json`



const customerMap = {
  "99ad38121b00a8503e28eacee54bcb3f": {
    shorthand: "wsp",
    name: "WSP Global Inc",
  },
};


const tags = ["bgi-cmdb-cis-sync"];

export default function ({ source = "bgi:" } = {}) {
  return {
    fields: [
      {
        computed: (doc) => {
          return `${source}${doc.datasource}`;
        },
        destinations: [
          {
            attr: "datasource",
          },
        ],
      },
      {
        computed: (doc) => {
          return Package.version;
        },
        destinations: [
          {
            attr: "@version",
          },
        ],
      },
      {
        source_attr: "criticality",
        destinations: [
          {
            attr: "criticality",
            converter: (value) => value && value.toString(),
          },
        ],
      },
      {
        source_attr: "support_group",
        destinations: [
          {
            attr: "support_group_l1",
            converter: (value) => _.get(value, "l1.name"),
          },
        ],
      },
      {
        source_attr: "geo",
        destinations: [
          {
            attr: "geo",
            converter: (value) => {
              const geo = {
                city_name: _.get(value, "city.name"),
                country_iso_code: _.get(value, "country.iso_code"),
                state_name: _.get(value, "state.name"),
                country_name: _.get(value, "country.name"),
                name: _.get(value, "name"),
                location: _.get(value, "location"),
              };

              return _.keys(geo).length > 0 ? geo : null;
            },
          },
        ],
      },
      {
        source_attr: "parents",
        destinations: [
          {
            attr: "parents",
          },
        ],
      },
      {
        source_attr: "category",
        destinations: [
          {
            attr: "category",
            converter: (value) => value["1"],
          },
        ],
      },
      {
        source_attr: "is_monitored",
        destinations: [
          {
            attr: "is_monitored",
            converter: (value) => (value ? "1" : "0"),
          },
        ],
      },
      {
        source_attr: "class",
        destinations: [
          {
            attr: "class",
          },
        ],
      },
      {
        source_attr: "ingested_at",
        destinations: [
          {
            attr: "@timestamp",
          },
          {
            attr: "extract_timestamp",
          },
        ],
      },
      {
        source_attr: "class_name",
        destinations: [
          {
            attr: "class_name",
          },
        ],
      },
      {
        source_attr: "parent_count",
        destinations: [
          {
            attr: "sys_updated_on",
            converter: (value) => value && value.toString(),
          },
        ],
      },
      {
        computed: (doc) => {
          return tags;
        },
        destinations: [
          {
            attr: "tags",
          },
        ],
      },
      {
        source_attr: "updated_at",
        destinations: [
          {
            attr: "sys_updated_on",
          },
        ],
      },
      {
        source_attr: "created_at",
        destinations: [
          {
            attr: "sys_created_on",
          },
        ],
      },
      {
        source_attr: "customer_specific_data",
        destinations: [
          {
            attr: "customer_specific_data",
            converter: (value) => {
              return value["1"];
            },
          },
        ],
      },
      {
        source_attr: "operational_status",
        destinations: [
          {
            attr: "operational_status",
          },
        ],
      },
      {
        source_attr: "name_normalized",
        destinations: [
          {
            attr: "name",
          },
        ],
      },
      {
        source_attr: "name",
        destinations: [
          {
            attr: "raw_data.name",
          },
        ],
      },
      {
        source_attr: "id",
        destinations: [
          {
            attr: "id",
          },
        ],
      },
      {
        source_attr: "sys_id",
        destinations: [
          {
            attr: "sys_id",
          },
        ],
      },
    ],
  };
}
