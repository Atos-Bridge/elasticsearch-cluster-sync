# elasticsearch-cluster-sync

This project was created to synchronize elasticsearch indices across clusters.


## Syncronization job configuration

The index pair configuration is done via environment variables.
Here is an example:


```

JOB.CMDB-CIS.SCHEDULE.INTERVAL=1m
JOB.CMDB-CIS.WORKER.BULKSIZE=50000
JOB.CMDB-CIS.WORKER.HITSSIZE=10000
JOB.CMDB-CIS.WORKER.TIMEFIELD=@timestamp
JOB.CMDB-CIS.WORKER.ELASTICSEARCH.SOURCE.URL=https://localhost:9200
JOB.CMDB-CIS.WORKER.ELASTICSEARCH.SOURCE.PASSWORD_FILE=./.temp/password
JOB.CMDB-CIS.WORKER.ELASTICSEARCH.SOURCE.USERNAME=elastic
JOB.CMDB-CIS.WORKER.ELASTICSEARCH.SOURCE.INDEX=cmdb-cis
JOB.CMDB-CIS.WORKER.ELASTICSEARCH.TARGET.URL=https://localhost:9200
JOB.CMDB-CIS.WORKER.ELASTICSEARCH.TARGET.PASSWORD_FILE=./.temp/password
JOB.CMDB-CIS.WORKER.ELASTICSEARCH.TARGET.USERNAME=elastic
JOB.CMDB-CIS.WORKER.ELASTICSEARCH.TARGET.INDEX=new-cmdb-cis
```



## Filtering input

To filter the input, you can create a json file with the name of the job in the filters directory.
There you can define filters as an array.
Here is an example:


```json
[
  {
    "terms": {
      "company.sys_id": ["99ad38121b00a8503e28eacee54bcb3f"]
    }
  }
]

```
