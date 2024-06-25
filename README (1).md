## Name
Routing datasources

## Description
This Cloud function is responsible for routing source files to destination using Referential routing in BQ.

## Architecture

![Architecture ingestion](image.png)

This involves Cloud Pub/Sub, Cloud Scheduler and Cloud Function.

The code was developed according to this documentation : https://cloud.google.com/scheduler/docs/tut-gcf-pub-sub

## Cloud Pub/Sub

As our source data are located on our ingestion GCP project, the first thing to do is to notify the Pub/Sub ingestion topic whenever a new file is uploaded in ingestion's project. To do so, use this command:

```
gcloud storage buckets notifications create gs://orange_data_ia_raw_datas/ --topic=data_ia_ingestion_data_sources_topic_dev --event-types=OBJECT_FINALIZE
```

Documentation to create Pub/Sub Topic: https://cloud.google.com/pubsub/docs/create-topic

#### Pub/Sub Topics
In order to make our cloud function, we need 2 topics:
- one which will be used by Cloud Scheduler in order to trigger the Cloud Function by sending one message to it.
- the other that contains all uploads notifications of ingestion source files

## Cloud Scheduler

The Cloud Scheduler job was configured as a cron and has as target type Pub/Sub. Whenever it is triggered, a message will be published to dedicated Pub/Sub topic and that will trigger the associated Cloud Function

## Cloud Function

The Cloud Function pulls every messages in Pub/Sub in order to know the new updated files.
Then, it copies these files to their destination buckets.