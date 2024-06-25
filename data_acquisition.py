import base64
import functions_framework
import os
import json
import ast
from google.cloud import (storage, bigquery, pubsub)
import google.cloud.logging
import logging

def env_vars(env_variable):
    return os.environ.get(env_variable, "Specified environment variable" + env_variable + "is not set.")

def transfer_file(source_project, source_bucket, source_object, source_object_filename, destination_project, destination_bucket, destination_directory):
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(source_bucket)

    destination_bucket = storage_client.bucket(destination_bucket) 
   
    blob = source_bucket.blob(source_object_filename)
    #copy to bucket destination
    destination_blob = source_bucket.copy_blob(blob, destination_bucket, source_object_filename)
    #copy to directory
    destination_bucket.rename_blob(destination_blob, f"{destination_directory}/{source_object_filename}")
    
    #deleting datasource after copy 
    logging.info(f"Deleting project {source_project}'s bucket {source_bucket}'s file {source_object_filename}")
    blob.delete()
    logging.info(f"""Transfering file {source_object} from project {source_project}'s bucket {source_bucket}
                 to project {destination_project}'s bucket {destination_bucket}'s directory {destination_directory}
                 """)

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def copy_datasources_to_destination_bucket(cloud_event):    
    #env vars
    PROJECT_ID = env_vars('PROJECT_ID')
    SUBSCRIPTION_NAME = env_vars('SUBSCRIPTION_NAME')
    ROUTING_REFERENTIAL_TABLE_ID = env_vars('ROUTING_REFERENTIAL_TABLE_ID')
    MAX_BATCH_SIZE = int(env_vars('MAX_BATCH_SIZE'))

    #setup logging and Big Query Client
    google.cloud.logging.Client(project=PROJECT_ID).setup_logging()
    client = bigquery.Client()
    
    logging.info("Triggered with message : " + str(base64.b64decode(cloud_event.data["message"]["data"])))

    #setup subscriber
    subscriber = pubsub.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)

    #consuming all messages from PubSub
    while True:

        response = subscriber.pull(
            request={
            "subscription": subscription_path,
            "max_messages": MAX_BATCH_SIZE,
            }
        )

        #when no message pulled
        if not response.received_messages:
            logging.info('No message pulled from PUB/SUB')
            break

        def decodeMsgAndComputeQuery(msg):
            decoded = ast.literal_eval(msg.message.data.decode('utf-8'))
            source_bucket = decoded["bucket"]
            source_object = decoded["name"]
            source_object_filename = source_object.split("/")[-1]
            query = f"""
            (
                SELECT *, "{source_object}" as source_object, "{source_object_filename}" as source_object_filename FROM {PROJECT_ID}.{ROUTING_REFERENTIAL_TABLE_ID}
                WHERE source_bucket="{source_bucket}" AND REGEXP_CONTAINS('{source_object_filename}', source_file_mask)
            )
            """
            return query
        
        query = 'UNION ALL\n'.join(list(map(decodeMsgAndComputeQuery, response.received_messages)))
        logging.info("Querying Big Query with :\n" + query)

        query_job = client.query(query)  # API request
        rows = query_job.result()  # Waits for query to finish

        if rows.total_rows >= 1:
            for row in rows:
                transfer_file(row["source_project"], row["source_bucket"], row["source_object"], row["source_object_filename"],
                               row["destination_project"], row["destination_bucket"], row["destination_directory"])
        else:
            logging.warn(f"Query result was empty! Please check if there was any source data.")

        ack_ids = [msg.ack_id for msg in response.received_messages]
        subscriber.acknowledge(
            request={
            "subscription": subscription_path,
            "ack_ids": ack_ids,
            }
        )