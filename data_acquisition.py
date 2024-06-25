import functions_framework
from google.cloud import (storage, bigquery)
import google.cloud.logging
import os
import logging
import json

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
    
@functions_framework.cloud_event
def copy_source_file_to_destination_bucket(cloud_event):
    source_bucket = cloud_event.data["bucket"]
    source_object = cloud_event.data["name"]
    source_object_filename = cloud_event.data["name"].split("/")[-1]
    source_project = env_vars("PROJECT_ID")
    ROUTING_REFERENTIAL_TABLE_ID = env_vars("ROUTING_REFERENTIAL_TABLE_ID")

    google.cloud.logging.Client(project=source_project).setup_logging()

 
    # reading referential table from Big Query in order to know where to upload the file
    client = bigquery.Client()

    QUERY = (
        f"""
        SELECT * FROM {source_project}.{ROUTING_REFERENTIAL_TABLE_ID} 
        WHERE REGEXP_CONTAINS('{source_object_filename}', source_file_mask) is true
        """)
    query_job = client.query(QUERY)  # API request
    rows = list(query_job.result())  # Waits for query to finish

    if len(rows) == 1:
        row = rows[0]
        logging.info("File mask matched one line : " + str(row))
        destination_project = row["destination_project"]
        destination_bucket = row["destination_bucket"]
        destination_directory = row["destination_directory"]
        transfer_file(source_project, source_bucket, source_object, source_object_filename, destination_project, destination_bucket, destination_directory)
    elif len(rows) >= 2:
        logging.error(f"Error: More than 1 line matched File mask {source_object_filename}")
    else:
        logging.error(f"Error: Matching File Mask for {source_object_filename} not found in referential")
