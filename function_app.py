"""
Azure Function triggered by an Event Grid event when a blob is created in a specific storage account.
"""

import os
import logging
import json

import azure.functions as func

from azure.storage.blob import BlobClient
from azure.core.exceptions import (
    HttpResponseError,
    ResourceNotFoundError,
    ResourceExistsError
)
from azure.identity import DefaultAzureCredential

app = func.FunctionApp()


@app.event_grid_trigger(arg_name="event")
@app.queue_ouput(
    arg_name="msg",
    queue_name=os.environ["QUEUE_NAME"],
    connection="AzureWebJobsStorage"
)
def file_drop_trigger(event: func.EventGridEvent, msg: func.out[func.QueueMessage]) -> None:
    """
    Azure Function triggered by a blob creation event via EventGrid.
    
    This function extracts relevant information from the event, formats it as a JSON
    message, and sends it to an Azure Storage Queue.
    
    @params:
    - event (func.EventGridEvent):
        The Event Grid event object triggered by the blob creation.
    - msg (func.out[func.QueueMessage]):
        Output binding for the Azure Storage Queue message.
    
    @returns:
    - None:
        The function sends a formatted message to the queue.
    """
    lease = None
    event_data = event.get_json()
    blob_name = event.subject.split("/")[-1]
    blob_url = event_data.get("url")
    result = json.dumps(
        {
            "id": event.id,
            "source": "file",
            "blob_name": blob_name,
            "blob_url": blob_url,
            "blob_type": event_data.get("blobType"),
            "content_type": event_data.get("contentType"),
            "content_length": event_data.get("contentLength"),
            "topic": event.topic,
            "subkect": event.subject,
            "event_type": event.event_type
        }
    )

    try:
        blob_client = BlobClient.from_blob_url(
            blob_url=blob_url,
            credential=DefaultAzureCredential(_)
        )
        lease = blob_client.acquire_lease()
        msg.set(json.dumps(result))
    except ResourceExistsError:
        logging.info(
            "Another process is currently leasing the blob %s. Exiting function.....",
            blob_name
        )
    except ResourceNotFoundError:
        logging.error("Blob not found: %s. Processing halted", blob_url)
    except Exception as exc:
        logging.error("Unexpected error occurred: %s", str(exc))
    finally:
        if lease and lease is not None:
            lease.release()
