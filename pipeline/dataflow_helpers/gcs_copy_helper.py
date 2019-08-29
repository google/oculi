# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Cloud Storage helper functions that run on Dataflow."""

import logging
import apache_beam as beam
import requests
from requests.exceptions import ConnectionError
from requests.exceptions import HTTPError
from requests.exceptions import RequestException
from google.api_core.exceptions import GoogleAPIError
from google.api_core.exceptions import ServerError
from google.auth.exceptions import GoogleAuthError
from google.cloud import storage


class UploadToGcs(beam.DoFn):
  """Class to upload to GCS."""

  @beam.utils.retry.with_exponential_backoff(
      initial_delay_secs=10.0, num_retries=3,
      retry_filter=lambda exception: isinstance(exception, ConnectionError))
  def wrapper_requests_call(self, asset_url, creative_id):
    request = requests.get(asset_url)
    status_code = request.status_code
    if status_code == 200:
      content = request.content
      return content
    elif status_code > 500:
      log_message = """Requests server error-
      status_code: {0}, creative_id: {1}""".format(status_code, creative_id)
      logging.warning(log_message)
      raise ConnectionError(log_message)
    else:
      log_message = """Requests HTTP error-
      status_code: {0}, creative_id: {1}""".format(status_code, creative_id)
      logging.warning(log_message)
      raise HTTPError(log_message)

  @beam.utils.retry.with_exponential_backoff(
      initial_delay_secs=10.0, num_retries=3,
      retry_filter=lambda exception: isinstance(exception, ServerError)
      or isinstance(exception, ConnectionError))
  def wrapper_gcs_upload(self, gcp_project, gcs_bucket_name, job_name,
                         job_type, file_name,
                         advertiser_id, asset_byte_string, creative_id):
    storage_client = storage.Client(project=gcp_project)
    bucket = storage_client.get_bucket(gcs_bucket_name)
    gcs_file_name = "{0}/{1}/{2}/{3}".format(job_name, advertiser_id,
                                             job_type, file_name)
    blob = bucket.blob(gcs_file_name)
    content_type = "image/jpg" if job_type == "image" else "video/mp4"
    blob.upload_from_string(data=asset_byte_string, content_type=content_type)

    gcs_url = "{0}{1}/{2}".format("gs://", gcs_bucket_name, gcs_file_name)
    uploaded_asset_details = {"Creative_ID": creative_id, "GCS_URL": gcs_url}
    return uploaded_asset_details

  def process(self, row, gcp_project, gcs_bucket_name, job_name, job_type):
    """Upload to GCS.

    Args:
      row: Sample row.
      gcp_project: GCP project for GCS bucket
      gcs_bucket_name: GCS bucket name
      job_name: user-readable name from jobfile
      job_type: Job Type
    Yields:
      A sample row (keys: 'Creative_ID' and 'GCS_URL' w/ gs:// prefix)
    """
    creative_id = row["Creative_ID"]
    advertiser_id = row["Advertiser_ID"]
    file_name = row["Creative_Name"] 
    asset_url = row["Full_URL"]

    # Download the asset to memory
    try:
      asset_content = self.wrapper_requests_call(asset_url, creative_id)
    except requests.exceptions.ConnectionError as conn_error:
      log_message = """Requests, connection error, after 3 retries -
      error: {0}, creative_id: {1}""".format(str(conn_error), creative_id)
      logging.error(log_message)
      return
    except RequestException as req_error:
      log_message = """Requests, generic error -
      error: {0}, creative_id: {1}""".format(str(req_error), creative_id)
      logging.error(log_message)
      return

    asset_byte_string = bytes(asset_content)
    try:
      uploaded_row = self.wrapper_gcs_upload(gcp_project, gcs_bucket_name,
                                             job_name, job_type,
                                             file_name, advertiser_id,
                                             asset_byte_string,
                                             creative_id)
      yield uploaded_row
    except GoogleAPIError as err_gapi:
      logging.error("""Google API Error after retries: %s,
                    Error type: %s creative_id: %s""",
                    err_gapi,
                    str(type(err_gapi)),
                    str(creative_id))
      return
    except GoogleAuthError as err_gauth:
      logging.error("""Google Auth Error after retries: %s,
                    Error type: %s creative_id: %s""",
                    err_gauth,
                    str(type(err_gauth)),
                    str(creative_id))
      return
    except RequestException as err_req:
      logging.error("""Request Error after retries: %s,
                    Error type: %s creative_id: %s""",
                    err_req,
                    str(type(err_req)),
                    str(creative_id))
      return
    except Exception as err:
      logging.error("""Undefined Error after retries: %s,
                    Error type: %s creative_id: %s""",
                    err,
                    str(type(err)),
                    str(creative_id))
      return

