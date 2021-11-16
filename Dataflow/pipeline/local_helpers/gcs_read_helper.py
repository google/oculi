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

"""Helpers for GCS API and creative extraction that run on the local machine."""

from __future__ import print_function

from google.cloud import storage
from google.oauth2 import service_account

LOCAL_CLIENT_SECRETS = 'client_secrets.json'


def init_gcs(credentials_path, gcp_project):
    """Initializes the GCS API.

  Args:
    credentials_path: filepath to client_secrets.json
    gcp_project: for project holding GCS bucket

  Returns:
    GCS service object

  Raises:
    ValueError
  """

    try:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path)
    except IOError:
        msg = 'no or invalid credentials found at {}, '.format(credentials_path)
        msg += 'have you run setup_environment.sh?'
        raise ValueError(msg)

    service = storage.Client(project=gcp_project, credentials=credentials)

    return service


def fetch_gcs_creatives(credentials_path, gcp_project, gcs_bucket,
                        job_type, limit=False):
    """Fetches creatives using the GCS API.

  Args:
    credentials_path: path to client_secrets.json
    gcp_project: holding gcs_bucket
    gcs_bucket: bucket name
    job_type: 'image' or 'video'
    limit: optional numerical limit for number of creatives

  Returns:
    list of dicts (keys: Creative_ID and GCS_URL)
  """

    print('fetching creatives from GCS...')
    gcs = init_gcs(credentials_path, gcp_project)
    bucket = gcs.get_bucket(gcs_bucket)
    blobs = bucket.list_blobs()

    gcs_creatives = []
    for blob in blobs:
        if limit and len(gcs_creatives) >= limit:
            break

        # skip folders
        if blob.name[-1] == '/':
            continue

        # check format
        accepted_formats = {
            'video': ['mp4', 'mov', 'wmv', 'm4v', 'webm'],
            'image': ['jpg', 'png', 'gif', 'jpeg']
        }
        extension = blob.name.split('.')[-1].lower()
        if extension not in accepted_formats[job_type]:
            print('skipping unsuitable format: {}'.format(extension))
            continue

        # extract creative ID
        try:
            # check if filename is {creative_id}_{other_stuff}.jpg
            creative_id = int(blob.name.split('_')[0])
        except (KeyError, ValueError):
            # hash the filename
            creative_id = hash(blob.name)

        # save creative
        gcs_creatives.append({
            'Creative_ID': creative_id,
            'GCS_URL': u'gs://{}/{}'.format(gcs_bucket, blob.name)
        })

    return gcs_creatives
