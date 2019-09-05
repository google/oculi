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

"""Helpers for CM API and creative extraction that run on the local machine."""

from __future__ import print_function

import datetime
import time

from googleapiclient import discovery
from googleapiclient.errors import HttpError
from google.oauth2 import service_account

LOCAL_CLIENT_SECRETS = 'client_secrets.json'


def init_cm(credentials_path):
  """Initializes the Campaign Manager API.

  Args:
    credentials_path: filepath to client_secrets.json

  Returns:
    CM service object

  Raises:
    ValueError
  """
  api_name = 'dfareporting'
  api_version = 'v3.3'
  oauth_scopes = ['https://www.googleapis.com/auth/dfatrafficking']

  try:
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path)
  except IOError:
    msg = 'no or invalid credentials found at {}, '.format(credentials_path)
    msg += 'have you run setup_environment.sh?'
    raise ValueError(msg)

  credentials = credentials.with_scopes(oauth_scopes)
  service = discovery.build(api_name, api_version, credentials=credentials)

  return service


def retry(request, retries=4):
  """Retries a standard CM request for appropriate error codes.

  Args:
    request: CM request (before .execute())
    retries: cap on retries

  Returns:
    response

  Raises:
    HttpError: if exceeded
  """
  for this_retry in range(retries):
    try:
      return request.execute()
    except HttpError as e:
      if this_retry == retries - 1 or e.resp.status not in [403, 429, 500, 503]:
        raise
      wait = 10 * 2**this_retry
      time.sleep(wait)


def fetch_cm_creatives(credentials_path, cm_profile_id, job_type,
                       start_date=False, end_date=False, limit=False):
  """Fetches creatives using the CM API.

  Args:
    credentials_path: path to client_secrets.json
    cm_profile_id: Campaign Manager User Profile with read access to creativesi
    job_type: 'image' or 'video'
    start_date: filter on "Last Modified" in CM
    end_date: filter on "Last Modified" in CM
    limit: optional numerical limit for number of creatives

  Returns:
    list of dicts, each with keys Creative_ID, Advertiser_ID, Creative_Name,
      Full_URL
  """

  cm = init_cm(LOCAL_CLIENT_SECRETS)

  creatives = []
  request = cm.creatives().list(profileId=cm_profile_id)
  print('fetching creatives...')
  while True:
    print('fetching next page...')
    response = retry(request)
    creatives += response['creatives']
    if 'nextPageToken' in response:
      request = cm.creatives().list_next(request, response)
    else:
      break
    if limit and len(creatives) >= limit:
      break

  if limit:
    creatives = creatives[:limit]
  print('fetched {} creatives'.format(len(creatives)))

  out_creatives = []

  for creative in creatives:
    if 'creativeAssets' not in creative:
      continue

    # convert ms since epoch to date
    last_modified = datetime.date.fromtimestamp(
        int(creative['lastModifiedInfo']['time']) / 1000)
    if start_date and last_modified < start_date:
      continue
    if end_date and last_modified > end_date:
      continue

    assets = creative['creativeAssets']
    assets.sort(reverse=True, key=(lambda asset: asset['fileSize']))

    accepted_formats = {
        'video': ['mp4', 'mov', 'wmv', 'm4v', 'webm'],
        'image': ['jpg', 'png', 'gif', 'jpeg']
    }

    url = None
    for asset in assets:
      # check two special cases for video creatives first, in which case
      # the URL is easy to get
      if job_type == 'video' and 'progressiveServingUrl' in asset:
        url = asset['progressiveServingUrl']
        break
      elif job_type == 'video' and 'streamingServingUrl' in asset:
        url = asset['streamingServingUrl']
        break
      # otherwise, for image creatives or video creatives not captured by the
      # above, try a reconstructed URL and check the file extension
      else:
        reconstructed_url = 'https://s0.2mdn.net/{}/{}'.format(
            creative['advertiserId'], asset['assetIdentifier']['name'])
        extension = reconstructed_url.split('.')[-1].lower()
        if extension in accepted_formats[job_type]:
          url = reconstructed_url
          break

    if url:
      out_creatives.append({
          'Creative_ID': creative['id'],
          'Advertiser_ID': creative['advertiserId'],
          'Creative_Name': creative['name'],
          'Full_URL': url
      })

  return out_creatives
