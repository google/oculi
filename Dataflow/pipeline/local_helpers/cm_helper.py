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

import random
import time

from googleapiclient import discovery
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from ssl import SSLError

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
            wait = 10 * 2 ** this_retry
            time.sleep(wait)
        except SSLError as e:
            if this_retry == retries - 1 or 'timed out' not in e.message:
                raise
            wait = 10 * 2 ** this_retry
            time.sleep(wait)


def fetch_cm_creatives(credentials_path, cm_profile_id, job_type,
                       start_date=False, end_date=False, limit=False):
    """Fetches creatives using the CM API.

  Args:
    credentials_path: path to client_secrets.json
    cm_profile_id: Campaign Manager User Profile with read access to creatives
    job_type: 'image' or 'video'
    start_date: filter on last changelog timestamp
    end_date: filter on last changelog timestamp
    limit: optional numerical limit for number of creatives

  Returns:
    list of dicts, each with keys Creative_ID, Advertiser_ID, Creative_Name,
      Full_URL
  """

    cm = init_cm(credentials_path)

    # This extraction is done in three steps:
    # 1. Search changelogs for all creatives modified in date range
    # In batches of 500 (maximum filter size for creative IDs):
    # 2. Fetch creative objects matching those IDs
    # 3. Extract URLs from each batch of creatives
    # Step 1 is done because a date filter isn't available in the API's creative
    # endpoint, and step 2 is done to reduce the number of API calls vs. 1 per
    # creative.

    # STEP 1: search changelogs
    print('fetching creative updates from changelogs...')

    # make time/date strings for filtering changelogs
    zero_time = "T00:00:00-00:00"  # UTC
    start_str = None if start_date is None else str(start_date) + zero_time
    end_str = None if end_date is None else str(end_date) + zero_time

    # assemble request
    request = cm.changeLogs().list(profileId=cm_profile_id,
                                   objectType='OBJECT_CREATIVE',
                                   minChangeTime=start_str,
                                   maxChangeTime=end_str)

    # paginate
    creative_ids = set()
    start = time.time()
    page = 0
    while True:
        page += 1
        print('fetching page {}, time: {}...'
              .format(page, time.time() - start))
        response = retry(request)

        # collect creative IDs from changelog response
        for changelog in response['changeLogs']:
            creative_ids.add(changelog['objectId'])

        if 'nextPageToken' in response:
            request = cm.changeLogs().list_next(request, response)
        else:
            break
        if limit and len(creative_ids) >= limit:
            break

    creative_ids = list(creative_ids)
    if limit and len(creative_ids) > limit:
        creative_ids = creative_ids[:limit]
    print('found {} creatives modified in date range'.format(len(creative_ids)))

    # BATCH: define batches for steps 2 & 3
    batch_size = 500
    out_creatives = []  # final output collection
    for i in range(0, len(creative_ids), batch_size):
        print('processing creative batch {} to {}...'.format(i, i + batch_size))

        # STEP 2: assemble creative list
        print('fetching creative details...')
        batch_cids = creative_ids[i:i + batch_size]
        request = cm.creatives().list(profileId=cm_profile_id, ids=batch_cids)
        response = retry(request)
        creatives = response['creatives']

        # STEP 3: extract asset URLs
        print('extracting URLs...')
        for creative in creatives:
            if 'creativeAssets' not in creative:
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
                        creative['advertiserId'],
                        asset['assetIdentifier']['name'].encode('utf-8'))
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

    print('found {} creatives with suitable assets'.format(len(out_creatives)))
    return out_creatives
