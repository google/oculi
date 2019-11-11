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

"""Helpers for ingesting a jobfile that run on the local machine."""

from __future__ import print_function

import os

from cm_helper import init_cm
from cm_helper import retry

import yaml


def open_jobfile(filepath, delete=False):
  """Attempts to open a jobfile from the specified path, optionally deleting it.

  Args:
    filepath: path to jobfile.yaml
    delete: whether to try to delete the file

  Returns:
    jobfile dictionary
  """

  with open(filepath, 'r') as jobfile_raw:
    jobfile = yaml.safe_load(jobfile_raw)
  if delete:
    try:
      os.remove(filepath)
    except (OSError, IOError) as e:
      print(e)

  return jobfile


def validate_jobfile_format(jobfile):
  """Takes a jobfile dictionary and returns it if formatted properly.

  Args:
    jobfile: dictionary after file ingestion

  Returns:
    jobfile dictionary

  Raises:
    ValueError
  """

  first_level_fields = [
      'job_name', 'job_type',
      'creative_source_type', 'creative_source_details',
      'data_destination', 'run_details'
  ]
  for attr in first_level_fields:
    if attr not in jobfile:
      msg = '{} missing from jobfile, refer to sample.yaml'.format(attr)
      raise ValueError(msg)

  job_types = ['image', 'video']
  if jobfile['job_type'] not in job_types:
    msg = 'job_type {} not recognized, must be one of {}'.format(
        jobfile['job_type'], job_types)
    raise ValueError(msg)

  creative_source_fields = {
      'bigquery': ['gcp_project', 'bq_dataset', 'bq_table'],
      'cm': ['cm_account_id', 'cm_profile_id', 'start_date', 'end_date'],
      'gcs': ['gcs_bucket', 'gcp_project']
  }
  if jobfile['creative_source_type'] not in creative_source_fields:
    msg = 'creative_source_type {} not recognized, must be one of {}'.format(
        jobfile['creative_source_type'], creative_source_fields.keys())
    raise ValueError(msg)
  for attr in creative_source_fields[jobfile['creative_source_type']]:
    if attr not in jobfile['creative_source_details']:
      msg = 'need {} to read from {}, refer to sample.yaml'.format(
          attr, jobfile['creative_source_type'])
      raise ValueError(msg)

  second_level_fields = {
      'data_destination': ['gcp_project', 'bq_dataset', 'gcs_bucket'],
      'run_details': ['gcp_project', 'temp_location', 'staging_location']
  }
  for field in second_level_fields:
    for attr in second_level_fields[field]:
      if attr not in jobfile[field]:
        msg = 'need {} in {}, refer to sample.yaml'.format(attr, field)
        raise ValueError(msg)

  return jobfile


def validate_jobfile_access(jobfile, credentials_path):
  """Takes a jobfile dictionary and returns it if formatted properly.

  Args:
    jobfile: dictionary after file ingestion
    credentials_path: filepath to client_secrets.json

  Returns:
    jobfile dictionary

  Raises:
    ValueError
  """

  if jobfile['creative_source_type'] == 'bigquery':
    pass  # TODO(team)
  elif jobfile['creative_source_type'] == 'gcs':
    pass  # TODO(team)
  elif jobfile['creative_source_type'] == 'cm':
    specified_profile = int(jobfile['creative_source_details']['cm_profile_id'])
    specified_account = int(jobfile['creative_source_details']['cm_account_id'])

    cm = init_cm(credentials_path)
    profiles = retry(cm.userProfiles().list())['items']
    found_profile = None

    for profile in profiles:
      if int(profile['profileId']) == specified_profile:
        found_profile = profile
        break

    if found_profile and int(found_profile['accountId']) != specified_account:
      msg = 'CM user profile {} belongs to account {}, but account {} specified'
      msg = msg.format(specified_profile, found_profile['accountId'],
                       specified_account)
      raise ValueError(msg)
    if not found_profile:
      if not profiles:
        msg = 'the service account has no CM profiles'
      else:
        msg = 'CM profile {} either doesn\'t exist, '.format(specified_profile)
        msg += 'or doesn\'t belong to this service account'
      raise ValueError(msg)

  # TODO(team): validate data_destination and run_details also

  return jobfile
