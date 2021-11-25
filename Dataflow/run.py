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

"""Starts Oculi jobs from a jobfile, triggering main.py for Dataflow."""

import argparse
import os
import sys

from pipeline.local_helpers.jobfile_helper import open_jobfile
from pipeline.local_helpers.jobfile_helper import validate_jobfile_access
from pipeline.local_helpers.jobfile_helper import validate_jobfile_format

import yaml

DATAFLOW_PYTHON_BINARY = 'python3'
WORKERS = '10'

REQUIREMENTS_PATH = 'pipeline/requirements.txt'
SETUP_PATH = 'pipeline/setup.py'
CREDENTIALS_PATH = 'client_secrets.json'

# temporary file used to transfer jobfile details (possibly modified) to
# main.py, which will then delete this file
TMP_PATH = 'tmp.yaml'

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Start the Oculi pipeline.')
    parser.add_argument('jobfile', action='store',
                        help='path to jobfile (see jobs/sample.yaml)')
    parser.add_argument('--limit', action='store', dest='limit',
                        type=int, default=0,
                        help='cap number of creatives (for testing)')
    parser.add_argument('--local', action='store_true',
                        dest='local', default=False,
                        help='run on local machine (for testing)')
    parser.add_argument('--client', action='store',
                        dest='client', type=str, default="undefined",
                        help='client name. E.g. Coca Cola')
    parser.add_argument('--brand', action='store',
                        dest='brand', type=str, default="undefined",
                        help='brand name. E.g. for Coca Cola it could be Fanta. If no brand set client name')
    parser.add_argument('--business_unit', action='store',
                        dest='business_unit', type=str, default="undefined",
                        help='Business unit. E.g. dk-offices. If no business unit set to client name')

    args = parser.parse_args()

    jobfile = open_jobfile(args.jobfile)
    jobfile = validate_jobfile_format(jobfile)
    jobfile = validate_jobfile_access(jobfile,
                                      credentials_path=CREDENTIALS_PATH)

    # insert runtime details into modified jobfile for main.py
    # main.py will read the jobfile from this tmp_file, then delete it
    jobfile['run_details']['business_unit'] = args.business_unit
    jobfile['run_details']['brand'] = args.brand
    jobfile['run_details']['client'] = args.client
    jobfile['creative_source_details']['limit'] = args.limit
    with open(TMP_PATH, 'w') as tmp_file:
        yaml.safe_dump(jobfile, tmp_file, default_flow_style=False)

    # trigger main.py and Dataflow
    parameters = {
        'python_binary': DATAFLOW_PYTHON_BINARY,
        'runner': 'DirectRunner' if args.local else 'DataflowRunner',
        'num_workers': WORKERS,
        'requirements_path': REQUIREMENTS_PATH,
        'setup_path': SETUP_PATH
    }
    command = ('{python_binary} pipeline/main.py '
               '--runner {runner} '
               '--max_num_workers {num_workers} '
               '--requirements_file {requirements_path} '
               '--setup_file {setup_path}').format(**parameters)
    print(command)
    os.system(command)
