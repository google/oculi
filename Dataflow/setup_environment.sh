#!/bin/bash

###########################################################################
#
#  Copyright 2019 Google Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
###########################################################################

SVC_ACCOUNT_NAME='Compute Engine default service account'
LOCAL_CREDS_FILE='client_secrets.json'
TIMEOUT_SECONDS=5

# Check gcloud settings and give the user a chance to exit and reconfigure
PROJECT=$(gcloud config get-value core/project 2>/dev/null)
USER=$(gcloud config get-value core/account 2>/dev/null)

echo "You are logged in as '$USER' to Cloud Project '$PROJECT'."
echo "To reconfigure, exit with Ctrl+C and run 'gcloud init'."
echo "Proceeding in $TIMEOUT_SECONDS seconds..."
sleep $TIMEOUT_SECONDS

# Check if service account key exists
if [ -f $LOCAL_CREDS_FILE ]; then
  echo "Local credentials already exist in '$LOCAL_CREDS_FILE'."
else
  COMMAND="gcloud iam service-accounts list \
    --format='value(email)' --filter='displayName:$SVC_ACCOUNT_NAME'"
  SVC_ACCOUNT=$(eval $COMMAND) # using eval here due to nested quotes and vars
  echo "Found service account: $SVC_ACCOUNT"

  # If not, create one
  echo "Creating new credentials..."
  gcloud iam service-accounts keys create \
    --iam-account $SVC_ACCOUNT $LOCAL_CREDS_FILE
  echo "Saved local credentials to '$LOCAL_CREDS_FILE'."
fi

# Install requirements
echo "Installing requirements with pip..."
sleep 2
pip install -r pipeline/requirements.txt

# Set default credentials (needed for running locally)
export GOOGLE_APPLICATION_CREDENTIALS=$(pwd)/$LOCAL_CREDS_FILE
echo "Setting default credentials to $GOOGLE_APPLICATION_CREDENTIALS"
gcloud auth activate-service-account --key-file=$LOCAL_CREDS_FILE
