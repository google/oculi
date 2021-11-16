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

OCULI_PYTHON="python3"
OCULI_ENV_PATH=./"Envs" # no trailing slash, glob chars outside quotes
OCULI_ENV_NAME="oculi-venv"

if [ ! -d $OCULI_ENV_PATH ]; then
  echo "Creating new folder: $OCULI_ENV_PATH"
  mkdir $OCULI_ENV_PATH
fi

if [ ! -d $OCULI_ENV_PATH/$OCULI_ENV_NAME ]; then
  echo "Creating new virtual env: $OCULI_ENV_NAME"
  $OCULI_PYTHON -m virtualenv $OCULI_ENV_PATH/$OCULI_ENV_NAME
else
  echo "Found existing virtual env: $OCULI_ENV_NAME"
fi

# Activate the virtual env in the current shell. This requires the script
# to be run as 'source presetup_virtualenv.sh' or '. presetup_virtualenv.sh'.
echo "Activating virtual env..."
source $OCULI_ENV_PATH/$OCULI_ENV_NAME/bin/activate
