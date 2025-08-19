#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e
set -x

#
# Documentation for detect-secrets is here:
# https://github.com/IBM/detect-secrets/blob/master/docs/scan.md
#
function install_detect_secrets {
  # Detect secrets installation used for scanning.
  pip install --upgrade "git+https://github.com/ibm/detect-secrets.git@master#egg=detect-secrets"
  # For running the pre-commit hook.
  pip install pre-commit
  echo "Installing the pre-commit hook..."
  pre-commit install
}

install_detect_secrets
