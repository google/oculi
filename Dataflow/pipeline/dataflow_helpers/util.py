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

"""Pipeline utilities intended to run on Dataflow."""

import apache_beam as beam


class FilterAPIOutput(beam.DoFn):
    """Filtering API output."""

    def process(self, row, endpoint):
        if row[endpoint]:
            field_selector = ["creative_id", "creative_url", endpoint]
            filtered_output = {key: row[key] for key in field_selector}
            yield filtered_output
