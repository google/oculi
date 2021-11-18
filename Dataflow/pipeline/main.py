# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Controller code for the Dataflow pipeline, including local setup."""

from __future__ import print_function

import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions

from dataflow_helpers import gcs_copy_helper
from dataflow_helpers import image_schema_definitions
from dataflow_helpers import util
from dataflow_helpers import video_schema_definitions
from dataflow_helpers import vision_helper

from local_helpers import cm_helper
from local_helpers import gcs_read_helper
from local_helpers import jobfile_helper

TMP_PATH = 'tmp.yaml'
CREDENTIALS_PATH = 'client_secrets.json'

root = logging.getLogger()
root.setLevel(logging.INFO)

# read jobfile details from the temporary file produced by run.py
# this also attempts to delete the file
jobfile = jobfile_helper.open_jobfile(TMP_PATH, delete=True)
print(jobfile)

# set options for Dataflow session
options = PipelineOptions(region='us-central1')
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.job_name = jobfile['job_name']
google_cloud_options.project = jobfile['run_details']['gcp_project']
google_cloud_options.temp_location = jobfile['run_details']['temp_location']
google_cloud_options.staging_location = (
    jobfile['run_details']['staging_location'])

write_disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE
create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED

image_endpoints = ['image_properties_annotation', 'text_annotations',
                   'safe_search_annotation', 'label_annotations',
                   'logo_annotations', 'face_annotations', 'object_annotations']

video_endpoints = ['text_annotations', 'segment_label_annotations',
                   'shot_label_annotations', 'shot_change_annotations',
                   'explicit_annotation',
                   'object_annotations', 'speech_transcription']

# assemble Dataflow graph
p = beam.Pipeline(options=options)

creative_limit = jobfile['creative_source_details']['limit']

if jobfile['creative_source_type'] == 'bigquery':
    source_bq_table = '{0}.{1}.{2}'.format(
        jobfile['creative_source_details']['gcp_project'],
        jobfile['creative_source_details']['bq_dataset'],
        jobfile['creative_source_details']['bq_table'])
    read_query = 'SELECT * FROM `{0}` {1}'.format(
        source_bq_table,
        'LIMIT {}'.format(creative_limit) if creative_limit else '')

    rows = (
            p
            | 'Read creatives from BQ table' >> beam.io.Read(
        beam.io.BigQuerySource(
            query=read_query,
            use_standard_sql=True)))

elif jobfile['creative_source_type'] == 'cm':
    cm_creatives = cm_helper.fetch_cm_creatives(
        CREDENTIALS_PATH,
        jobfile['creative_source_details']['cm_profile_id'],
        jobfile['job_type'],
        jobfile['creative_source_details']['start_date'],
        jobfile['creative_source_details']['end_date'],
        limit=creative_limit)

    rows = (
            p
            | 'Pull creatives from CM' >> beam.Create(cm_creatives))

if jobfile['creative_source_type'] in ['bigquery', 'cm']:
    upload = (
            rows
            | 'Copy assets to GCS' >> beam.ParDo(
        gcs_copy_helper.UploadToGcs(),
        gcp_project=jobfile['data_destination']['gcp_project'],
        gcs_bucket_name=jobfile['data_destination']['gcs_bucket'],
        job_name=jobfile['job_name'],
        job_type=jobfile['job_type']))

elif jobfile['creative_source_type'] == 'gcs':
    gcs_creatives = gcs_read_helper.fetch_gcs_creatives(
        CREDENTIALS_PATH,
        jobfile['creative_source_details']['gcp_project'],
        jobfile['creative_source_details']['gcs_bucket'],
        jobfile['job_type'],
        limit=creative_limit)

    upload = (
            p
            | 'Read creatives from GCS' >> beam.Create(gcs_creatives))

job_type = jobfile['job_type']
destination_bq_dataset = '{0}:{1}'.format(
    jobfile['data_destination']['gcp_project'],
    jobfile['data_destination']['bq_dataset'])

if job_type == 'image':
    annotated_creatives = (
            upload
            | 'Annotate image creatives' >> beam.ParDo(
        vision_helper.ExtractImageMetadata()))

    for endpoint in image_endpoints:
        filtered_output = (
                annotated_creatives
                | 'Extract {0}'.format(endpoint) >> beam.ParDo(
            util.FilterAPIOutput(), endpoint=endpoint))

        write_to_bq = (
                filtered_output
                | 'Write {0} to BQ'.format(endpoint)
                >> beam.io.WriteToBigQuery(
            project='cloud-in-a-box',
            table='{0}.{1}'.format(destination_bq_dataset, endpoint),
            schema=image_schema_definitions.get_table_schema(endpoint=endpoint),
            write_disposition=write_disposition,
            create_disposition=create_disposition))

else:
    annotated_creatives = (
            upload
            | 'Annotate video creatives' >> beam.ParDo(
        vision_helper.ExtractVideoMetadata()))

    for endpoint in video_endpoints:
        print(f"!!!!!!!!!!!!!!!!!!!! Endpoint {endpoint}")
        filtered_output = (
                annotated_creatives
                | 'Extract {0}'.format(endpoint) >> beam.ParDo(
            util.FilterAPIOutput(), endpoint=endpoint))

        write_to_bq = (
                filtered_output
                | 'Write {0} to BQ'.format(endpoint)
                >> beam.io.WriteToBigQuery(
            project='cloud-in-a-box',
            table='{0}.{1}'.format(destination_bq_dataset, endpoint),
            schema=video_schema_definitions.get_table_schema(endpoint),
            write_disposition=write_disposition,
            create_disposition=create_disposition))
try:
    results = p.run()
    print('Pipeline started. If running on Dataflow, check the Dataflow console.')
except Exception as ex:
    logging.error(f"{str(ex)}",  exc_info=True)