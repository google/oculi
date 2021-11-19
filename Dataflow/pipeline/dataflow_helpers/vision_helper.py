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

"""Helpers for Vision/Video Intelligence API that run on Dataflow."""

import logging

import apache_beam as beam
import json
import copy
from google.api_core.exceptions import ClientError
from google.api_core.exceptions import GoogleAPIError
from google.api_core.exceptions import ServerError
from google.cloud import videointelligence
from google.cloud import vision
from google.protobuf.json_format import MessageToDict


class ExtractImageMetadata(beam.DoFn):
    """Class to process image creatives.

  Server Error codes:
    2 - UNKNOWN (500 Internal Server Error),
    4 - DEADLINE EXCEEDED (504 Gateway Timeout)
    5 - Not Found
    8 - RESOURCE EXHAUSTED (429 Too Many Requests)
    13 - INTERNAL (500 Internal Server Error)
    14 - UNAVAILABLE (503 Service Unavailable)
    15 - DATA LOSS (500 Internal Server Error)
  """

    @beam.utils.retry.with_exponential_backoff(
        initial_delay_secs=10.0, num_retries=3,
        retry_filter=lambda exception: isinstance(exception, ServerError))
    def wrapper_vision_api_call(self, client, image, features, creative_id):
        response = client.annotate_image({"image": image, "features": features})
        error_code = response.error.code
        if error_code:
            log_message = """Found error in API response:
                      code: {0},
                      message: {1},
                      creative_id: {2}""".format(response.error.code,
                                                 response.error.message,
                                                 creative_id)
            if error_code in [2, 4, 5, 8, 13, 14, 15]:
                logging.warning(log_message)
                raise ServerError(log_message)
            else:
                raise ClientError(log_message)
        return response

    def process(self, row):
        gcs_prefix = "https://storage.cloud.google.com"
        creative_id = row["Creative_ID"]
        gs_uri = row["GCS_URL"]
        image = {"source": {"image_uri": gs_uri}}
        features = [{"type": vision.Feature.Type.TEXT_DETECTION},
                    {"type": vision.Feature.Type.IMAGE_PROPERTIES},
                    {"type": vision.Feature.Type.SAFE_SEARCH_DETECTION},
                    {"type": vision.Feature.Type.LABEL_DETECTION},
                    {"type": vision.Feature.Type.LOGO_DETECTION},
                    {"type": vision.Feature.Type.FACE_DETECTION},
                    {"type": vision.Feature.Type.OBJECT_LOCALIZATION}]
        try:
            client = vision.ImageAnnotatorClient()
            response = self.wrapper_vision_api_call(client,
                                                    image, features, creative_id)
            text_annotations = list(map(MessageToDict, response.text_annotations[1:]))
            image_prop_annotation = MessageToDict(
                response.image_properties_annotation)
            safe_search_annotation = MessageToDict(response.safe_search_annotation)
            label_annotations = list(map(MessageToDict, response.label_annotations))
            logo_annotations = list(map(MessageToDict, response.logo_annotations))
            face_annotations = list(map(MessageToDict, response.face_annotations))
            object_annotations = list(map(MessageToDict,
                                          response.localized_object_annotations))
            image_row = {
                "creative_id": creative_id,
                "creative_url": gcs_prefix + gs_uri[4:],
                "text_annotations": text_annotations,
                "image_properties_annotation": image_prop_annotation,
                "safe_search_annotation": safe_search_annotation,
                "label_annotations": label_annotations,
                "logo_annotations": logo_annotations,
                "face_annotations": face_annotations,
                "object_annotations": object_annotations
            }
            yield image_row
        except ClientError as cerr:
            logging.error("Catching client error: %s for creative_id: %d",
                          str(cerr.message),
                          creative_id)
        except ServerError as serr:
            logging.error("Catching server error: %s after 5 retries for id: %d",
                          str(serr.message),
                          creative_id)
        except GoogleAPIError as err:
            logging.error("Catching generic API error: %s for creative_id: %d",
                          str(err.message),
                          creative_id)


class ExtractVideoMetadata(beam.DoFn):
    """Class to process video creatives."""

    @beam.utils.retry.with_exponential_backoff(
        initial_delay_secs=10.0, num_retries=3,
        retry_filter=lambda exception: isinstance(exception, ServerError))
    def wrapper_video_api_call(self, video_client, gs_uri, features,
                               video_context):
        operation = video_client.annotate_video(input_uri=gs_uri, features=features, video_context=video_context)
        # TODO(team): Video jobs currently have an issue where Dataflow autoscales
        # down to 1 worker. Our running theory is that it's caused by this
        # (presumably) long-running synchronous call: when many workers are waiting
        # on these responses, Dataflow sees workers not using CPU and determines
        # they're not needed.
        response = operation.result(timeout=2400)  # 40 mins
        return response

    def process(self, row):
        def fetch_relevant_segment_fields(segment_row):
            new_segment_row = {"confidence": segment_row["confidence"],
                               "segment": segment_row["segment"],
                               "frames": segment_row["frames"]}
            return new_segment_row

        def fetch_relevant_text_fields(row):
            segments_list = row["segments"]
            confidence_list = map(fetch_relevant_segment_fields,
                                  segments_list)
            new_row = {"text": row["text"],
                       "segments": confidence_list}
            return new_row

        def fetch_relevant_label_fields(segment_label_row):
            new_segment_label_row = {"entity": segment_label_row["entity"],
                                     "segments": segment_label_row["segments"]}
            return new_segment_label_row

        gcs_prefix = "https://storage.cloud.google.com"
        creative_id = row["Creative_ID"]
        gs_uri = row["GCS_URL"]

        try:
            video_client = videointelligence.VideoIntelligenceServiceClient()
            features = [videointelligence.enums.Feature.LABEL_DETECTION,
                        videointelligence.enums.Feature.TEXT_DETECTION,
                        videointelligence.enums.Feature.SHOT_CHANGE_DETECTION,
                        videointelligence.enums.Feature.EXPLICIT_CONTENT_DETECTION,
                        videointelligence.enums.Feature.SPEECH_TRANSCRIPTION,
                        videointelligence.enums.Feature.OBJECT_TRACKING
                        ]

            config = videointelligence.types.SpeechTranscriptionConfig(
                language_code="en-US",
                enable_automatic_punctuation=True)
            video_context = videointelligence.types.VideoContext(
                speech_transcription_config=config)

            result = self.wrapper_video_api_call(video_client, gs_uri, features,
                                                 video_context)
            # result_json = result.__class__.to_json(result)
            # result_dict = json.loads(result_json)

            if not result:
                logging.info("Catching empty response for id: %s", creative_id)
                return
            if result.annotation_results[0].speech_transcriptions:
                contains_speech = result.annotation_results[0]
                doesnt_contain_speech = result.annotation_results[1]
            else:
                contains_speech = result.annotation_results[1]
                doesnt_contain_speech = result.annotation_results[0]

            # text_annotations = list(map(MessageToDict,
            #                             doesnt_contain_speech.text_annotations))
            text_annotations = [MessageToDict(n) for n in doesnt_contain_speech.text_annotations]
            # rel_text_annotations = map(fetch_relevant_text_fields, text_annotations)
            rel_text_annotations = [fetch_relevant_text_fields(n) for n in text_annotations]
            # rel_text_annotations = map(fetch_relevant_text_fields,  result_dict['annotationResults'][0]['textAnnotations'])  # text_annotations)

            # segment_label_annotations = result_dict['annotationResults'][0]['segmentLabelAnnotations']
            segment_label_annotations = list(map(MessageToDict,
                                                 doesnt_contain_speech
                                                 .segment_label_annotations))
            # shot_label_annotations = result_dict['annotationResults'][0]['shotLabelAnnotations']
            shot_label_annotations = list(map(MessageToDict,
                                              doesnt_contain_speech
                                              .shot_label_annotations))
            rel_seg_label_annotations = list(map(fetch_relevant_label_fields,
                                                 segment_label_annotations))

            rel_shot_label_annotations = list(map(fetch_relevant_label_fields,
                                                  shot_label_annotations))
            # shot_change_annotations = result_dict['annotationResults'][0]['shotAnnotations']
            shot_change_annotations = list(map(MessageToDict,
                                               doesnt_contain_speech
                                               .shot_annotations))
            explicit_annotation = MessageToDict(doesnt_contain_speech
                                                .explicit_annotation)
            speech_transcription = MessageToDict(contains_speech
                                                 .speech_transcriptions[0])
            #
            if speech_transcription["alternatives"] == [{}]:
                speech_transcription = []

            # object_annotations = result_dict['annotationResults'][0]['objectAnnotations']
            object_annotations = list(map(MessageToDict,
                                          result.annotation_results[0]
                                          .object_annotations))
            logging.info(f"This is it :: {rel_text_annotations}")
            video_row = {
                "creative_id": creative_id,
                "creative_url": gcs_prefix + gs_uri[4:],
                "text_annotations": rel_text_annotations,
                "segment_label_annotations": rel_seg_label_annotations,
                "shot_label_annotations": rel_shot_label_annotations,
                "shot_change_annotations": shot_change_annotations,
                "explicit_annotation": explicit_annotation,
                "speech_transcription": speech_transcription,
                "object_annotations": object_annotations
            }

            yield video_row
        except ClientError as cerr:
            logging.error("Catching client error: %s for creative_id: %d",
                          str(cerr.message),
                          creative_id, exc_info=True)
        except ServerError as serr:
            logging.error("Catching server error: %s after 5 retries for id: %d",
                          str(serr.message),
                          creative_id, exc_info=True)
        except GoogleAPIError as err:
            logging.error("Catching generic API error: %s for creative_id: %d",
                          str(err),
                          creative_id, exc_info=True)
        except Exception as eerr:
            logging.error("Catching generic exception error: %s for creative_id: %d",
                          str(eerr),
                          creative_id, exc_info=True)
