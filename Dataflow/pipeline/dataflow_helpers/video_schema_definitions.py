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

"""Schema definitions for tables with Video Intelligence API output."""

field_common = [{
    "name": "creative_id",
    "type": "INTEGER",
    "mode": "NULLABLE"
}, {
    "name": "creative_url",
    "type": "STRING",
    "mode": "NULLABLE"
}]

field_bounding_box = [{"name": "vertices", "type": "RECORD", "mode": "REPEATED",
                       "fields": [{"name": "x", "type": "FLOAT",
                                   "mode": "NULLABLE"},
                                  {"name": "y", "type": "FLOAT",
                                   "mode": "NULLABLE"}]
                       }]

field_segments = [{"name": "confidence", "type": "FLOAT", "mode": "NULLABLE"},
                  {"name": "segment", "type": "RECORD", "mode": "NULLABLE",
                   "fields": [{"name": "startTimeOffset",
                               "type": "STRING", "mode": "NULLABLE"},
                              {"name": "endTimeOffset",
                               "type": "STRING", "mode": "NULLABLE"}]
                   },
                  {"name": "frames", "type": "RECORD", "mode": "REPEATED",
                   "fields": [{"name": "timeOffset",
                               "type": "STRING", "mode": "NULLABLE"},
                              {"name": "rotatedBoundingBox",
                               "type": "RECORD", "mode": "NULLABLE",
                               "fields": field_bounding_box
                               }]
                   }]

field_labels = [{"name": "entity", "type": "RECORD", "mode": "NULLABLE",
                 "fields": [{"name": "languageCode", "type": "STRING",
                             "mode": "NULLABLE"},
                            {"name": "entityId", "type": "STRING",
                             "mode": "NULLABLE"},
                            {"name": "description",
                             "type": "STRING", "mode": "NULLABLE"}]
                 },
                {"name": "segments", "type": "RECORD", "mode": "REPEATED",
                 "fields": [{"name": "confidence", "type": "FLOAT",
                             "mode": "NULLABLE"},
                            {"name": "segment", "type": "RECORD",
                             "mode": "NULLABLE",
                             "fields": [{"name": "startTimeOffset",
                                         "type": "STRING",
                                         "mode": "NULLABLE"},
                                        {"name": "endTimeOffset",
                                         "type": "STRING",
                                         "mode": "NULLABLE"}
                                        ]
                             }
                            ]
                 }]
schema_seg_label = {
    "fields": field_common +
              [{"name": "segment_label_annotations", "type": "RECORD",
                "mode": "REPEATED",
                "fields": field_labels
                }]
}

schema_shot_label = {
    "fields": field_common +
              [{"name": "shot_label_annotations", "type": "RECORD",
                "mode": "REPEATED",
                "fields": field_labels
                }]
}
schema_text_annotations = {
    "fields": field_common +
              [{"name": "text_annotations", "type": "RECORD",
                "mode": "REPEATED",
                "fields": [{"name": "text", "type": "STRING",
                            "mode": "NULLABLE"},
                           {"name": "segments", "type": "RECORD",
                            "mode": "REPEATED",
                            "fields": field_segments}]
                }]
}
schema_shot_change = {
    "fields": field_common +
              [{"name": "shot_change_annotations", "type": "RECORD",
                "mode": "REPEATED",
                "fields": [{"name": "startTimeOffset", "type": "STRING",
                            "mode": "NULLABLE"},
                           {"name": "endTimeOffset", "type": "STRING",
                            "mode": "NULLABLE"}
                           ]
                }]
}

schema_explicit = {
    "fields": field_common +
              [{"name": "explicit_annotation", "type": "RECORD",
                "mode": "NULLABLE",
                "fields": [{"name": "frames", "type": "RECORD",
                            "mode": "REPEATED",
                            "fields": [{"name": "timeOffset",
                                        "type": "STRING",
                                        "mode": "NULLABLE"},
                                       {"name": "pornographyLikelihood",
                                        "type": "STRING", "mode": "NULLABLE"}]
                            }]
                }]
}

schema_objects = {
    "fields": field_common +
              [{"name": "object_annotations", "type": "RECORD",
                "mode": "REPEATED",
                "fields": [{"name": "confidence", "type": "FLOAT",
                            "mode": "NULLABLE"},
                           {"name": "segment", "type": "RECORD",
                            "mode": "NULLABLE",
                            "fields": [{"name": "endTimeOffset",
                                        "type": "STRING",
                                        "mode": "NULLABLE"},
                                       {"name": "startTimeOffset",
                                        "type": "STRING",
                                        "mode": "NULLABLE"}]},
                           {"name": "entity", "type": "RECORD",
                            "mode": "NULLABLE",
                            "fields": [{"name": "languageCode",
                                        "type": "STRING",
                                        "mode": "NULLABLE"},
                                       {"name": "entityId",
                                        "type": "STRING",
                                        "mode": "NULLABLE"},
                                       {"name": "description",
                                        "type": "STRING",
                                        "mode": "NULLABLE"}]},
                           {"name": "frames", "type": "RECORD",
                            "mode": "REPEATED",
                            "fields": [{"name": "timeOffset", "type": "STRING",
                                        "mode": "NULLABLE"},
                                       {"name": "normalizedBoundingBox",
                                        "type": "RECORD", "mode": "NULLABLE",
                                        "fields": [{"name": "top",
                                                    "type": "FLOAT",
                                                    "mode": "NULLABLE"},
                                                   {"name": "right",
                                                    "type": "FLOAT",
                                                    "mode": "NULLABLE"},
                                                   {"name": "bottom",
                                                    "type": "FLOAT",
                                                    "mode": "NULLABLE"},
                                                   {"name": "left",
                                                    "type": "FLOAT",
                                                    "mode": "NULLABLE"}]
                                        }]
                            }]
                }]
}

schema_speech = {
    "fields": field_common +
              [{"name": "speech_transcription", "type": "RECORD",
                "mode": "NULLABLE",
                "fields": [{"name": "languageCode", "type": "STRING",
                            "mode": "NULLABLE"},
                           {"name": "alternatives", "type": "RECORD",
                            "mode": "REPEATED",
                            "fields": [{"name": "confidence", "type": "FLOAT",
                                        "mode": "NULLABLE"},
                                       {"name": "transcript",
                                        "type": "STRING", "mode": "NULLABLE"},
                                       {"name": "words", "type": "RECORD",
                                        "mode": "REPEATED",
                                        "fields": [{"name": "endTime",
                                                    "type": "STRING",
                                                    "mode": "NULLABLE"},
                                                   {"name": "word",
                                                    "type": "STRING",
                                                    "mode": "NULLABLE"},
                                                   {"name": "startTime",
                                                    "type": "STRING",
                                                    "mode": "NULLABLE"}]
                                        }]
                            }]
                }]
}


def get_table_schema(endpoint):
    """Get the relevant table schema for the specified endpoint.

    Args:
      endpoint: Vision API endpoint.
    Returns:
      Relevant table schema for the endpoint.
    """
    table_schema_map = {
        "text_annotations": schema_text_annotations,
        "segment_label_annotations": schema_seg_label,
        "shot_label_annotations": schema_shot_label,
        "shot_change_annotations": schema_shot_change,
        "explicit_annotation": schema_explicit,
        "object_annotations": schema_objects,
        "speech_transcription": schema_speech
    }
    return table_schema_map[endpoint]