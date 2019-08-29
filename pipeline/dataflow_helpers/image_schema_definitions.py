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

"""Schema definitions for tables with Vision API output."""

field_common = [{
    "name": "creative_id",
    "type": "INTEGER",
    "mode": "NULLABLE"
}, {
    "name": "creative_url",
    "type": "STRING",
    "mode": "NULLABLE"
}]

field_color = [{
    "name":
        "color",
    "type":
        "RECORD",
    "mode":
        "NULLABLE",
    "fields": [{
        "name": "red",
        "type": "FLOAT",
        "mode": "NULLABLE"
    }, {
        "name": "green",
        "type": "FLOAT",
        "mode": "NULLABLE"
    }, {
        "name": "blue",
        "type": "FLOAT",
        "mode": "NULLABLE"
    }]
}, {
    "name": "score",
    "type": "FLOAT",
    "mode": "NULLABLE"
}, {
    "name": "pixelFraction",
    "type": "FLOAT",
    "mode": "NULLABLE"
}]

field_dom_colors = [{
    "name":
        "dominantColors",
    "type":
        "RECORD",
    "mode":
        "NULLABLE",
    "fields": [{
        "name": "colors",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": field_color
    }]
}]
schema_image_prop = {
    "fields": [{
        "name": "creative_id",
        "type": "INTEGER",
        "mode": "NULLABLE"
    }, {
        "name": "creative_url",
        "type": "STRING",
        "mode": "NULLABLE"
    }, {
        "name": "image_properties_annotation",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": field_dom_colors
    }]
}

field_bounding_poly = {
    "name":
        "boundingPoly",
    "type":
        "RECORD",
    "mode":
        "NULLABLE",
    "fields": [{
        "name":
            "vertices",
        "type":
            "RECORD",
        "mode":
            "REPEATED",
        "fields": [{
            "name": "x",
            "type": "INTEGER",
            "mode": "NULLABLE"
        }, {
            "name": "y",
            "type": "INTEGER",
            "mode": "NULLABLE"
        }]
    }]
}
schema_text_annotations = {
    "fields": [{
        "name": "creative_id",
        "type": "INTEGER",
        "mode": "NULLABLE"
    }, {
        "name": "creative_url",
        "type": "STRING",
        "mode": "NULLABLE"
    }, {
        "name":
            "text_annotations",
        "type":
            "RECORD",
        "mode":
            "REPEATED",
        "fields": [{
            "name": "description",
            "type": "STRING",
            "mode": "NULLABLE"
        }, field_bounding_poly]
    }]
}

schema_safe_search_annotations = {"fields":
                                      field_common +
                                      [{"fields": [{"mode": "NULLABLE",
                                                    "name": "medical",
                                                    "type": "STRING"},
                                                   {"mode": "NULLABLE",
                                                    "name": "spoof",
                                                    "type": "STRING"},
                                                   {"mode": "NULLABLE",
                                                    "name": "violence",
                                                    "type": "STRING"},
                                                   {"mode": "NULLABLE",
                                                    "name": "adult",
                                                    "type": "STRING"},
                                                   {"mode": "NULLABLE",
                                                    "name": "racy",
                                                    "type": "STRING"}],
                                        "mode": "NULLABLE",
                                        "name": "safe_search_annotation",
                                        "type": "RECORD"}]}

schema_label_annotations = {"fields":
                                field_common +
                                [{"name": "label_annotations",
                                  "type": "RECORD",
                                  "mode": "REPEATED",
                                  "fields": [{"name": "score",
                                              "type": "FLOAT",
                                              "mode": "NULLABLE"},
                                             {"name": "topicality",
                                              "type": "FLOAT",
                                              "mode": "NULLABLE"},
                                             {"name": "mid",
                                              "type": "STRING",
                                              "mode": "NULLABLE"},
                                             {"name": "description",
                                              "type": "STRING",
                                              "mode": "NULLABLE"}]}]
                           }

field_vertices = [{"name": "y", "type": "INTEGER", "mode": "NULLABLE"},
                  {"name": "x", "type": "INTEGER", "mode": "NULLABLE"}]

schema_logo_annotations = {"fields":
                               field_common +
                               [{"name": "logo_annotations",
                                 "type": "RECORD",
                                 "mode": "REPEATED",
                                 "fields": [{"name": "score", "type": "FLOAT",
                                             "mode": "NULLABLE"},
                                            {"name": "mid", "type": "STRING",
                                             "mode": "NULLABLE"},
                                            {"name": "description",
                                             "type": "STRING",
                                             "mode": "NULLABLE"},
                                            {"name": "boundingPoly",
                                             "type": "RECORD",
                                             "mode": "NULLABLE",
                                             "fields": [{"name": "vertices",
                                                         "type": "RECORD",
                                                         "mode": "REPEATED",
                                                         "fields":
                                                             field_vertices}]
                                            }]
                                }]
                          }

schema_face = {"fields":
                   field_common +
                   [{"name": "face_annotations", "type": "RECORD",
                     "mode": "REPEATED",
                     "fields": [{"name": "headwearLikelihood", "type": "STRING",
                                 "mode": "NULLABLE"},
                                {"name": "panAngle",
                                 "type": "FLOAT", "mode": "NULLABLE"},
                                {"name": "underExposedLikelihood",
                                 "type": "STRING", "mode": "NULLABLE"},
                                {"name": "landmarkingConfidence",
                                 "type": "FLOAT", "mode": "NULLABLE"},
                                {"name": "detectionConfidence",
                                 "type": "FLOAT", "mode": "NULLABLE"},
                                {"name": "joyLikelihood", "type": "STRING",
                                 "mode": "NULLABLE"},
                                {"name": "sorrowLikelihood",
                                 "type": "STRING", "mode": "NULLABLE"},
                                {"name": "surpriseLikelihood", "type": "STRING",
                                 "mode": "NULLABLE"},
                                {"name": "tiltAngle",
                                 "type": "FLOAT", "mode": "NULLABLE"},
                                {"name": "angerLikelihood", "type": "STRING",
                                 "mode": "NULLABLE"},
                                {"name": "rollAngle", "type": "FLOAT",
                                 "mode": "NULLABLE"},
                                {"name": "blurredLikelihood", "type": "STRING",
                                 "mode": "NULLABLE"},
                                {"name": "boundingPoly", "type": "RECORD",
                                 "mode": "NULLABLE",
                                 "fields": [{"name": "vertices",
                                             "type": "RECORD",
                                             "mode": "REPEATED",
                                             "fields": [{"name": "y",
                                                         "type": "INTEGER",
                                                         "mode": "NULLABLE"},
                                                        {"name": "x",
                                                         "type": "INTEGER",
                                                         "mode": "NULLABLE"}
                                                        ]}]},
                                {"name": "fdBoundingPoly", "type": "RECORD",
                                 "mode": "NULLABLE",
                                 "fields": [{"name": "vertices",
                                             "type": "RECORD",
                                             "mode": "REPEATED",
                                             "fields": [{"name": "y",
                                                         "type": "INTEGER",
                                                         "mode": "NULLABLE"},
                                                        {"name": "x",
                                                         "type": "INTEGER",
                                                         "mode": "NULLABLE"}]}]},
                                {"name": "landmarks", "type": "RECORD",
                                 "mode": "REPEATED",
                                 "fields": [{"name": "type",
                                             "type": "STRING",
                                             "mode": "NULLABLE"},
                                            {"name": "position",
                                             "type": "RECORD",
                                             "mode": "NULLABLE",
                                             "fields": [{"name": "y",
                                                         "type": "FLOAT",
                                                         "mode": "NULLABLE"},
                                                        {"name": "x",
                                                         "type": "FLOAT",
                                                         "mode": "NULLABLE"},
                                                        {"name": "z",
                                                         "type": "FLOAT",
                                                         "mode": "NULLABLE"}]
                                            }]
                                 }]
                    }]
              }

schema_object = {"fields":
                     field_common +
                     [{"name": "object_annotations", "type": "RECORD",
                       "mode": "REPEATED",
                       "fields": [{"name": "score", "type": "FLOAT",
                                   "mode": "NULLABLE"},
                                  {"name": "mid",
                                   "type": "STRING",
                                   "mode": "NULLABLE"},
                                  {"name": "name",
                                   "type": "STRING",
                                   "mode": "NULLABLE"},
                                  {"name": "boundingPoly",
                                   "type": "RECORD",
                                   "mode": "NULLABLE",
                                   "fields":
                                       [{"name": "normalizedVertices",
                                         "type": "RECORD", "mode": "REPEATED",
                                         "fields": [{"name": "y",
                                                     "type": "FLOAT",
                                                     "mode": "NULLABLE"},
                                                    {"name": "x",
                                                     "type": "FLOAT",
                                                     "mode": "NULLABLE"}]
                                        }]
                                  }]
                      }]
                }

def get_table_schema(endpoint):
  table_schema_map = {
      "image_properties_annotation": schema_image_prop,
      "text_annotations": schema_text_annotations,
      "safe_search_annotation": schema_safe_search_annotations,
      "label_annotations": schema_label_annotations,
      "logo_annotations": schema_logo_annotations,
      "face_annotations": schema_face,
      "object_annotations": schema_object
  }
  return table_schema_map[endpoint]
