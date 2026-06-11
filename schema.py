DRILLING_LOG_SCHEMA = """
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "DrillingLog",
  "type": "object",
  "properties": {
    "sequence_number":  { "type": "integer" },
    "well_uid":         { "type": "string" },
    "wellbore_uid":     { "type": "string" },
    "log_uid":          { "type": "string" },
    "rig_id":           { "type": "string" },
    "index_type":       { "type": "string" },
    "index_mnemonic":   { "type": "string" },
    "mnemonic_list":    { "type": "array", "items": { "type": "string" } },
    "unit_list":        { "type": "array", "items": { "type": "string" } },
    "data": {
      "type": "array",
      "items": {},
      "minItems": 9,
      "maxItems": 9
    }
  },
  "required": [
    "sequence_number",
    "well_uid",
    "wellbore_uid",
    "log_uid",
    "rig_id",
    "index_type",
    "index_mnemonic",
    "mnemonic_list",
    "unit_list",
    "data"
  ],
  "additionalProperties": false
}
"""