---
"effect-view-server": major
---

Require `kafka.json` callers to provide a lazy `Schema.toCodecJson(RowSchema)` factory, remove the JSON codec's runtime `schema` field, and direct non-canonical Kafka wire formats to typed custom codecs.
