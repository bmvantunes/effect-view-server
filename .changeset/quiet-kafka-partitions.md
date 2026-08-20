---
"effect-view-server": patch
---

Reacquire configured Kafka start offsets when topics gain partitions at runtime, preserving already-pulled live records while preventing new-partition rows from being silently skipped from `LATEST`.
