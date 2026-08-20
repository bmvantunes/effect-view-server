---
"effect-view-server": patch
---

Reacquire configured Kafka start offsets when topics gain partitions at runtime, preventing new-partition rows from being silently skipped from `LATEST`.
