# Week 10 local failure matrix

All tests were performed on isolated/local state. This is not a production failover certification.

| Scenario | Injection | Observation | Result |
|---|---|---|---|
| Duplicate event | 2% duplicate replay and 10 extra replayed source rows after restart | 999 Silver rows = 999 distinct transaction IDs; restart batch added zero fresh rows | PASS — [stream verifier](stream-failure.json) |
| Late event | Send withheld first event after watermark advanced | `LATE_EVENT` appears in DLQ with source trace; withheld ID absent from Silver | PASS — [stream verifier](stream-failure.json) |
| Unsupported schema | Producer `--mode invalid` | `UNSUPPORTED_SCHEMA_VERSION` in DLQ | PASS — [stream verifier](stream-failure.json) |
| Malformed JSON | Producer `--mode malformed` | `MALFORMED_JSON` in DLQ | PASS — [stream verifier](stream-failure.json) |
| Spark restart | Stop and restart with same warehouse, checkpoint, and stream ID | Nine completed batches; 999 distinct Silver IDs; duplicate replay added zero | PASS — [stream verifier](stream-failure.json) |
| PostgreSQL paused | Stop the project Postgres container, execute incoming quality gate, restart it | Gate exited 1 with `WAREHOUSE_UNAVAILABLE`; after restart gate passed and Gold remained 100,000 unique IDs | PASS — [before](pg-before-quality.json), [paused](pg-paused-quality.json), [recovered](pg-recovered-quality.json) |
| Model inference timeout/busy worker | Test a 100 ms model under a 10 ms deadline and one bounded slot | `/score` HTTP 200, `degraded=true`, `MODEL_INFERENCE_TIMEOUT`, rule-only fallback | PASS — [unit test](../../tests/unit/test_week10_api_timeout.py) |

The API benchmark is separate from the forced timeout test. Its second run had seven degraded responses, but that initial benchmark version did not record reason codes; their exact cause is unknown. Do not relabel those seven as proven timeouts. The model timeout does not interrupt a hung Python thread; production serving would require process isolation or a remote service with circuit breaking.
