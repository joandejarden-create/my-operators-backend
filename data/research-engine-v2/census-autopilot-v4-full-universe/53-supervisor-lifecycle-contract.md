# Supervisor lifecycle contract

## Allowed supervisor exits

| Condition | Action |
| --- | --- |
| BUILD_COMPLETE (exit 10) | Supervisor exits COMPLETE |
| HARD_CIRCUIT with --exit-on-hard-block | Optional exit 30; default stay alive HARD_BLOCKED |
| FATAL_CONFIGURATION (40) | Exit |
| SIGTERM/SIGINT | Graceful stop, preserve checkpoint |

## Forbidden reasons to exit

- controller completed one run
- one lane finished
- queue drained
- runtime boundary / SESSION_CAP equivalent
- resume ticket exists

## Loop

```
acquire CENSUS_V4_WORKER_LOCK
if waiting retry window → sleep
spawn controller
map exit code → restart / sleep / hard-block / complete
heartbeat every 30s
repeat
```

Controller runtime boundary ⇒ supervisor relaunches controller (exit 0 = CHECKPOINT_CONTINUE).
