# Runbook: hotel-view-sink consumer lag

## Trigger
Consumer lag is above the agreed threshold for a sustained period.

## User impact
Hotel-price materialized views may become stale.

## First checks
1. Confirm total lag and per-partition lag.
2. Check producer rate.
3. Check sink replica count and pod health.
4. Compare active consumers with partition count.
5. Check sink logs and processing latency.
6. Check Redis availability and latency.
7. Check Kafka broker health, ISR, and partition leaders.
8. Check recent deployments/config changes.

## Recovery options
- Restore failed downstream dependency.
- Roll back a bad sink deployment.
- Let KEDA scale within the partition limit.
- Reduce processing bottleneck.
- Rebalance/restart only when justified.
- Add partitions only after considering key ordering and operational impact.

## Evidence to save
- consumer-group describe output
- Kafka topic describe output
- pod state/restarts
- KEDA/HPA state
- CPU/memory
- relevant logs
- timeline of changes

## After recovery
- verify lag returns to zero
- verify materialized view freshness
- document root cause
- create prevention action
