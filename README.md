# Goanna

Tenant observability for the Mulga stack: per-VM metrics, a CloudWatch-compatible API, CloudWatch Logs and alarms.

Goanna is the **tenant** plane. It stores and serves only what a customer owns, scoped to their account. Operator and host telemetry never enters it, so it cannot leak through the CloudWatch API — fleet monitoring is a separate stack.

**Status: Phase 1.** Ingest and storage only. There is no public API yet.

## What it does today

`goannad` subscribes to the per-VM metric batches spinifex publishes on `metrics.ec2.<instance-id>` and appends them to an embedded Prometheus TSDB. It serves liveness, readiness and status endpoints; there is no CloudWatch API yet.

```
spinifex qmp-collector ──NATS──▶ JetStream ──▶ goannad ──▶ TSDB
```

Eight series per instance, on the 60s or 300s EC2 monitoring tiers:

```
goanna_ec2_cpu_utilization      goanna_ec2_disk_read_bytes    goanna_ec2_disk_read_ops
goanna_ec2_network_in_bytes     goanna_ec2_disk_write_bytes   goanna_ec2_disk_write_ops
goanna_ec2_network_out_bytes    goanna_ec2_memory_actual_bytes
```

## Independence

Goanna has **no Go module dependency on spinifex, and spinifex has none on Goanna.** Every boundary is a protocol:

| Boundary | Protocol |
|---|---|
| ingest | NATS `metrics.ec2.<instance-id>`, JSON batches |
| query | CloudWatch + CW Logs API, SigV4 over HTTPS (Phase 2) |
| cold tier | S3 to predastore (Phase 3) |
| alarm actions | SNS-over-NATS (Phase 4) |

This is why `internal/wire` declares its own decode structs rather than importing spinifex's. The contract is the JSON on the subject, and the agreement is held by a fixture **captured from the live subject** — not one built from the producer's types. A hand-built fixture agrees on field names while saying nothing about their units, and that gap is exactly how a seconds timestamp reached a millisecond appender. Recapture from a running node when the producer changes.

`ts` on the wire is Unix **seconds**. The TSDB indexes milliseconds, so `Batch.TimestampMS` is the only thing that may reach an appender, and `Validate` rejects a timestamp outside plausible-seconds range so a producer switching units fails loudly instead of filing every sample in the year 58000.

Goanna also owns the JetStream stream definition. JetStream captures a message by subject regardless of which API published it, so the producer's existing core-NATS publish lands in the stream unchanged and no spinifex change is needed for durability or replay. `TestCorePublishIsCapturedByTheStream` is that claim as a test.

## Running

```bash
make build
./bin/goannad --nats-url nats://127.0.0.1:4222 --data-dir /var/lib/goanna/tsdb
```

| Flag | Environment | Default |
|---|---|---|
| `--nats-url` | `GOANNA_NATS_URL` | `nats://127.0.0.1:4222` |
| `--nats-token` | `GOANNA_NATS_TOKEN` | — |
| `--data-dir` | `GOANNA_DATA_DIR` | `/var/lib/goanna/tsdb` |
| `--stream` | `GOANNA_STREAM` | `GOANNA_METRICS` |
| `--health-addr` | `GOANNA_HEALTH_ADDR` | `127.0.0.1:8445` |
| `--log-level` | `GOANNA_LOG_LEVEL` | `info` |
| `--retention` | — | `360h` (15d) |

## Endpoints

| Path | Meaning |
|---|---|
| `/healthz` | liveness. Always 200 while the process serves. Checks nothing else, so a NATS outage does not trigger a restart. |
| `/readyz` | 200 when NATS is connected and the TSDB is readable, 503 otherwise, naming the failing check. |
| `/statusz` | version, uptime, ingest counters and TSDB head stats. Diagnostic only. |

Readiness deliberately excludes metric freshness. A node with no running guests
produces no batches, and that is healthy rather than broken.

## Storage

The hot tier is a local TSDB. predastore is the cold tier, not a replacement:
S3 has no append, so a 60s batch per instance would mean roughly 1,440 objects
per instance per day. The local head batches into blocks that get shipped whole,
which is the Thanos/Mimir shape with our own S3 underneath.

**Phase 1 limitation:** the hot tier is unreplicated and lives on one node. The
JetStream stream retains raw batches for 24h, so a rebuild inside a day replays
clean. Past that there is no recovery until the block flush lands in Phase 3.

## Dependencies

Importing `prometheus/prometheus/tsdb` pulls `prometheus/config`, which reaches
every service-discovery provider — Kubernetes and Azure SDK packages end up
linked into the binary. Nothing calls them; `tsdb.DB.ApplyConfig` exists for the
Prometheus server's YAML reload and Go compiles at package granularity, so there
is no import that avoids it. `.github/dependabot.yml` scopes alerts to direct
dependencies, and `govulncheck` in preflight catches anything reachable.

## Development

```bash
make preflight    # lint + govulncheck + tests + coverage; must pass before committing
make test
make fix          # auto-fix lint issues
```
