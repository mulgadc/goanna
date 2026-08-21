# Goanna

Tenant observability for the Mulga stack: per-VM metrics, a CloudWatch-compatible API, CloudWatch Logs and alarms.

Goanna is the **tenant** plane. It stores and serves only what a customer owns, scoped to their account. Operator and host telemetry never enters it, so it cannot leak through the CloudWatch API — fleet monitoring is a separate stack.

**Status: Phase 1.** Ingest and storage only. There is no public API yet.

## What it does today

`goannad` subscribes to the per-VM metric batches spinifex publishes on `metrics.ec2.<instance-id>` and appends them to an embedded Prometheus TSDB.

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

This is why `internal/wire` declares its own decode structs rather than importing spinifex's. The contract is the JSON on the subject; the agreement is held by a fixture generated from the producer's own types, not by a shared package.

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
| `--log-level` | `GOANNA_LOG_LEVEL` | `info` |
| `--retention` | — | `360h` (15d) |

## Development

```bash
make preflight    # lint + govulncheck + tests + coverage; must pass before committing
make test
make fix          # auto-fix lint issues
```
