# Metro Markets Custom Changelog

This changelog tracks custom modifications and features developed by Metro Markets for our internal fork of [Metarank](https://github.com/metarank/metarank).

- **Base version**: Metarank 0.7.11
- **Fork maintained by**: Andrey Fedyukov <andrey.fedyukov@metro-markets.de> (Search Team)

For upstream Metarank changes, see [CHANGELOG.md](CHANGELOG.md) or the [official releases](https://github.com/metarank/metarank/releases).

---

## 0.7.13-custom (2025-11-12)

### Problem
Kafka consumer permanently stopped consuming events when Redis restarted. Offsets were committed immediately after poll, before Redis write completed, leading to data loss for critical item events (product updates). After failure, consumer did not recover until manual restart.

### Solution
* **Delayed offset commit**: Offsets now committed only after successful processing and Redis write (chunk-level commit after entire poll batch processed)
* **Supervisor pattern**: Automatic stream restart with exponential backoff (5s, 10s, ..., max 60s) on failures
* **Poison pill protection**: Bad JSON records skipped without stopping stream (using `onFinalizeWeak` to ensure offset commit after each poll batch, safe even on stream cancellation)
* **Rebalance safety**: Synchronous offset commit during partition rebalance with try/catch protection
* **Kafka offset semantics**: Commit offset+1 (next message to read) following Kafka best practices
* **Graceful shutdown**: Kubernetes `terminationGracePeriodSeconds` and `preStop` hook for proper cleanup

### Result
At-least-once delivery for all events, automatic recovery from Redis failures, no data loss on Redis restart, minimal duplicates (only on actual failures).

### Configuration Requirements
**Kafka** (required): Set `enable.auto.commit: "false"` in input options
**Redis** (recommended): Reduce command timeout from 300s to 30s (state) / 10s (train) for faster failure detection

Files changed: `KafkaSource.scala`, `Serve.scala`, `MetarankFlow.scala`, Kubernetes configs

---

## 0.7.12-custom (2025-10-31)

### Problem
Redis write operations were silently failing when pipeline mode was disabled (`pipeline.enabled: false`). Commands were not executed and data was lost without any errors or warnings. Model persistence to Redis also lacked proper error handling and diagnostic logging.

### Solution
* **Fixed Redis pipeline**: Added fallback to direct command execution when pipelining disabled (commands now actually execute via `IO.fromCompletableFuture`)
* **Improved model persistence**: Added comprehensive error handling and logging for model save operations (before/after write, error details)

### Result
Redis writes work correctly regardless of pipeline configuration. Model persistence failures are now properly logged and reported instead of silently failing.

Files changed: `RedisClient.scala`, `RedisModelStore.scala`

---

## 0.7.11-custom (2025-07-14)

### Feature: Pre-encoded Vectors Support

Added `preencoded` parameter to `field_match` bi-encoder features, enabling direct use of pre-computed embeddings without requiring model inference.

**Use case**: When item/ranking embeddings are already computed externally (e.g., by data pipeline), you can now use them directly for cosine similarity calculation without loading ONNX models.

**Example**:
```yaml
- name: query_item_image_cos
  type: field_match
  rankingField: ranking.embedding
  itemField: item.image_embedding
  distance: cos
  preencoded: true      # Use pre-computed vectors directly
  ttl: 90d
  method:
    type: bi-encoder
    dim: 512
```

**Changes**:
* Added `preencoded: true` option to bi-encoder field_match features
* Support for `ScalarField` with `SDoubleList` (Array[Double]) in events
* Improved binary serialization/deserialization for vector fields
* Removed requirement for model when using pre-encoded vectors

**Result**: Reduced inference latency and resource usage when embeddings are pre-computed. No ONNX model loading required for pre-encoded mode.

Files changed: `FieldMatchBiencoderFeature.scala`, `FieldCodec.scala`, `Field.scala`, `Scalar.scala`, `EncoderConfig.scala`, `RankingEventFormat.scala`

---

## Base: Metarank 0.7.11

Fork point from upstream Metarank. See [upstream changelog](https://github.com/metarank/metarank/releases/tag/v0.7.11) for details.
