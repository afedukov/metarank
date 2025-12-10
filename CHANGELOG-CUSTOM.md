# Metro Markets Custom Changelog

This changelog tracks custom modifications and features developed by Metro Markets for our internal fork of [Metarank](https://github.com/metarank/metarank).

- **Base version**: Metarank 0.7.11
- **Fork maintained by**: Andrey Fedyukov <andrey.fedyukov@metro-markets.de> (Search Team)

For upstream Metarank changes, see [CHANGELOG.md](CHANGELOG.md) or the [official releases](https://github.com/metarank/metarank/releases).

---

## 0.7.14-custom (2025-12-10)

### Problem
Products with zero impressions (cold start) received `NaN` for all rate features (CTR, cart_rate, purchase_rate), causing ML model to rank them extremely low regardless of other signals like exact brand match. This created a "chicken and egg" problem: no impressions → no CTR data → low rank → no impressions → endless cycle. New products or products from small sellers were effectively invisible in search results even when they perfectly matched user queries.

Additionally, during investigation we discovered a critical integer division bug affecting **all products**: `globalRate` calculation used `Long / Long` instead of `Double / Double`, causing loss of precision. For example, `980642 / 453937 = 2` (integer) instead of `2.160` (correct), resulting in inflated CTR values by ~30-50% for all products. This affected ranking accuracy globally.

### Solution
* **Cold start handling for rate features**: Item-level counters (clicks/impressions) are now optional when normalization is enabled - missing counters default to 0 instead of causing `NaN`
* **Fallback to global CTR**: Products without historical data now receive global average CTR calculated from global counters: `(weight + 0) / (weight * globalRate + 0)`
* **Integer division bug fix**: Fixed critical bug where `globalRate` was calculated using integer division (`Long / Long`), causing all products to have inflated CTR values by ~30-50%. Now uses correct float division (`Double / Double`) with explicit `.toDouble` conversion
* **Division by zero protection**: Added safety check `if (globalClicks > 0.0)` to handle edge case when global statistics are not yet available (cold start scenario)
* **Preserved existing behavior**: Products with historical data continue using the same normalization formula; only products with missing item-level counters are affected

### Result
New products and products without impressions now start with reasonable baseline CTR (global average) instead of `NaN`, giving them a fair chance to be shown and accumulate real engagement data. This solves the cold start problem for rate-based features while maintaining accurate ranking for products with sufficient historical data. Additionally, all products now have more accurate CTR values due to the integer division bug fix - CTR values decreased by ~5-30% to correct levels, improving overall ranking precision. **Note:** Model retraining is required after deployment to adapt to the new feature scale.

### Technical Details
**Formula** (with normalization enabled):
```
item_ctr = (weight + itemClicks) / (weight * (globalImpressions / globalClicks) + itemImpressions)

When itemClicks = 0 and itemImpressions = 0:
item_ctr = weight / (weight * globalRate) = globalCTR
```

**Example** (Spain production data, 90d period):
- Global: 453,937 clicks / 980,642 impressions = 46.29% CTR
- New product: (50 + 0) / (50 * 2.160 + 0) = 46.29% CTR (global average)
- Product with data: uses standard normalized formula (unchanged)

**Integer Division Bug Fix:**
```scala
// OLD CODE (bug):
globalRate = bottomGlobalNum.values(i).value / topGlobalNum.values(i).value
// 980642 / 453937 = 2 (Long/Long = integer division, lost precision!)

// NEW CODE (fixed):
val globalClicks = topGlobalNum.values(i).value.toDouble
val globalImpressions = bottomGlobalNum.values(i).value.toDouble
globalRate = globalImpressions / globalClicks
// 980642.0 / 453937.0 = 2.160 (Double/Double = correct!)
```

**Impact on CTR calculation:**
- Old (wrong): denominator = 50 * 2 + itemImpressions → inflated CTR
- New (correct): denominator = 50 * 2.160 + itemImpressions → accurate CTR
- Result: CTR values decreased by ~5-30% for all products with data (more accurate)

Files changed: `RateFeature.scala`

---

## 0.7.13-custom (2025-11-12)

### Problem
Kafka consumer permanently stopped consuming events when Redis restarted. Offsets were committed immediately after poll, before Redis write completed, leading to data loss for critical item events (product updates). After failure, consumer did not recover until manual restart.

### Solution
* **Delayed offset commit**: Offsets now committed only after successful processing and Redis write (chunk-level commit after entire poll batch processed)
* **Supervisor pattern**: Automatic stream restart with exponential backoff (5s, 10s, ..., max 60s) on failures
* **Poison pill protection**: Bad JSON records skipped without stopping stream (using explicit `++ Stream.exec` sequencing to ensure offset commit after each poll batch)
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
