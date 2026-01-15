# Batch vs Stream Processing Analysis

## 1. Latency Comparison

| Mode | Approx. latency to see new data | Comments |
|------|--------------------------------|----------|
| **Batch** | ~17 seconds (full job) | Processes all 10,200 records at once. New data appears only after complete ETL run finishes. For 6,926 valid records, total processing time was 16.71 seconds. |
| **Stream** | 5-15 seconds per micro-batch | Data appears incrementally as micro-batches are processed. With 5-10 second trigger intervals, new records visible within 5-15 seconds of file creation. Processed 10,200 records in ~100 micro-batches over 5 minutes. |

**Key Observations:**
- **Batch**: Single execution, all-or-nothing. If a new order arrives during processing, it must wait for the next batch run.
- **Stream**: Continuous processing with 5-10 second triggers. Each micro-batch (100 rows) processed independently, providing near-real-time visibility.
- **Watermark Impact**: 30-minute watermark in streaming handles late arrivals, but data delayed beyond 30 minutes is dropped.

---
## 2. Complexity Comparison

### What was easier: Batch
**Advantages:**
-  Simple error handling - separate quarantine files for invalid data
-  Easy to separate cancelled orders into dedicated directory
-  Can inspect intermediate DataFrames during development
-  Straightforward deduplication with `dropDuplicates()`
-  Complete output mode works everywhere (files, memory, console)
-  No state management concerns
-  Clear execution model: read → transform → write

### What was harder: Stream
**Challenges:**
- ️ Cannot easily write quarantine during processing - must filter out bad data
- ️ `dropDuplicates` requires watermark on event-time column for state cleanup
- ️ Stateful operations need memory management and checkpointing
- ️ Watermark configuration critical - too short drops valid late data, too long consumes memory
- ️ Complete output mode NOT supported with file sinks
- ️ Must use `foreachBatch` for complex outputs (partitioning, metrics logging)
- ️ Debugging harder - cannot easily inspect intermediate DataFrames
- ️ Need multiple queries for multiple outputs (console + file + metrics)

### Streaming Limitations Encountered

1. **No Easy Quarantine**: Invalid records are filtered out, not saved. To save them, would need a separate streaming query.

2. **Watermark Required for Deduplication**:
```python
   df = df.withWatermark("order_timestamp", "30 minutes")
   df = df.dropDuplicatesWithinWatermark(["order_id"])
```

3. **Complete Mode Restrictions**: Running totals per product work only with memory/console sinks, not files. Had to create CSV snapshots via `foreachBatch`.

4. **State Management**: Aggregations consume memory. Watermark helps cleanup old state but requires careful tuning.

5. **Multiple Queries Needed**: Required 4 separate streaming queries:
   - Console sink (windowed revenue)
   - File sink (windowed revenue with partitioning)
   - Memory sink (product totals)
   - CSV snapshots (product totals)

---
## 3. Performance & Resources

| Mode | Input rows | Processing time | Output rows | Notes |
|------|-----------|-----------------|-------------|-------|
| **Batch** | 10,200 | 16.71 seconds | 6,926 valid | Single execution. 1,068 quarantined, 2,047 cancelled, 159 duplicates removed |
| **Stream** | 10,200 | ~306 seconds (5+ min) | 6,926 valid | 100 micro-batches, each 100 rows. Processed incrementally with 5-10s trigger intervals |

### Detailed Breakdown:

**Batch Processing:**
- Read + validate + transform + aggregate + write: **16.71 seconds**
- Memory usage: Peak during joins and aggregations
- Resource efficiency: Process once, minimal overhead
- Output: 4 analytical tables (orders, daily_revenue, product_revenue, hourly_revenue)

**Stream Processing:**
- Total runtime: **306.93 seconds** (~5 minutes)
- Number of micro-batches: **53 batches** (console sink)
- Average batch processing: ~5-6 seconds per batch
- Trigger interval: 5-10 seconds between batches
- Output: Windowed aggregations (10-minute windows), running totals, CSV snapshots
---

## 4. When Would You Choose Each?

### Choose **Batch** When:

1. **Historical Analysis**
   - Daily/weekly/monthly reports
   - Example: Generate end-of-day financial summary at midnight
   - Why: Complete dataset available, no need for real-time updates

2. **Complex Joins and Aggregations**
   - Data warehouse ETL pipelines
   - Example: Monthly customer segmentation with 100+ features
   - Why: Easier to implement complex transformations without state management

3. **Cost Optimization**
   - Processing large datasets once per day
   - Example: Process 1TB of logs every night during off-peak hours
   - Why: Run only when needed, minimal infrastructure costs

4. **Data Quality Focus**
   - Quarantine and manual review workflows
   - Example: Regulatory compliance reporting with audit trails
   - Why: Easy to separate, inspect, and reprocess bad data

### Choose **Stream** When:

1. **Real-Time Monitoring**
   - Live dashboards and KPIs
   - Example: Website traffic monitoring with 1-minute refresh
   - Why: Immediate visibility into current state

2. **Time-Sensitive Decisions**
   - Fraud detection, anomaly detection
   - Example: Flag suspicious credit card transaction within 5 seconds
   - Why: Every second counts to prevent losses

3. **Continuous Data Arrival**
   - IoT sensors, clickstream, application logs
   - Example: Process sensor data from 10,000 devices 24/7
   - Why: Data never stops, batch windows are artificial

4. **Event-Driven Workflows**
   - Trigger actions based on events
   - Example: Send alert when order volume drops 50% in 10 minutes
   - Why: React immediately to patterns in data

### Hybrid Approach (Lambda Architecture):

**Use Both** when you need:
- **Fast + Accurate**: Real-time dashboard (stream) + nightly corrections (batch)
- **Hot + Cold**: Recent data in stream, historical data in batch
- **Example**: 
  - Stream: Show sales in last hour (approximate, fast)
  - Batch: Correct daily totals overnight (accurate, complete)

---