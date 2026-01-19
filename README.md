# E-Commerce Data Processing: Batch vs Stream

A comprehensive PySpark project demonstrating the differences between batch and streaming data processing using an e-commerce orders dataset.
---

##  Project Overview

This project implements **two separate data processing pipelines** using PySpark:

1. **Batch ETL Pipeline** - Processes historical orders data in bulk
2. **Streaming ETL Pipeline** - Processes orders data in near-real-time using Spark Structured Streaming

Both pipelines apply the same business logic (cleaning, validation, enrichment, aggregations) to demonstrate the trade-offs between batch and stream processing approaches.

### Dataset
- **E-commerce orders** with 10,000+ records
- Fields: `order_id`, `customer_id`, `product_id`, `order_timestamp`, `quantity`, `price`, `status`
- Intentional data quality issues: missing IDs, negative values, duplicates, late arrivals
- **Dimension tables**: customers (1,000 records), products (50 records)

---

##  Features

### Batch Processing
- ✅ Read all historical data at once from CSV files
- ✅ Explicit schema definition (no schema inference)
- ✅ Data quality checks with quarantine files for invalid records
- ✅ Separate handling of cancelled orders
- ✅ Deduplication based on `order_id`
- ✅ Enrichment with customer and product dimensions
- ✅ Multiple aggregations: daily revenue, product revenue, hourly revenue, top 10 products
- ✅ Partitioned Parquet output by `order_date`
- ✅ Comprehensive metrics logging

### Stream Processing
- ✅ Read streaming data from file source (simulates real-time ingestion)
- ✅ Same data cleaning and validation as batch
- ✅ Watermark-based deduplication (30-minute watermark)
- ✅ Time-based windowed aggregations (10-minute windows)
- ✅ Running totals per product
- ✅ Multiple sinks: console (debugging), file (Parquet), memory (aggregations), CSV snapshots
- ✅ Checkpoint management for fault tolerance
- ✅ Late data handling with watermarking

### Kafka Streaming (Bonus)
- ✅ Production-grade event streaming with Apache Kafka
- ✅ KRaft mode (no Zookeeper dependency)
- ✅ Real-time message processing (1-3 second latency)
- ✅ Message persistence and replay capability
- ✅ Kafka UI for monitoring and debugging
- ✅ Same transformations as file-based streaming
- ✅ Consumer group management
- ✅ Horizontal scalability

### Common Components
- 🔧 Synthetic data generator with configurable data quality issues
- 🔧 Event generator for streaming simulation (splits data into micro-batches)
- 🔧 Shared schema definitions for consistency
- 🔧 YAML-based configuration
- 🔧 Comprehensive documentation comparing batch vs stream

---

##  Requirements

### Software
- **Python**: 3.11
- **PySpark**: 4.0.1
- **Java JDK**: 17 (Temurin recommended)
- **Hadoop utilities**: winutils.exe and hadoop.dll (Windows only)
- **Docker Desktop**: For Kafka integration (optional)

### Python Dependencies
```
pyspark==4.0.1
pyyaml==6.0.1
pandas==2.3.3
kafka-python==2.0.2
```

### Environment Variables

**Windows:**
```bash
# Java
set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-17.x.x

# Hadoop (for Windows)
set HADOOP_HOME=C:\hadoop
set PATH=%PATH%;C:\hadoop\bin
```

**macOS/Linux:**
```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home
```

---

## Project Structure
```
batch_vs_stream/
│
├── docker-compose.yml           # Kafka + Kafka UI setup (KRaft mode)
├── config/
│   └── config.yaml              # All configuration (paths, Spark, Kafka settings)
│
├── data/
│   ├── batch/                   # Input CSV files for batch processing
│   ├── stream/
│   │   └── input/               # Micro-batch CSV files for streaming
│   ├── dimensions/              # Customer and product dimension tables
│   └── warehouse/
│       ├── batch/               # Batch processing outputs
│       │   ├── orders/          # Partitioned by order_date
│       │   ├── daily_revenue/
│       │   ├── product_revenue/
│       │   ├── hourly_revenue/
│       │   ├── quarantine/      # Invalid records
│       │   └── cancelled/       # Cancelled orders
│       └── stream/              # Stream processing outputs
│           ├── windowed_revenue/
│           ├── kafka_windowed_revenue/  # Kafka streaming output
│           ├── product_totals_snapshots/
│           └── checkpoint/
│
├── src/
│   ├── common/
│   │   ├── schema.py            # Shared schema definitions
│   │   ├── data_generator.py   # Synthetic orders data generator
│   │   └── dimension_generator.py  # Generate customer/product tables
│   │
│   ├── batch/
│   │   ├── batch_job.py         # Main batch ETL entry point
│   │   ├── io_utils.py          # Spark session, config, I/O utilities
│   │   └── transformations.py  # Pure transformation functions
│   │
│   └── stream/
│       ├── stream_job.py        # Main file-based streaming ETL
│       ├── stream_transformations.py  # Streaming transformations
│       ├── event_generator.py   # Simulates real-time data arrival
│       ├── kafka_producer.py    # Kafka producer (sends events)
│       └── kafka_stream_job.py  # Kafka streaming consumer
│
├── docs/
│   ├── batch_vs_stream.md       # Theory: Batch vs Stream comparison
│   ├── batch_stream_analysis.md # Analysis: Performance, complexity
│   └── kafka_comparison.md      # File-based vs Kafka streaming
│
├── requirements.txt
├── .gitignore
└── README.md
```
---

##  Setup Instructions

### 1. Clone the Repository
```bash
git clone <your-repo-url>
cd batch_vs_stream
```

### 2. Create Virtual Environment
```bash
# Create virtual environment
python -m venv venv

# Activate (Windows)
venv\Scripts\activate

# Activate (macOS/Linux)
source venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Verify Java Installation
```bash
java -version
# Should show: openjdk version "17.x.x"
```

### 5. Configure Environment Variables
See [Requirements](#requirements) section above.

### 6. Generate Data

**Step 1: Generate dimension tables**
```bash
python src/common/dimension_generator.py
```
Output:
- `data/dimensions/customers.csv` (1,000 customers)
- `data/dimensions/products.csv` (50 products)

**Step 2: Generate orders data**
```bash
python src/common/data_generator.py
```
Output:
- `data/batch/orders_20260101.csv` (10,000+ orders with data quality issues)

Expected output:
```
Generating orders...
Generated 10200 orders
Duplicates: 200
Negative Qty: 500
Negative Price: 500
Late Data: 500
Saved orders to CSV
```

---

## Usage

### Run Batch Processing
```bash
python src/batch/batch_job.py
```

**What it does:**
1. Reads all CSV files from `data/batch/`
2. Applies data quality checks (removes invalid records to quarantine)
3. Separates cancelled orders
4. Removes duplicates
5. Enriches with customer and product dimensions
6. Calculates aggregations (daily, product, hourly revenue)
7. Writes results to `data/warehouse/batch/` as partitioned Parquet files

**Expected output:**
```
======================================================================
BATCH ETL JOB STARTED
Timestamp: 2026-01-15 01:46:47
======================================================================
Loading configuration...
Creating Spark session...
Spark version: 4.0.1
Reading input data...
[READ] Input records Count: 10200

Data quality checks...
[QUALITY] Missing order_id Count: 100
  ✓ Missing order_id records written to quarantine
[QUALITY] Invalid values (negative qty/price) Count: 968
  ✓ Invalid values written to quarantine

Status values normalized
Duplicates removed: 159
Separating cancelled orders...
[CANCELLED] Cancelled orders Count: 2047
  ✓ Cancelled orders written to separate directory

Enriching with dimension tables and deriving columns...
[ENRICHED] Valid active orders Count: 6926
  ✓ Derived: order_date, order_day_of_week, hour_of_day, total_amount
  ✓ Joined with customer dimension
  ✓ Joined with product dimension

Calculating business metrics...
  ✓ Daily revenue aggregated (30 days)
  ✓ Product revenue aggregated (50 products)
  ✓ Top 10 products by revenue
  ✓ Hourly revenue aggregated (24 hours)

Writing results to warehouse...
  ✓ Orders written to: data/warehouse/batch/orders/
  ✓ Daily revenue written
  ✓ Product revenue written
  ✓ Hourly revenue written

======================================================================
BATCH ETL JOB COMPLETED
======================================================================
Input records:        10200
Valid output records: 6926
Cancelled orders:     2047
Quarantined records:  1068
Duplicates removed:   159
Execution time:       16.71 seconds
======================================================================
```

---

### Run Streaming Processing

**You need TWO terminal windows running simultaneously:**

#### Terminal 1: Start Event Generator
```bash
python src/stream/event_generator.py
```

**What it does:**
- Reads source CSV file
- Splits into micro-batches (100 rows each)
- Writes batches as separate files to `data/stream/input/`
- Sleeps 1-3 seconds between batches (simulates real-time arrival)

**Expected output:**
```
Total rows: 10200
Batch size: 100
Sleep interval: 1.0 - 3.0 seconds
Output directory: /path/to/data/stream/input

[Batch 0001] Written 100 rows → events_batch_0001.csv
Sleeping for 2.34 seconds...
[Batch 0002] Written 100 rows → events_batch_0002.csv
Sleeping for 1.87 seconds...
...
✓ Generation complete!
  Total batches: 102
  Total rows written: 10200
```

#### Terminal 2: Start Streaming Job
```bash
python src/stream/stream_job.py
```

**What it does:**
1. Sets up streaming source from `data/stream/input/`
2. Applies same cleaning/validation as batch (but streaming-compatible)
3. Enriches with dimensions (stream-to-batch join)
4. Calculates windowed aggregations (10-minute windows)
5. Calculates running totals per product
6. Writes to multiple sinks:
   - Console (for debugging)
   - Parquet files (windowed revenue)
   - CSV snapshots (product totals)

**Expected output:**
```
======================================================================
STREAMING ETL JOB STARTED
Timestamp: 2026-01-15 01:48:17
======================================================================

[1/6] Loading configuration...
[2/6] Loading dimension tables...
  ✓ Customers: 1000 records
  ✓ Products: 50 records

[3/6] Setting up streaming source...
  Input path: /path/to/data/stream/input
  Max files per trigger: 2
  ✓ Streaming source configured

[4/6] Applying transformations...
  ✓ Data cleaning applied (watermark: 30 minutes)
  ✓ Derived columns added
  ✓ Joined with customer dimension
  ✓ Joined with product dimension

[5/6] Setting up aggregations...
  ✓ Windowed aggregation configured (10 minutes windows)
  ✓ Product totals aggregation configured

[6/6] Starting streaming queries...
  ✓ Console sink started (windowed revenue)
  ✓ File sink started (windowed revenue)
  ✓ Memory sink started (product totals)
  ✓ CSV snapshot sink started (product totals)

======================================================================
STREAMING QUERIES ARE RUNNING
======================================================================
Waiting for data to arrive...
Press Ctrl+C to stop
======================================================================

[Console-WindowedRevenue] Batch #0
  Rows processed: 138
+-------------------+-------------------+----------+------------+-------------+
|window_start       |window_end         |product_id|orders_count|total_revenue|
+-------------------+-------------------+----------+------------+-------------+
|2026-01-01 07:00:00|2026-01-01 07:10:00|p14       |1           |399.4        |
|2026-01-01 07:40:00|2026-01-01 07:50:00|p18       |1           |1244.72      |
...
+-------------------+-------------------+----------+------------+-------------+

[Memory-ProductTotals] Batch #0
  Rows processed: 47

  Top 10 Products by Revenue:
+----------+------------+-------------+
|product_id|total_orders|total_revenue|
+----------+------------+-------------+
|p1        |8           |11509.38     |
|p12       |5           |9244.72      |
...
+----------+------------+-------------+

   Snapshot written: batch_0000.csv

[Console-WindowedRevenue] Batch #1
  Rows processed: 122
...

# Let it run for ~5 minutes to process all batches
# Press Ctrl+C to stop

======================================================================
STREAMING ETL JOB COMPLETED
======================================================================
Total runtime: 306.93 seconds
======================================================================
```
### Option 3: Kafka Streaming (Production-Grade) 

#### Prerequisites: Start Kafka

**Terminal 1: Start Kafka with Docker**
```bash
# In project root directory
docker-compose up -d

# Check containers are running
docker ps

# Expected output:
# - kafka-simple (Up)
# - kafka-ui (Up)

# Verify Kafka is ready
docker logs kafka-simple --tail 50
# Should see: "Kafka Server started"
```

**What's running:**
- **Kafka broker** on `localhost:9092` (KRaft mode)
- **Kafka UI** on `http://localhost:8080` (web monitoring interface)


#### Run Kafka Streaming Pipeline

**Terminal 1: Start Kafka Consumer (Spark Streaming)**
```bash
python src/stream/kafka_stream_job.py
```

**Expected output:**
```
======================================================================
KAFKA STREAMING ETL JOB STARTED
======================================================================
[1/6] Loading configuration...
[2/6] Creating Spark session with Kafka support...
...
KAFKA STREAMING QUERIES RUNNING
======================================================================
Waiting for Kafka messages...
Press Ctrl+C to stop
```

**Terminal 2: Start Kafka Producer**
```bash
python src/stream/kafka_producer.py
```

**Expected output:**
```
======================================================================
KAFKA PRODUCER STARTED
======================================================================
Total rows: 10200
Batch size: 100
Topic: ecommerce-orders
======================================================================

[Batch 0001] Sent: 100 | Failed: 0
  Sleeping for 2.34 seconds...
[Batch 0002] Sent: 100 | Failed: 0
...
```

**Browser: Monitor in Kafka UI**

Open **http://localhost:8080**

Navigate to:
- **Topics** → `ecommerce-orders` → **Messages** (see messages in real-time)
- **Consumers** → `spark-ecommerce-group` → **Consumer Lag**

---

##  Documentation

### Theory & Analysis

1. **[batch_vs_stream.md](docs/batch_vs_stream.md)**
   - Comprehensive comparison table (latency, data sources, fault tolerance, use cases, tools)
   - Scenario analysis: When to use batch vs stream
   - Lambda/Kappa architecture explanation

2. **[batch_stream_analysis.md](docs/batch_stream_analysis.md)**
   - Real performance metrics from this project
   - Complexity comparison (what was easier/harder)
   - Streaming limitations encountered
   - Detailed recommendations on when to choose each approach

### Configuration

All settings are in `config/config.yaml`:
```yaml
spark:
  app_name: "ECommerce Data Processing"
  master: "local[*]"
  log_level: "WARN"

batch:
  input_path: "data/batch/"
  output_path: "data/warehouse/batch/"
  quarantine_path: "data/warehouse/batch/quarantine/"
  cancelled_path: "data/warehouse/batch/cancelled/"

stream:
  input_path: "data/stream/input/"
  output_path: "data/warehouse/stream/"
  checkpoint_path: "data/warehouse/stream/checkpoint/"
  max_files_per_trigger: 2
  watermark_delay: "30 minutes"
  window_duration: "10 minutes"

generator:
  num_records: 10000
  duplicate_rate: 0.02
  negative_quantity_rate: 0.05
  negative_price_rate: 0.05
  late_data_rate: 0.05
  missing_id_rate: 0.01
```

---

## 📸 Screenshots

### Batch Processing Execution
<img width="654" height="514" alt="image" src="https://github.com/user-attachments/assets/46c35c97-c6a3-468e-8966-3be4723999f3" />
<img width="670" height="244" alt="image" src="https://github.com/user-attachments/assets/2eac73d9-5073-46e6-b4a1-be52fff202c3" />

---

### Event Generator
<img width="717" height="365" alt="image" src="https://github.com/user-attachments/assets/e544cdd1-6938-43d7-8b88-5bfde9e99f10" />

---

### Streaming Processing Execution
<img width="642" height="152" alt="image" src="https://github.com/user-attachments/assets/d97d02ed-54c6-4fb6-b5af-b922803b7d98" />
<img width="701" height="514" alt="image" src="https://github.com/user-attachments/assets/2a875cfa-654d-4aad-850a-f0ef3918d854" />
<img width="748" height="702" alt="image" src="https://github.com/user-attachments/assets/d416710b-b9e6-4fc3-846a-0ea836e1ab44" />
<img width="690" height="631" alt="image" src="https://github.com/user-attachments/assets/0429cbda-54ac-442b-94ff-ad79b8e63287" />
<img width="639" height="423" alt="image" src="https://github.com/user-attachments/assets/1c3544bb-60ea-4695-9c96-27d555306711" />
<img width="597" height="275" alt="image" src="https://github.com/user-attachments/assets/d2ac2f4a-8ab9-4ffe-ba92-d950aa7c1d0c" />

---

### Output Data Structure
<img width="332" height="455" alt="image" src="https://github.com/user-attachments/assets/786d52f5-1765-4c46-98cb-f5b5bc510960" />

*Warehouse structure showing batch and stream outputs*

---
### Kafka integration
<img width="806" height="436" alt="image" src="https://github.com/user-attachments/assets/a2e5e587-4a6f-4283-b7cb-f22018229a3b" />
<img width="618" height="151" alt="image" src="https://github.com/user-attachments/assets/b04f58ec-41c2-4a76-8712-edfce5e5da68" />
<img width="640" height="353" alt="image" src="https://github.com/user-attachments/assets/844feb2a-92de-4615-a19f-667cd515ff12" />
<img width="957" height="128" alt="image" src="https://github.com/user-attachments/assets/99e98592-8288-4dd7-a22e-296790c0946f" />






---

## 🎓 Key Learnings

### Batch Processing
- ✅ **Simpler** to implement and debug
- ✅ **Better throughput** for large datasets (16 seconds for 10K records)
- ✅ **Easier error handling** with quarantine files
- ✅ **More flexible** transformations without state management concerns
- ❌ Higher latency - must wait for full job completion
- ❌ Not suitable for real-time requirements

### Stream Processing
- ✅ **Near-real-time** processing (5-15 second latency)
- ✅ **Continuous operation** - always ready for new data
- ✅ **Event-driven** - react to data as it arrives
- ❌ More complex to implement (watermarks, state management, checkpointing)
- ❌ Harder to debug - cannot easily inspect intermediate results
- ❌ Slower overall throughput due to micro-batch overhead (306 seconds for same 10K records)

### Kafka Streaming
- ✅ **Production-ready** architecture
- ✅ **Lowest latency** (1-5 seconds)
- ✅ **Message persistence** and replay capability
- ✅ **Horizontal scalability**
- ❌ More complex setup (requires Kafka infrastructure)

### Streaming Limitations vs Batch
1. **No easy quarantine** - must filter instead of saving invalid records
2. **Watermark required** for deduplication and time-based operations
3. **Complete output mode** not supported with file sinks
4. **State management** - memory consumption grows with stateful operations
5. **Multiple queries needed** for multiple outputs
6. **Late data handling** - data beyond watermark is dropped

### When to Use Each
- **Batch**: Historical analysis, complex ETL, cost optimization, periodic reporting
- **Stream**: Real-time dashboards, fraud detection, IoT monitoring, event-driven workflows
- **Hybrid**: Use both in Lambda architecture for fast approximate results + accurate historical analysis

---
