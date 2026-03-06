# Data Lake Architecture

A production-grade data lake implementation using the medallion architecture pattern (Bronze/Silver/Gold layers) with MinIO as S3-compatible storage, DuckDB as the query engine, and Terraform templates for AWS deployment.

## Architecture

```
                    ┌─────────────────────────────────────────────┐
                    │            Data Lake Architecture           │
                    └─────────────────────────────────────────────┘

  Sources                Bronze              Silver              Gold
 ┌───────┐          ┌─────────────┐     ┌─────────────┐    ┌─────────────┐
 │  CSV  │──────┐   │  Raw Data   │     │  Cleaned &  │    │ Aggregated  │
 │  JSON │──────┤   │  Append-    │────>│  Conformed  │───>│  Business   │
 │ JSONL │──────┘──>│  Only       │     │  Deduplied  │    │  Metrics    │
 └───────┘          │  Partitioned│     │  Parquet    │    │  Star Schema│
                    └─────────────┘     └─────────────┘    └──────┬──────┘
                          │                                       │
                    ┌─────┴─────┐                          ┌──────┴──────┐
                    │  Metadata │                          │   DuckDB    │
                    │  Catalog  │                          │   Query     │
                    │  (SQLite) │                          │   Engine    │
                    └───────────┘                          └─────────────┘
```

**Key Components:**

- **Bronze Layer** - Raw data ingestion with hash-based deduplication, Hive-style partitioning (`year=YYYY/month=MM/day=DD/source=X`)
- **Silver Layer** - Data cleaning, type coercion, deduplication, Parquet output with Snappy compression
- **Gold Layer** - Star schema aggregations with fact and dimension table generation
- **Data Catalog** - SQLite-backed metadata store with schema evolution tracking and data lineage
- **Query Engine** - DuckDB-based SQL queries over Parquet files with multiple output formats
- **Cost Estimator** - AWS cost projection for S3, Glue, Athena, and Lambda services
- **Quality Framework** - Configurable data quality checks (null rates, uniqueness, ranges, schema conformance)
- **Dashboard** - Streamlit monitoring UI with layer health, data quality heatmaps, ingestion throughput, and cost breakdown

## Quick Start

### Prerequisites

- Python 3.12+
- Docker and Docker Compose

### 1. Install Dependencies

```bash
git clone git@github.com:KarasiewiczStephane/data-lake-architecture.git
cd data-lake-architecture
make install
```

### 2. Start MinIO and Initialize Buckets

```bash
make up
```

This starts MinIO (S3-compatible storage on ports 9000/9001) and automatically creates the `datalake-bronze`, `datalake-silver`, and `datalake-gold` buckets.

### 3. Generate and Ingest Sample Data

```bash
# Generate synthetic e-commerce data (customers, products, orders)
python data/sample/generate_sample_data.py

# Ingest into the bronze layer
python -m src.cli ingest -s data/sample/customers.csv -t customers --source-name ecommerce

# Process bronze -> silver (with deduplication)
python -m src.cli process -t customers -l silver --dedup-columns customer_id
```

### 4. Launch the Dashboard

```bash
make dashboard
```

Opens the Streamlit monitoring dashboard at `http://localhost:8501` with:
- Medallion layer health metrics (record counts per Bronze/Silver/Gold)
- Data quality heatmap (null rates, uniqueness, schema conformance)
- Daily ingestion throughput chart
- AWS monthly cost breakdown

## CLI Usage

The CLI provides commands for the full data pipeline:

```bash
# Ingest CSV into bronze layer
python -m src.cli ingest -s data/sample/customers.csv -t customers --source-name ecommerce

# Process bronze -> silver (with deduplication)
python -m src.cli process -t customers -l silver --dedup-columns customer_id

# Run SQL queries (JSON or CSV output)
python -m src.cli query "SELECT * FROM silver_customers LIMIT 10"
python -m src.cli query "SELECT * FROM silver_customers" -f csv

# Search the data catalog
python -m src.cli catalog search -t revenue

# View data lineage
python -m src.cli catalog lineage -t customers -l silver

# Estimate AWS costs (table or JSON output)
python -m src.cli cost-estimate -c configs/cost_params.yaml -f table
python -m src.cli cost-estimate -c configs/cost_params.yaml -f json

# Initialize buckets manually (not needed if using make up)
python -m src.cli init
```

Use `-v` for verbose (debug) logging:

```bash
python -m src.cli -v ingest -s data.csv -t orders --source-name pos
```

## Cost Estimation

Estimate monthly AWS costs based on your workload parameters:

```bash
python -m src.cli cost-estimate -c configs/cost_params.yaml
```

```
AWS Monthly Cost Estimate
========================================
S3 Storage:        $3.68
S3 Requests:       $0.33
S3 Transfer:       $0.90
Glue ETL:          $6.60
Athena Queries:    $2.44
Lambda Compute:    $0.03
========================================
TOTAL:             $13.98/month
```

Edit `configs/cost_params.yaml` to model your workload:

```yaml
data_volume:
  bronze_gb: 100
  silver_gb: 50
  gold_gb: 10

query_pattern:
  queries_per_month: 1000
  avg_data_scanned_gb: 0.5
```

## Terraform Deployment

Reference Terraform templates are provided in `terraform/` for deploying to AWS:

```bash
cd terraform

# Initialize and validate
terraform init -backend=false
terraform validate
terraform fmt -check

# Review the plan
terraform plan -var="environment=dev"

# Deploy
terraform apply -var="environment=dev"
```

Resources provisioned:
- S3 buckets (bronze/silver/gold) with lifecycle rules, encryption, and versioning
- Glue catalog database and crawlers for each layer
- Athena workgroup with query cost controls
- Lambda function for event-driven processing
- IAM roles with least-privilege policies

## Development

```bash
make install    # Install dependencies
make test       # Run tests with coverage
make lint       # Lint and format with ruff
make up         # Start MinIO stack
make down       # Stop MinIO stack
make dashboard  # Launch Streamlit dashboard
make run        # Run CLI (python -m src.main)
make shell      # Open shell in datalake container
make clean      # Remove __pycache__ and .pyc files
```

### Running Tests

```bash
# Full suite with coverage
pytest tests/ -v --cov=src --cov-report=term-missing

# Specific module
pytest tests/test_quality_checks.py -v

# With coverage threshold
pytest tests/ --cov=src --cov-fail-under=80
```

## Project Structure

```
data-lake-architecture/
├── src/
│   ├── catalog/           # Metadata store and schema manager
│   ├── cost/              # AWS cost estimator
│   ├── dashboard/         # Streamlit monitoring dashboard
│   │   └── app.py
│   ├── processing/        # Bronze, silver, gold layer processors
│   │   ├── bronze_loader.py
│   │   ├── silver_processor.py
│   │   ├── gold_aggregator.py
│   │   └── quality_checks.py
│   ├── query/             # DuckDB query engine
│   ├── storage/           # MinIO client and data partitioner
│   ├── utils/             # Config and logging utilities
│   ├── cli.py             # Click CLI entry point
│   └── main.py            # Main entry point (runs CLI)
├── terraform/             # AWS infrastructure templates
├── tests/                 # Test suite (87% coverage)
├── configs/               # YAML configuration files
├── data/sample/           # Sample data generator
├── .github/workflows/     # CI pipeline
├── docker-compose.yml     # Local development stack (MinIO)
├── Dockerfile
├── Makefile
└── requirements.txt
```

## License

MIT
