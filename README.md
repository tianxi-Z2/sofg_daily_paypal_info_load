<<<<<<< HEAD
# PayPal Pipeline Enterprise

<div align="center">

![Pipeline Status](https://img.shields.io/badge/pipeline-enterprise--ready-green)
![Python](https://img.shields.io/badge/python-3.11+-blue)
![Airflow](https://img.shields.io/badge/airflow-2.7+-orange)
![License](https://img.shields.io/badge/license-MIT-blue)

**Enterprise-grade ETL pipeline for PayPal transaction data processing**

</div>

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   PayPal API    │───▶│  Data Pipeline   │───▶│   BigQuery DW   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │  Cloud Storage   │
                       │   (Raw/Parsed)   │
                       └──────────────────┘
```

### 🎯 Key Features

- ✅ **Infrastructure as Code** - Terraform automation
- ✅ **Containerized Deployment** - Docker & Docker Compose
- ✅ **Orchestrated Workflows** - Apache Airflow
- ✅ **Scalable Storage** - Google Cloud Storage
- ✅ **Data Warehouse** - BigQuery with partitioning
- ✅ **Data Quality** - Comprehensive validation
- ✅ **Enterprise Security** - Service accounts & encryption
- ✅ **Monitoring** - Logging, metrics & alerting

## 📁 Project Structure

```
paypal-pipeline-enterprise/
├── terraform/                 # Infrastructure as Code
│   ├── main.tf               # Main Terraform configuration
│   ├── variables.tf          # Variable definitions
│   ├── outputs.tf            # Output definitions
│   └── terraform.tfvars.example
├── docker/                   # Container configuration
│   ├── Dockerfile            # Application container
│   ├── requirements.txt      # Python dependencies
│   └── .dockerignore        # Docker ignore rules
├── scripts/                  # ETL business logic
│   ├── __init__.py          # Package initialization
│   ├── fetch_transactions.py # PayPal data extraction
│   ├── transform.py         # Data transformation
│   ├── load_to_bq.py        # BigQuery loading
│   └── utils.py             # Common utilities
├── dags/                    # Airflow orchestration
│   └── paypal_dag.py        # Main pipeline DAG
├── config/                  # Configuration files
│   ├── schema.json          # BigQuery table schema
│   └── pipeline_config.yaml # Pipeline configuration
├── docker-compose.yml       # Multi-service orchestration
├── .env.example            # Environment configuration template
├── .gitignore              # Git ignore rules
└── README.md               # Project documentation
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Google Cloud Platform account
- PayPal Developer account
- Terraform (for infrastructure deployment)

### 1. Environment Setup

```bash
# Clone the repository
git clone <repository-url>
cd paypal-pipeline-enterprise

# Copy environment template
cp .env.example .env

# Edit configuration
nano .env  # Update with your credentials
```

### 2. Infrastructure Deployment

```bash
# Navigate to Terraform directory
cd terraform

# Copy and edit Terraform variables
cp terraform.tfvars.example terraform.tfvars
nano terraform.tfvars

# Initialize and deploy infrastructure
terraform init
terraform plan
terraform apply
```

### 3. Service Account Setup

```bash
# Get service account key from Terraform output
terraform output -raw service_account_key | base64 -d > ../sa-key.json

# Set up environment variables (copy from terraform output)
terraform output setup_commands
```

### 4. Start the Pipeline

```bash
# Return to project root
cd ..

# Start all services
docker-compose up -d

# Check service status
docker-compose ps
```

### 5. Access Airflow UI

1. Open browser to `http://localhost:8080`
2. Login with credentials from `.env` file (default: airflow/airflow)
3. Enable the `paypal_data_pipeline` DAG
4. Trigger a manual run or wait for scheduled execution

## 🔧 Configuration

### Environment Variables

Key environment variables in `.env`:

```bash
# GCP Configuration
GCP_PROJECT_ID=your-gcp-project-id
GCS_BUCKET=your-paypal-data-bucket
BQ_DATASET=paypal_data

# PayPal API
PAYPAL_CLIENT_ID=your_client_id
PAYPAL_CLIENT_SECRET=your_client_secret
PAYPAL_SANDBOX=true

# Pipeline Settings
ENVIRONMENT=dev
LOG_LEVEL=INFO
```

### Pipeline Configuration

Customize pipeline behavior in `config/pipeline_config.yaml`:

```yaml
paypal:
  api:
    timeout: 120
    max_retries: 3
    page_size: 500

data_quality:
  validation:
    enabled: true
    fail_on_error: false

monitoring:
  enabled: true
  metrics:
    - daily_transaction_count
    - pipeline_execution_time
```

## 🔄 Pipeline Workflow

### 1. Extract (`fetch_transactions.py`)
- Connects to PayPal Reporting API
- Handles authentication and pagination
- Saves raw data to Cloud Storage
- Implements retry logic and error handling

### 2. Transform (`transform.py`)
- Parses raw PayPal JSON responses
- Normalizes data structure
- Validates data quality
- Converts to BigQuery-compatible format

### 3. Load (`load_to_bq.py`)
- Creates dataset and table if needed
- Loads data with partitioning
- Updates metadata timestamps
- Creates analytical views
- Validates loaded data

### 4. Orchestrate (`paypal_dag.py`)
- Schedules daily execution
- Manages task dependencies
- Handles error notifications
- Provides comprehensive monitoring

## 📊 Data Schema

### Transaction Table Structure

| Field | Type | Description |
|-------|------|-------------|
| `transaction_id` | STRING | Unique PayPal transaction ID |
| `transaction_date` | TIMESTAMP | Transaction timestamp |
| `amount` | FLOAT64 | Transaction amount |
| `currency_code` | STRING | Currency (USD, EUR, etc.) |
| `transaction_status` | STRING | Status (Success, Pending, etc.) |
| `payer_email` | STRING | Payer email address |
| `fee_amount` | FLOAT64 | PayPal processing fee |
| `net_amount` | FLOAT64 | Net amount after fees |
| `items` | RECORD | Repeated record of transaction items |

### Generated Views

- `daily_summary` - Daily transaction aggregations
- `payer_summary` - Customer transaction history
- `recent_transactions` - Last 7 days of transactions

## 🛠️ Development

### Running Individual Scripts

```bash
# Extract data
docker-compose exec pipeline-runner python scripts/fetch_transactions.py \
  --start-date 2024-01-01 --end-date 2024-01-01 \
  --output-path /tmp/raw_data.json

# Transform data
docker-compose exec pipeline-runner python scripts/transform.py \
  --input-path /tmp/raw_data.json \
  --output-path /tmp/parsed_data.jsonl

# Load to BigQuery
docker-compose exec pipeline-runner python scripts/load_to_bq.py \
  --source-path /tmp/parsed_data.jsonl
```

### Testing

```bash
# Run tests
docker-compose exec pipeline-runner python -m pytest tests/

# Run with coverage
docker-compose exec pipeline-runner python -m pytest --cov=scripts tests/
```

### Code Quality

```bash
# Format code
docker-compose exec pipeline-runner black scripts/

# Lint code
docker-compose exec pipeline-runner flake8 scripts/

# Sort imports
docker-compose exec pipeline-runner isort scripts/
```

## 📈 Monitoring

### Airflow Monitoring

- **Web UI**: `http://localhost:8080`
- **Task Status**: View in Airflow Graph View
- **Logs**: Available in Airflow UI and local `logs/` directory

### Optional Monitoring Stack

Enable monitoring services:

```bash
# Start with monitoring profile
docker-compose --profile monitoring up -d

# Access Grafana
open http://localhost:3000  # admin/admin

# Access Prometheus
open http://localhost:9090
```

### Data Quality Metrics

The pipeline automatically generates:
- Transaction count validation
- Data completeness scores
- Duplicate detection
- Schema validation results

## 🔒 Security

### Best Practices Implemented

- ✅ Service account authentication
- ✅ Secrets management via environment variables
- ✅ Network isolation with Docker networks
- ✅ Minimal container privileges
- ✅ Sensitive data masking in logs

### Security Checklist

- [ ] Rotate service account keys regularly
- [ ] Enable audit logging in GCP
- [ ] Use Cloud KMS for encryption keys
- [ ] Implement VPC for network security
- [ ] Set up Cloud Security Scanner

## 🚨 Troubleshooting

### Common Issues

**Pipeline fails with authentication error:**
```bash
# Check service account key
docker-compose exec pipeline-runner cat /app/sa-key.json

# Verify GCP access
docker-compose exec pipeline-runner gcloud auth list
```

**Airflow UI not accessible:**
```bash
# Check Airflow services
docker-compose ps

# View Airflow logs
docker-compose logs airflow-webserver
```

**PayPal API rate limiting:**
- Reduce `page_size` in config
- Increase `retry_delay` in config
- Check PayPal API quotas

### Log Locations

- **Airflow Logs**: `logs/` directory
- **Container Logs**: `docker-compose logs <service>`
- **GCP Logs**: Cloud Logging in GCP Console

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run code quality checks
6. Submit a pull request

### Development Setup

```bash
# Install development dependencies
pip install -r docker/requirements.txt

# Set up pre-commit hooks
pre-commit install

# Run full test suite
pytest tests/
```

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙋 Support

- **Issues**: GitHub Issues
- **Documentation**: This README and inline code comments
- **Email**: tianxi.zhao@altardstate.com

---

<div align="center">

**Built with ❤️ for enterprise IT teams**

</div># sofg_daily_paypal_info_load
=======
# sofg_daily_paypal_info_load
>>>>>>> 8a46f80ebdeb9fae2efc8113036baef15c97b860
