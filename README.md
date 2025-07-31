# PayPal Pipeline - Using Your Own GCP Account

## 🎯 **Overview**

This guide enables you to run Elvis's PayPal data pipeline using **your own Google Cloud Platform (GCP) account** with complete resource isolation. No code changes required from Elvis - just configuration!

### Why Use Your Own GCP Account?
- ✅ **Complete resource isolation** - Your data stays in your account
- ✅ **Cost control** - You manage your own billing and budgets  
- ✅ **Access control** - Full control over permissions and security
- ✅ **Compliance** - Meet your organization's data governance requirements
- ✅ **Learning opportunity** - Hands-on experience with GCP services

---

## 🏗️ **Step 1: Set Up Your GCP Resources**

### 1.1 Create a New GCP Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Click the project selector → "New Project"
3. **Project name**: `[your-name]-paypal-pipeline` (e.g., `john-paypal-pipeline`)
4. **Note down your Project ID** (e.g., `john-paypal-pipeline-467319`) - you'll need this later

### 1.2 Enable Required APIs

In Cloud Shell or using `gcloud` CLI:
```bash
# Set your project ID
export PROJECT_ID="your-project-id-here"
gcloud config set project $PROJECT_ID

# Enable required APIs
gcloud services enable storage.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable iam.googleapis.com
```

### 1.3 Create Cloud Storage Bucket

```bash
# Replace YOUR_NAME with your name (must be globally unique)
export BUCKET_NAME="your-name-paypal-data-bucket"
gsutil mb gs://$BUCKET_NAME

# Set up lifecycle policy (optional - deletes files after 90 days)
cat > lifecycle.json << EOF
{
  "rule": [
    {
      "action": {"type": "Delete"},
      "condition": {"age": 90}
    }
  ]
}
EOF

gsutil lifecycle set lifecycle.json gs://$BUCKET_NAME
```

### 1.4 Create BigQuery Dataset

```bash
# Create the dataset
bq mk --dataset --location=US $PROJECT_ID:paypal_data

# Or via Console:
# 1. Go to BigQuery in GCP Console
# 2. Click your project ID
# 3. Click "Create Dataset"
# 4. Dataset ID: paypal_data
# 5. Location: US
```

---

## 🔑 **Step 2: Create Service Account & Permissions**

### 2.1 Create Service Account

```bash
# Create service account
gcloud iam service-accounts create paypal-pipeline-sa \
    --display-name="PayPal Pipeline Service Account" \
    --description="Service account for PayPal data pipeline"

# Set the service account email variable
export SA_EMAIL="paypal-pipeline-sa@$PROJECT_ID.iam.gserviceaccount.com"
```

### 2.2 Assign Required Roles

```bash
# Storage Admin - for GCS operations
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/storage.admin"

# BigQuery Admin - for dataset/table management
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/bigquery.admin"

# BigQuery Data Editor - for data operations
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/bigquery.dataEditor"

# BigQuery Job User - for running queries
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/bigquery.jobUser"
```

### 2.3 Generate Service Account Key

```bash
# Download the service account key
gcloud iam service-accounts keys create sa-key.json \
    --iam-account=$SA_EMAIL

# Secure the key file
chmod 600 sa-key.json
```

**Alternative: Using GCP Console**
1. Go to IAM & Admin → Service Accounts
2. Click "Create Service Account"
3. Name: `paypal-pipeline-sa`
4. Assign the roles mentioned above
5. Go to Keys tab → Add Key → Create New Key → JSON
6. Download and save as `sa-key.json`

---

## ⚙️ **Step 3: Configure the Pipeline**

### 3.1 Clone Elvis's Project

```bash
# Clone the repository
git clone https://github.com/tianxi-Z2/sofg_daily_paypal_info_load.git
cd sofg_daily_paypal_info_load
```

### 3.2 Update Environment Variables

Edit the `.env` file with your GCP details:

```bash
# Copy the example and edit
cp .env .env.backup  # backup original

# Edit the .env file
nano .env  # or use your preferred editor
```

**Update these values in `.env`:**
```bash
# ===== YOUR GCP CONFIGURATION =====
GCP_PROJECT_ID=your-project-id-here
GCS_BUCKET=your-name-paypal-data-bucket
BQ_DATASET=paypal_data
BQ_TABLE=transactions

# ===== PAYPAL CONFIGURATION =====
# Option 1: Use your own PayPal Developer Account
PAYPAL_CLIENT_ID=your-paypal-client-id
PAYPAL_CLIENT_SECRET=your-paypal-client-secret

# Option 2: Use Elvis's test credentials (if shared)
# PAYPAL_CLIENT_ID=AfKBtYQ-9xh17FdMjvuYB5PDSsDPHojQ2hpbvyChEr7j9gv7-pa83u_ccvYQGYNH3iUJd9889MO6MrCX
# PAYPAL_CLIENT_SECRET=EBLHnaxBy1XF1bIAa69M_AN_oW2oCiqU1lMm3dHUPhAi3spknRU6t4hRxSOAz0siD8X5L2ljkvijZJ_u

PAYPAL_SANDBOX=true

# ===== KEEP THESE UNCHANGED =====
ENVIRONMENT=dev
LOG_LEVEL=INFO
GOOGLE_APPLICATION_CREDENTIALS=/app/sa-key.json

# All other settings can remain as-is
```

### 3.3 Place Your Service Account Key

```bash
# Ensure your sa-key.json is in the project root directory
ls -la sa-key.json
# Should show: -rw------- 1 user user 2410 Jul 31 15:30 sa-key.json
```

---

## 🚀 **Step 4: Deploy and Run**

### 4.1 Deploy the Pipeline

```bash
# Option 1: Use the setup wizard (recommended)
chmod +x setup_wizard.sh
./setup_wizard.sh

# Option 2: Direct deployment
chmod +x deploy_paypal_pipeline.sh
./deploy_paypal_pipeline.sh
```

### 4.2 Access Your Pipeline

After successful deployment:
- **Airflow Web UI**: http://localhost:8080
- **Username**: `airflow`
- **Password**: `airflow`

### 4.3 Verify Everything Works

1. In Airflow UI, find `paypal_data_pipeline` DAG
2. Click ▶️ "Trigger DAG"
3. Monitor the execution
4. Check that data appears in your BigQuery dataset

---

## 💰 **Cost Management**

### Expected Monthly Costs
| Service | Estimated Cost |
|---------|----------------|
| Cloud Storage | $1 - $5 |
| BigQuery | $5 - $20 |
| **Total** | **$10 - $30** |

### Cost Optimization Tips

#### 1. Set Up Budget Alerts
```bash
# Create a $50 monthly budget alert
gcloud billing budgets create \
    --billing-account=YOUR_BILLING_ACCOUNT_ID \
    --display-name="PayPal Pipeline Budget" \
    --budget-amount=50USD \
    --threshold-rules-percent=50,90,100
```

#### 2. Monitor Usage
- Enable [GCP Billing Export to BigQuery](https://cloud.google.com/billing/docs/how-to/export-data-bigquery)
- Set up [custom cost alerts](https://cloud.google.com/billing/docs/how-to/budgets)
- Review monthly billing reports

#### 3. Clean Up Resources
```bash
# Delete old GCS objects (lifecycle policy handles this automatically)
gsutil rm gs://$BUCKET_NAME/paypal/raw/2024-*

# Delete old BigQuery data
bq query --use_legacy_sql=false \
"DELETE FROM \`$PROJECT_ID.paypal_data.transactions\` 
WHERE transaction_date < DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)"
```

---

## 🔒 **Security Best Practices**

### Service Account Security
- **Principle of least privilege**: Only assign necessary roles
- **Rotate keys regularly**: Generate new keys every 90 days
- **Monitor access**: Review IAM audit logs monthly

### Data Protection
- **Encrypt sensitive data**: BigQuery encryption is enabled by default
- **Network security**: Consider VPC-native clusters for production
- **Access logging**: Enable Cloud Audit Logs

### Key Management
```bash
# Regularly rotate service account keys
gcloud iam service-accounts keys create new-sa-key.json \
    --iam-account=$SA_EMAIL

# Delete old keys after updating your deployment
gcloud iam service-accounts keys delete KEY_ID \
    --iam-account=$SA_EMAIL
```

---

## 🔍 **Monitoring Your Pipeline**

### 1. View Your Data in BigQuery

```sql
-- Check total transactions
SELECT COUNT(*) as total_transactions 
FROM `your-project-id.paypal_data.transactions`;

-- Daily transaction summary
SELECT 
    DATE(transaction_date) as date,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount
FROM `your-project-id.paypal_data.transactions`
GROUP BY DATE(transaction_date)
ORDER BY date DESC;
```

### 2. Monitor GCS Usage

```bash
# Check bucket size
gsutil du -sh gs://$BUCKET_NAME

# List recent files
gsutil ls -l gs://$BUCKET_NAME/paypal/raw/
```

### 3. Airflow Monitoring

- **DAG Status**: Check DAG runs in Airflow UI
- **Task Logs**: Review individual task logs for errors
- **Performance**: Monitor task duration and success rates

---

## 🛠️ **Troubleshooting**

### Common Issues

#### 1. Permission Denied Errors
```bash
# Check service account permissions
gcloud projects get-iam-policy $PROJECT_ID \
    --flatten="bindings[].members" \
    --format="table(bindings.role)" \
    --filter="bindings.members:serviceAccount:$SA_EMAIL"

# Re-assign roles if needed
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/storage.admin"
```

#### 2. BigQuery Dataset Not Found
```bash
# Verify dataset exists
bq ls $PROJECT_ID:

# Create if missing
bq mk --dataset --location=US $PROJECT_ID:paypal_data
```

#### 3. GCS Bucket Access Issues
```bash
# Test bucket access
gsutil ls gs://$BUCKET_NAME

# Check bucket permissions
gsutil iam get gs://$BUCKET_NAME
```

#### 4. Service Account Key Issues
```bash
# Validate JSON format
python3 -c "import json; print('Valid JSON' if json.load(open('sa-key.json')) else 'Invalid JSON')"

# Check key permissions
gcloud auth activate-service-account --key-file=sa-key.json
gcloud auth list
```

---

## 🧹 **Cleanup (When Done)**

### Complete Resource Cleanup
```bash
# Delete BigQuery dataset
bq rm -r -f $PROJECT_ID:paypal_data

# Delete GCS bucket
gsutil rm -r gs://$BUCKET_NAME

# Delete service account
gcloud iam service-accounts delete $SA_EMAIL

# Delete the entire project (nuclear option)
gcloud projects delete $PROJECT_ID
```

### Partial Cleanup (Keep Infrastructure)
```bash
# Just delete data
bq query --use_legacy_sql=false \
"DELETE FROM \`$PROJECT_ID.paypal_data.transactions\` WHERE TRUE"

gsutil rm -r gs://$BUCKET_NAME/paypal/
```

---

## 📞 **Getting Help**

### Before Asking for Help
1. Check Airflow task logs for specific error messages
2. Verify your GCP resources exist and have correct permissions
3. Ensure your `.env` file has the correct project details
4. Test GCP authentication manually

### Where to Get Support
- **Elvis**: For pipeline-specific questions
- **GCP Documentation**: For Google Cloud issues
- **GitHub Issues**: For code-related problems
- **Stack Overflow**: For general troubleshooting

### Useful Commands for Debugging
```bash
# Check Docker containers
docker-compose ps

# View service logs
docker-compose logs airflow-webserver

# Test GCP authentication
docker-compose exec airflow-webserver python -c "
from google.cloud import storage
client = storage.Client()
print('GCP auth successful!')
print(f'Project: {client.project}')
"

# Verify environment variables
docker-compose exec airflow-webserver env | grep -E "(GCP|PAYPAL)"
```

---

## 🎊 **Success Checklist**

Your setup is successful when you can:

- [ ] Access Airflow at http://localhost:8080
- [ ] See `paypal_data_pipeline` DAG in green status
- [ ] Manually trigger the DAG without errors
- [ ] View data in your BigQuery `paypal_data.transactions` table
- [ ] See files uploaded to your GCS bucket
- [ ] Monitor costs in your GCP billing console

**Congratulations! You're now running Elvis's PayPal pipeline with your own GCP resources!** 🚀

---

## 📚 **Additional Resources**

- [Google Cloud IAM Best Practices](https://cloud.google.com/iam/docs/using-iam-securely)
- [BigQuery Cost Optimization](https://cloud.google.com/bigquery/docs/best-practices-costs)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [PayPal API Documentation](https://developer.paypal.com/docs/api/)

---

*This guide enables complete resource isolation while using Elvis's battle-tested pipeline code. No modifications needed to the original codebase!*

**Elvis Built with ❤️ for SOFG IT teams(Also for the greatest bigboss in this world)**
