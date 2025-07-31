# fix_gcp_auth.sh
# Quick fix for GCP authentication issues in PayPal Pipeline

echo "🔧 Fixing GCP Authentication for PayPal Pipeline"
echo "=================================================="

# Check current sa-key.json status
if [ -d "sa-key.json" ]; then
    echo "❌ Found: sa-key.json is a DIRECTORY (this is the problem!)"
    echo "📝 Moving to backup..."
    mv sa-key.json sa-key.json.backup.dir
    echo "✅ Moved sa-key.json directory to sa-key.json.backup.dir"
fi

# Check if we have a real service account key file
if [ -f "terraform/sa-key.json" ]; then
    echo "✅ Found service account key in terraform/sa-key.json"
    echo "📝 Copying to root directory..."
    cp terraform/sa-key.json ./sa-key.json
    echo "✅ Copied service account key to root directory"
elif [ -f ".terraform/sa-key.json" ]; then
    echo "✅ Found service account key in .terraform/sa-key.json"
    echo "📝 Copying to root directory..."
    cp .terraform/sa-key.json ./sa-key.json
    echo "✅ Copied service account key to root directory"
else
    echo "⚠️  No existing service account key found"
    echo "📝 Creating placeholder file (you'll need to replace with real key)"
    echo '{"type": "service_account", "project_id": "placeholder"}' > sa-key.json
    echo "✅ Created placeholder sa-key.json"
    echo ""
    echo "🚨 IMPORTANT: Replace sa-key.json with your real GCP service account key!"
    echo "   - Download from GCP Console → IAM & Admin → Service Accounts"
    echo "   - Or copy from your terraform output"
fi

# Check file permissions
if [ -f "sa-key.json" ]; then
    chmod 600 sa-key.json
    echo "✅ Set proper permissions on sa-key.json (600)"
fi

# Verify the fix
echo ""
echo "🔍 Verification:"
echo "=================="
if [ -f "sa-key.json" ] && [ ! -d "sa-key.json" ]; then
    echo "✅ sa-key.json is now a FILE (correct!)"
    echo "📏 File size: $(wc -c < sa-key.json) bytes"

    # Check if it's a valid JSON
    if python3 -m json.tool sa-key.json > /dev/null 2>&1; then
        echo "✅ sa-key.json contains valid JSON"

        # Check if it looks like a service account key
        if grep -q "service_account" sa-key.json && grep -q "project_id" sa-key.json; then
            echo "✅ sa-key.json looks like a valid service account key"
        else
            echo "⚠️  sa-key.json might be a placeholder - replace with real key"
        fi
    else
        echo "❌ sa-key.json contains invalid JSON"
    fi
else
    echo "❌ sa-key.json is still not a proper file"
fi

echo ""
echo "🚀 Next Steps:"
echo "=============="
echo "1. If using placeholder: Replace sa-key.json with your real GCP service account key"
echo "2. Restart Airflow services: docker-compose restart airflow-webserver airflow-scheduler"
echo "3. Set Airflow variables if not already set:"
echo "   docker-compose exec airflow-webserver airflow variables set GCP_PROJECT_ID 'your-project-id'"
echo "   docker-compose exec airflow-webserver airflow variables set GCS_BUCKET 'your-bucket-name'"
echo "   docker-compose exec airflow-webserver airflow variables set PAYPAL_CLIENT_ID 'your-client-id'"
echo "   docker-compose exec airflow-webserver airflow variables set PAYPAL_CLIENT_SECRET 'your-secret'"
echo "4. Test the pipeline in Airflow UI"

echo ""
echo "✅ GCP Authentication fix completed!"