"""
PayPal Pipeline Integration Test

Test script to verify all components of the PayPal pipeline are working correctly.
"""

import os
import sys
import json
import subprocess
from datetime import datetime, timedelta
from typing import Dict, List, Tuple


def run_command(command: str) -> Tuple[bool, str]:
    """Run shell command and return result"""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True, check=True)
        return True, result.stdout.strip()
    except subprocess.CalledProcessError as e:
        return False, e.stderr.strip()


def check_file_exists(path: str, description: str) -> bool:
    """Check if a file exists"""
    if os.path.exists(path):
        print(f"✅ {description}: {path}")
        return True
    else:
        print(f"❌ {description}: {path} NOT FOUND")
        return False


def check_directory_structure() -> bool:
    """Check if all required directories and files exist"""
    print("🔍 Checking Directory Structure...")

    required_files = [
        ("scripts/fetch_transactions.py", "PayPal API fetcher"),
        ("scripts/transform.py", "Data transformer"),
        ("scripts/load_to_bq.py", "BigQuery loader"),
        ("scripts/utils.py", "Utility functions"),
        ("config/schema.json", "BigQuery schema"),
        ("config/pipeline_config.yaml", "Pipeline configuration"),
        ("dags/paypal_dag.py", "Airflow DAG"),
        ("docker-compose.yml", "Docker configuration"),
    ]

    all_exist = True
    for file_path, description in required_files:
        if not check_file_exists(file_path, description):
            all_exist = False

    return all_exist


def check_gcp_auth() -> bool:
    """Check GCP authentication setup"""
    print("\n🔍 Checking GCP Authentication...")

    # Check if sa-key.json exists and is a file
    if not os.path.exists("sa-key.json"):
        print("❌ sa-key.json not found")
        return False

    if os.path.isdir("sa-key.json"):
        print("❌ sa-key.json is a directory (should be a file)")
        return False

    print("✅ sa-key.json exists and is a file")

    # Check if it's valid JSON
    try:
        with open("sa-key.json", 'r') as f:
            key_data = json.load(f)

        if key_data.get("type") == "service_account":
            print("✅ sa-key.json contains service account credentials")
            project_id = key_data.get("project_id", "unknown")
            print(f"📝 Project ID: {project_id}")
            return True
        else:
            print("⚠️  sa-key.json might be a placeholder file")
            return False
    except json.JSONDecodeError:
        print("❌ sa-key.json contains invalid JSON")
        return False


def check_docker_status() -> bool:
    """Check if Docker services are running"""
    print("\n🔍 Checking Docker Status...")

    # Check if docker-compose is available
    success, output = run_command("docker-compose --version")
    if not success:
        print("❌ Docker Compose not available")
        return False
    print(f"✅ Docker Compose: {output}")

    # Check running containers
    success, output = run_command("docker-compose ps")
    if success:
        if "airflow-webserver" in output and "airflow-scheduler" in output:
            print("✅ Airflow services are running")
            return True
        else:
            print("⚠️  Airflow services might not be running")
            print("💡 Try: docker-compose up -d")
            return False
    else:
        print("❌ Could not check Docker status")
        return False


def check_airflow_connection() -> bool:
    """Check if Airflow is accessible"""
    print("\n🔍 Checking Airflow Connection...")

    # Try to access Airflow CLI
    success, output = run_command("docker-compose exec -T airflow-webserver airflow version")
    if success:
        print(f"✅ Airflow CLI accessible: {output}")
        return True
    else:
        print("❌ Could not access Airflow CLI")
        print("💡 Make sure Airflow containers are running")
        return False


def check_airflow_variables() -> bool:
    """Check if required Airflow variables are set"""
    print("\n🔍 Checking Airflow Variables...")

    required_variables = [
        "GCP_PROJECT_ID",
        "GCS_BUCKET",
        "PAYPAL_CLIENT_ID",
        "PAYPAL_CLIENT_SECRET",
    ]

    all_set = True
    for var in required_variables:
        success, output = run_command(f"docker-compose exec -T airflow-webserver airflow variables get {var}")
        if success and output and not output.startswith("Variable"):
            # Hide sensitive values
            if "SECRET" in var or "PASSWORD" in var:
                print(f"✅ {var}: [HIDDEN]")
            else:
                print(f"✅ {var}: {output[:50]}...")
        else:
            print(f"❌ {var}: Not set")
            all_set = False

    return all_set


def check_dag_status() -> bool:
    """Check if the PayPal DAG is loaded correctly"""
    print("\n🔍 Checking DAG Status...")

    # Check if DAG is listed
    success, output = run_command("docker-compose exec -T airflow-webserver airflow dags list")
    if success and "paypal_data_pipeline" in output:
        print("✅ PayPal DAG found in DAG list")
    else:
        print("❌ PayPal DAG not found")
        return False

    # Check for import errors
    success, output = run_command("docker-compose exec -T airflow-webserver airflow dags list-import-errors")
    if success:
        if output.strip():
            print("❌ DAG import errors detected:")
            print(output)
            return False
        else:
            print("✅ No DAG import errors")
            return True
    else:
        print("⚠️  Could not check for import errors")
        return False


def test_script_imports() -> bool:
    """Test if all scripts can be imported correctly"""
    print("\n🔍 Testing Script Imports...")

    # Test Python path setup
    sys.path.insert(0, 'scripts')

    scripts_to_test = [
        ('utils', 'Utility functions'),
        ('fetch_transactions', 'PayPal API fetcher'),
        ('transform', 'Data transformer'),
        ('load_to_bq', 'BigQuery loader'),
    ]

    all_imported = True
    for script_name, description in scripts_to_test:
        try:
            __import__(script_name)
            print(f"✅ {description}: Import successful")
        except ImportError as e:
            print(f"❌ {description}: Import failed - {str(e)}")
            all_imported = False
        except Exception as e:
            print(f"⚠️  {description}: Import warning - {str(e)}")

    return all_imported


def test_configuration_files() -> bool:
    """Test if configuration files are valid"""
    print("\n🔍 Testing Configuration Files...")

    all_valid = True

    # Test schema.json
    try:
        with open('config/schema.json', 'r') as f:
            schema = json.load(f)
        print(f"✅ BigQuery schema: {len(schema)} fields defined")
    except Exception as e:
        print(f"❌ BigQuery schema: {str(e)}")
        all_valid = False

    # Test pipeline_config.yaml
    try:
        import yaml
        with open('config/pipeline_config.yaml', 'r') as f:
            config = yaml.safe_load(f)
        print(f"✅ Pipeline config: {len(config)} sections defined")
    except ImportError:
        print("⚠️  PyYAML not available, skipping YAML validation")
    except Exception as e:
        print(f"❌ Pipeline config: {str(e)}")
        all_valid = False

    return all_valid


def run_sample_test() -> bool:
    """Run a simple test of the transformation logic"""
    print("\n🔍 Running Sample Data Test...")

    try:
        # Import transformer
        sys.path.insert(0, 'scripts')
        from transform import PayPalTransactionParser

        # Create sample data matching your PayPal format
        sample_data = {
            "metadata": {"total_transactions": 1},
            "transactions": [{
                "transaction_info": {
                    "transaction_id": "TEST123",
                    "transaction_amount": {"currency_code": "USD", "value": "100.00"},
                    "transaction_status": "S",
                    "transaction_initiation_date": "2025-07-30T10:00:00+00:00",
                    "fee_amount": {"currency_code": "USD", "value": "3.50"}
                },
                "payer_info": {
                    "email_address": "test@example.com",
                    "payer_name": {"given_name": "Test", "surname": "User"},
                    "country_code": "US"
                },
                "cart_info": {
                    "item_details": [{
                        "item_name": "Test Product",
                        "item_quantity": "1",
                        "item_amount": {"currency_code": "USD", "value": "100.00"}
                    }]
                }
            }]
        }

        # Test transformation
        parser = PayPalTransactionParser()
        parsed = parser.parse_transactions(sample_data)

        if parsed and len(parsed) == 1:
            print("✅ Sample transformation successful")
            print(f"📝 Parsed transaction ID: {parsed[0].get('transaction_id')}")
            return True
        else:
            print("❌ Sample transformation failed")
            return False

    except Exception as e:
        print(f"❌ Sample test failed: {str(e)}")
        return False


def generate_recommendations(results: Dict[str, bool]) -> None:
    """Generate recommendations based on test results"""
    print("\n💡 Recommendations:")
    print("=" * 50)

    if not results.get('directory_structure'):
        print("🔧 Fix missing files:")
        print("   - Ensure all scripts are in the scripts/ directory")
        print("   - Check that config files exist in config/ directory")

    if not results.get('gcp_auth'):
        print("🔧 Fix GCP authentication:")
        print("   - Run: ./fix_gcp_auth.sh")
        print("   - Or manually copy your service account key to sa-key.json")

    if not results.get('docker_status'):
        print("🔧 Start Docker services:")
        print("   - Run: docker-compose up -d")
        print("   - Wait a few minutes for services to initialize")

    if not results.get('airflow_variables'):
        print("🔧 Set Airflow variables:")
        print("   - Run the variable setup commands from the guide")
        print("   - Use your real PayPal and GCP credentials")

    if not results.get('dag_status'):
        print("🔧 Fix DAG issues:")
        print("   - Replace dags/paypal_dag.py with the fixed version")
        print("   - Restart Airflow: docker-compose restart airflow-webserver airflow-scheduler")

    if all(results.values()):
        print("🎉 All tests passed! Your pipeline is ready to run!")
        print("🚀 Next steps:")
        print("   1. Visit http://localhost:8080")
        print("   2. Enable the 'paypal_data_pipeline' DAG")
        print("   3. Trigger a test run")
        print("   4. Monitor the execution in Airflow UI")


def main():
    """Main test function"""
    print("🧪 PayPal Pipeline Integration Test")
    print("=" * 50)

    # Run all tests
    test_results = {
        'directory_structure': check_directory_structure(),
        'gcp_auth': check_gcp_auth(),
        'docker_status': check_docker_status(),
        'airflow_connection': check_airflow_connection(),
        'airflow_variables': check_airflow_variables(),
        'dag_status': check_dag_status(),
        'script_imports': test_script_imports(),
        'configuration_files': test_configuration_files(),
        'sample_test': run_sample_test(),
    }

    # Print summary
    print("\n📊 Test Results Summary:")
    print("=" * 50)
    passed = sum(test_results.values())
    total = len(test_results)

    for test_name, result in test_results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test_name:20s}: {status}")

    print(f"\nOverall: {passed}/{total} tests passed ({passed / total * 100:.1f}%)")

    # Generate recommendations
    generate_recommendations(test_results)

    return 0 if all(test_results.values()) else 1


if __name__ == "__main__":
    exit(main())