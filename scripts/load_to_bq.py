# scripts/load_to_bq.py
"""
BigQuery Data Loader

Loads transformed PayPal transaction data to BigQuery with validation and monitoring.
"""

import os
import json
import logging
import argparse
from datetime import datetime
from typing import List, Dict, Optional
from pathlib import Path
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from utils import setup_logging


class BigQueryLoader:
    """Load PayPal transaction data to BigQuery with comprehensive validation"""

    def __init__(self, project_id: str, dataset_id: str, table_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.client = bigquery.Client(project=project_id)
        self.logger = setup_logging('BigQueryLoader')

        # Full table reference
        self.table_ref = f"{project_id}.{dataset_id}.{table_id}"

    def create_dataset_if_not_exists(self) -> None:
        """Create dataset if it doesn't exist"""
        dataset_id_full = f"{self.project_id}.{self.dataset_id}"

        try:
            dataset = self.client.get_dataset(dataset_id_full)
            self.logger.info(f"Dataset {self.dataset_id} already exists")
        except NotFound:
            self.logger.info(f"Creating dataset {self.dataset_id}")
            dataset = bigquery.Dataset(dataset_id_full)
            dataset.location = "US"
            dataset.description = "PayPal transaction data and analytics"

            # Set labels
            dataset.labels = {
                "environment": os.environ.get("ENVIRONMENT", "dev"),
                "team": "data-engineering",
                "project": "paypal-pipeline"
            }

            dataset = self.client.create_dataset(dataset, timeout=30)
            self.logger.info(f"Created dataset {self.dataset_id}")

    def load_schema_from_file(self, schema_path: str) -> List[bigquery.SchemaField]:
        """Load BigQuery schema from JSON file"""
        if not os.path.exists(schema_path):
            self.logger.warning(f"Schema file not found: {schema_path}, using default schema")
            return self.get_default_schema()

        try:
            with open(schema_path, 'r') as f:
                schema_json = json.load(f)

            schema_fields = []
            for field in schema_json:
                # Handle nested fields for RECORD type
                if field.get('type') == 'RECORD' and 'fields' in field:
                    nested_fields = []
                    for nested_field in field['fields']:
                        nested_fields.append(bigquery.SchemaField(
                            nested_field['name'],
                            nested_field['type'],
                            mode=nested_field.get('mode', 'NULLABLE')
                        ))
                    schema_fields.append(bigquery.SchemaField(
                        field['name'],
                        field['type'],
                        mode=field.get('mode', 'NULLABLE'),
                        fields=nested_fields
                    ))
                else:
                    schema_fields.append(bigquery.SchemaField(
                        field['name'],
                        field['type'],
                        mode=field.get('mode', 'NULLABLE'),
                        description=field.get('description', '')
                    ))

            self.logger.info(f"Loaded schema with {len(schema_fields)} fields from {schema_path}")
            return schema_fields

        except Exception as e:
            self.logger.error(f"Error loading schema from {schema_path}: {str(e)}")
            return self.get_default_schema()

    def get_default_schema(self) -> List[bigquery.SchemaField]:
        """Get default BigQuery table schema"""
        return [
            bigquery.SchemaField("transaction_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("paypal_account_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("transaction_status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("transaction_subject", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("transaction_note", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("invoice_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("amount", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("currency_code", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("fee_amount", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("net_amount", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("transaction_date", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("updated_date", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("payer_email", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("payer_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("payer_country", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("payer_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("payment_method", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("store_info", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("custom_field", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("shipping_method", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("shipping_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("shipping_address", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("item_count", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("items", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("item_name", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("item_quantity", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("item_unit_price", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("item_amount", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("item_description", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("item_sku", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("item_category", "STRING", mode="NULLABLE"),
            ]),
            bigquery.SchemaField("parsed_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="NULLABLE"),
        ]

    def create_table_if_not_exists(self, schema_path: Optional[str] = None,
                                   partition_field: str = "transaction_date") -> None:
        """Create table if it doesn't exist"""
        table_id_full = f"{self.project_id}.{self.dataset_id}.{self.table_id}"

        try:
            table = self.client.get_table(table_id_full)
            self.logger.info(f"Table {self.table_id} already exists")
            return
        except NotFound:
            self.logger.info(f"Creating table {self.table_id}")

            # Load schema
            if schema_path:
                schema = self.load_schema_from_file(schema_path)
            else:
                schema = self.get_default_schema()

            table = bigquery.Table(table_id_full, schema=schema)
            table.description = "PayPal transaction data with comprehensive details"

            # Set up partitioning
            if partition_field:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=partition_field
                )
                self.logger.info(f"Table partitioned by {partition_field}")

            # Set up clustering
            table.clustering_fields = ["transaction_status", "currency_code", "payment_method"]

            # Set labels
            table.labels = {
                "environment": os.environ.get("ENVIRONMENT", "dev"),
                "team": "data-engineering"
            }

            table = self.client.create_table(table)
            self.logger.info(f"Created table {self.table_id} with {len(schema)} fields")

    def load_from_file(self, source_path: str, write_disposition: str = "WRITE_APPEND") -> bigquery.LoadJob:
        """Load data from JSON/JSONL file with comprehensive configuration"""
        self.logger.info(f"Loading data from {source_path}")

        # Determine source format
        if source_path.endswith('.jsonl'):
            source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        elif source_path.endswith('.json'):
            source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON  # Assume JSONL format
        else:
            raise ValueError(f"Unsupported file format: {source_path}")

        job_config = bigquery.LoadJobConfig(
            source_format=source_format,
            write_disposition=write_disposition,
            max_bad_records=10,
            ignore_unknown_values=True,
            allow_jagged_rows=False,
            allow_quoted_newlines=False
        )

        # Add job labels for tracking
        job_config.labels = {
            "pipeline": "paypal-etl",
            "environment": os.environ.get("ENVIRONMENT", "dev"),
            "date": datetime.now().strftime("%Y-%m-%d")
        }

        # Load data
        if source_path.startswith('gs://'):
            # Load from GCS
            load_job = self.client.load_table_from_uri(
                source_path,
                self.table_ref,
                job_config=job_config
            )
        else:
            # Load from local file
            with open(source_path, 'rb') as f:
                load_job = self.client.load_table_from_file(
                    f,
                    self.table_ref,
                    job_config=job_config
                )

        self.logger.info(f"Started load job {load_job.job_id}")
        return load_job

    def wait_for_job(self, job: bigquery.LoadJob) -> Dict[str, any]:
        """Wait for job to complete and return detailed results"""
        try:
            job.result()  # Wait for job to complete

            if job.errors:
                self.logger.error(f"Job completed with errors: {job.errors}")
                raise Exception(f"BigQuery load job failed: {job.errors}")

            # Collect job statistics
            job_stats = {
                'job_id': job.job_id,
                'state': job.state,
                'created': job.created.isoformat() if job.created else None,
                'started': job.started.isoformat() if job.started else None,
                'ended': job.ended.isoformat() if job.ended else None,
                'input_files': job.input_files,
                'input_file_bytes': job.input_file_bytes,
                'output_bytes': job.output_bytes,
                'output_rows': job.output_rows,
                'bad_records': getattr(job, 'bad_records', 0),  # secure ask
                'errors': job.errors
            }

            self.logger.info(f"Job completed successfully: {job.output_rows} rows loaded")
            return job_stats

        except Exception as e:
            self.logger.error(f"Job failed: {str(e)}")
            raise

    def validate_data(self, date_filter: Optional[str] = None) -> Dict:
        """Comprehensive data validation with optional date filtering"""
        self.logger.info("Validating loaded data...")

        date_condition = ""
        if date_filter:
            date_condition = f"WHERE DATE(transaction_date) = DATE('{date_filter}')"

        validation_queries = {
            "total_rows": f"""
                SELECT COUNT(*) as count
                FROM `{self.table_ref}`
                {date_condition}
            """,
            "unique_transactions": f"""
                SELECT COUNT(DISTINCT transaction_id) as count
                FROM `{self.table_ref}`
                {date_condition}
            """,
            "null_transaction_ids": f"""
                SELECT COUNT(*) as count
                FROM `{self.table_ref}`
                {date_condition}
                AND transaction_id IS NULL
            """,
            "date_range": f"""
                SELECT 
                    MIN(transaction_date) as min_date,
                    MAX(transaction_date) as max_date,
                    COUNT(DISTINCT DATE(transaction_date)) as unique_dates
                FROM `{self.table_ref}`
                {date_condition}
            """,
            "status_distribution": f"""
                SELECT 
                    COALESCE(transaction_status, 'NULL') as status,
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
                FROM `{self.table_ref}`
                {date_condition}
                GROUP BY transaction_status
                ORDER BY count DESC
            """,
            "amount_summary": f"""
                SELECT 
                    COUNT(*) as total_transactions,
                    COUNTIF(amount > 0) as positive_amounts,
                    COUNTIF(amount = 0) as zero_amounts,
                    COUNTIF(amount < 0) as negative_amounts,
                    ROUND(AVG(amount), 2) as avg_amount,
                    ROUND(SUM(amount), 2) as total_amount,
                    ROUND(MIN(amount), 2) as min_amount,
                    ROUND(MAX(amount), 2) as max_amount
                FROM `{self.table_ref}`
                {date_condition}
            """
        }

        results = {}
        for name, query in validation_queries.items():
            try:
                query_job = self.client.query(query)
                rows = list(query_job.result())

                if name in ["total_rows", "unique_transactions", "null_transaction_ids"]:
                    results[name] = rows[0].count if rows else 0
                elif name == "date_range":
                    if rows and rows[0].min_date:
                        results[name] = {
                            "min_date": rows[0].min_date.isoformat() if rows[0].min_date else None,
                            "max_date": rows[0].max_date.isoformat() if rows[0].max_date else None,
                            "unique_dates": rows[0].unique_dates if rows else 0
                        }
                    else:
                        results[name] = {"min_date": None, "max_date": None, "unique_dates": 0}
                elif name == "amount_summary":
                    if rows:
                        results[name] = dict(rows[0])
                    else:
                        results[name] = {}
                else:
                    results[name] = [dict(row) for row in rows]

            except Exception as e:
                self.logger.error(f"Validation query '{name}' failed: {str(e)}")
                results[name] = {"error": str(e)}

        # Calculate data quality metrics
        total_rows = results.get('total_rows', 0)
        unique_transactions = results.get('unique_transactions', 0)
        null_ids = results.get('null_transaction_ids', 0)

        results['data_quality'] = {
            'completeness_score': (1 - null_ids / total_rows) * 100 if total_rows > 0 else 0,
            'uniqueness_score': (unique_transactions / total_rows) * 100 if total_rows > 0 else 0,
            'total_score': 0  # Will be calculated below
        }

        # Overall data quality score
        completeness = results['data_quality']['completeness_score']
        uniqueness = results['data_quality']['uniqueness_score']
        results['data_quality']['total_score'] = (completeness + uniqueness) / 2

        self.logger.info(f"Validation complete: {total_rows} total rows, "
                         f"{unique_transactions} unique transactions, "
                         f"{results['data_quality']['total_score']:.1f}% quality score")

        return results

    def create_or_update_views(self) -> None:
        """Create useful analytical views"""
        views = {
            "daily_summary": f"""
                CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset_id}.daily_summary` AS
                SELECT 
                    DATE(transaction_date) as date,
                    transaction_status,
                    currency_code,
                    COUNT(*) as transaction_count,
                    ROUND(SUM(amount), 2) as total_amount,
                    ROUND(SUM(fee_amount), 2) as total_fees,
                    ROUND(SUM(net_amount), 2) as total_net,
                    ROUND(AVG(amount), 2) as avg_amount
                FROM `{self.table_ref}`
                WHERE transaction_date IS NOT NULL
                GROUP BY date, transaction_status, currency_code
                ORDER BY date DESC, transaction_count DESC
            """,
            "payer_summary": f"""
                CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset_id}.payer_summary` AS
                SELECT 
                    payer_email,
                    payer_name,
                    payer_country,
                    COUNT(*) as transaction_count,
                    ROUND(SUM(amount), 2) as total_amount,
                    MIN(transaction_date) as first_transaction,
                    MAX(transaction_date) as last_transaction,
                    COUNT(DISTINCT DATE(transaction_date)) as active_days
                FROM `{self.table_ref}`
                WHERE payer_email IS NOT NULL AND payer_email != ''
                GROUP BY payer_email, payer_name, payer_country
                HAVING transaction_count > 1
                ORDER BY total_amount DESC
            """,
            "recent_transactions": f"""
                CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset_id}.recent_transactions` AS
                SELECT *
                FROM `{self.table_ref}`
                WHERE transaction_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
                ORDER BY transaction_date DESC
            """
        }

        for view_name, query in views.items():
            try:
                self.client.query(query).result()
                self.logger.info(f"Created/updated view: {view_name}")
            except Exception as e:
                self.logger.error(f"Failed to create view {view_name}: {str(e)}")

    def update_loaded_timestamp(self, limit: Optional[int] = None) -> int:
        """Update loaded_at timestamp for newly loaded rows"""
        query = f"""
            UPDATE `{self.table_ref}`
            SET loaded_at = CURRENT_TIMESTAMP()
            WHERE loaded_at IS NULL
        """

        if limit:
            query += f" LIMIT {limit}"

        job = self.client.query(query)
        job.result()

        rows_affected = job.num_dml_affected_rows or 0
        self.logger.info(f"Updated {rows_affected} rows with loaded_at timestamp")
        return rows_affected


def main():
    """Main entry point for script execution"""
    parser = argparse.ArgumentParser(description='Load PayPal data to BigQuery')
    parser.add_argument('--source-path', required=True, help='Source file path (local or GCS)')
    parser.add_argument('--project-id', help='GCP Project ID')
    parser.add_argument('--dataset-id', default='paypal_data', help='BigQuery dataset ID')
    parser.add_argument('--table-id', default='transactions', help='BigQuery table ID')
    parser.add_argument('--schema-path', help='Path to BigQuery schema JSON file')
    parser.add_argument('--write-disposition', choices=['WRITE_APPEND', 'WRITE_TRUNCATE', 'WRITE_EMPTY'],
                        default='WRITE_APPEND', help='Write disposition')
    parser.add_argument('--create-views', action='store_true', help='Create analysis views')
    parser.add_argument('--validate', action='store_true', help='Validate loaded data')
    parser.add_argument('--validation-output', help='Path to save validation results')
    parser.add_argument('--date-filter', help='Date filter for validation (YYYY-MM-DD)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')

    args = parser.parse_args()

    # Set up logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level)

    # Get project ID from args or environment
    project_id = args.project_id or os.environ.get('GCP_PROJECT_ID')
    if not project_id:
        raise ValueError("GCP Project ID must be provided via args or GCP_PROJECT_ID environment variable")

    try:
        # Create loader
        loader = BigQueryLoader(
            project_id=project_id,
            dataset_id=args.dataset_id,
            table_id=args.table_id
        )

        # Create dataset and table if needed
        loader.create_dataset_if_not_exists()
        loader.create_table_if_not_exists(schema_path=args.schema_path)

        # Load data
        job = loader.load_from_file(
            source_path=args.source_path,
            write_disposition=args.write_disposition
        )

        # Wait for job to complete and get statistics
        job_stats = loader.wait_for_job(job)

        # Update loaded timestamp
        updated_rows = loader.update_loaded_timestamp()

        # Create views if requested
        if args.create_views:
            loader.create_or_update_views()

        # Validate if requested
        validation_results = None
        if args.validate:
            validation_results = loader.validate_data(date_filter=args.date_filter)

            if args.validation_output:
                combined_results = {
                    'job_statistics': job_stats,
                    'validation_results': validation_results,
                    'updated_rows': updated_rows,
                    'timestamp': datetime.now().isoformat()
                }

                with open(args.validation_output, 'w') as f:
                    json.dump(combined_results, f, indent=2, default=str)
                loader.logger.info(f"Saved results to {args.validation_output}")

        print(f"Successfully loaded {job_stats.get('output_rows', 0)} rows to BigQuery")
        if validation_results:
            quality_score = validation_results.get('data_quality', {}).get('total_score', 0)
            print(f"Data quality score: {quality_score:.1f}%")

        return 0

    except Exception as e:
        logging.error(f"Load operation failed: {str(e)}")
        return 1


if __name__ == "__main__":
    exit(main())