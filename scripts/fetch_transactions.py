# scripts/fetch_transactions.py
"""
PayPal Transaction Fetcher

Extracts transaction data from PayPal Reporting API and saves to GCS.
"""

import os
import json
import logging
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from pathlib import Path
import time
import argparse
from google.cloud import storage
from utils import setup_logging, retry_on_failure


class PayPalTransactionFetcher:
    """PayPal Transaction Fetcher using Reporting API"""

    def __init__(self, client_id: str, client_secret: str, sandbox: bool = True):
        self.client_id = client_id
        self.client_secret = client_secret
        self.sandbox = sandbox
        self.base_url = "https://api-m.sandbox.paypal.com" if sandbox else "https://api-m.paypal.com"
        self.access_token = None
        self.token_expires_at = None
        self.logger = setup_logging('PayPalTransactionFetcher')
        self.session = requests.Session()

        # Set session defaults
        self.session.headers.update({
            'User-Agent': 'PayPal-Pipeline/1.0',
            'Accept': 'application/json'
        })

    @retry_on_failure(max_retries=3, delay=5)
    def get_access_token(self) -> str:
        """Get PayPal access token with retry logic"""
        if self.access_token and self.token_expires_at and datetime.now() < self.token_expires_at:
            return self.access_token

        self.logger.info("Getting PayPal access token...")

        url = f"{self.base_url}/v1/oauth2/token"
        headers = {
            "Accept": "application/json",
            "Accept-Language": "en_US",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = "grant_type=client_credentials"

        response = self.session.post(
            url,
            headers=headers,
            data=data,
            auth=(self.client_id, self.client_secret),
            timeout=30
        )

        if response.status_code == 200:
            token_data = response.json()
            self.access_token = token_data["access_token"]
            expires_in = token_data.get("expires_in", 3600)
            self.token_expires_at = datetime.now() + timedelta(seconds=expires_in - 60)
            self.logger.info("Access token obtained successfully")
            return self.access_token
        else:
            raise Exception(f"Failed to get access token: {response.status_code} - {response.text}")

    def fetch_transactions(self, start_date: str, end_date: str,
                           transaction_status: Optional[str] = None,
                           page_size: int = 500) -> List[Dict]:
        """
        Fetch transaction data using PayPal Reporting API

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            transaction_status: Transaction status filter
            page_size: Number of records per page (max 500)
        """
        all_transactions = []
        page = 1
        max_pages = 100  # Safety limit

        self.logger.info(f"Fetching transactions: {start_date} to {end_date}")
        if transaction_status:
            self.logger.info(f"Filtering by status: {transaction_status}")

        while page <= max_pages:
            try:
                url = f"{self.base_url}/v1/reporting/transactions"
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.get_access_token()}",
                }

                params = {
                    "start_date": f"{start_date}T00:00:00-0000",
                    "end_date": f"{end_date}T23:59:59-0000",
                    "fields": "all",
                    "page_size": min(page_size, 500),
                    "page": page
                }

                if transaction_status:
                    params["transaction_status"] = transaction_status

                self.logger.debug(f"Requesting page {page}...")
                response = self.session.get(url, headers=headers, params=params, timeout=120)

                if response.status_code == 200:
                    data = response.json()
                    transactions = data.get("transaction_details", [])

                    if not transactions:
                        self.logger.info(f"No more data on page {page}")
                        break

                    all_transactions.extend(transactions)
                    self.logger.info(f"Retrieved {len(transactions)} transactions from page {page}")

                    # Check for next page
                    links = data.get("links", [])
                    has_next = any(link.get("rel") == "next" for link in links)

                    if not has_next or len(transactions) < page_size:
                        self.logger.info("All data retrieved")
                        break

                    page += 1
                    time.sleep(0.5)  # Rate limiting

                elif response.status_code == 429:
                    self.logger.warning("Rate limit reached, waiting 60 seconds...")
                    time.sleep(60)
                    continue

                else:
                    self.logger.error(f"API request failed: {response.status_code} - {response.text}")
                    if response.status_code in [401, 403]:
                        # Token might be expired, reset and retry once
                        self.access_token = None
                        if page == 1:  # Only retry on first page to avoid infinite loop
                            continue
                    break

            except Exception as e:
                self.logger.error(f"Error fetching page {page}: {str(e)}")
                break

        self.logger.info(f"Total transactions fetched: {len(all_transactions)}")
        return all_transactions

    def save_raw_data(self, transactions: List[Dict], start_date: str, end_date: str,
                      output_path: str) -> str:
        """Save raw transaction data to JSON file with metadata"""
        metadata = {
            "extraction_time": datetime.now().isoformat(),
            "date_range": {
                "start_date": start_date,
                "end_date": end_date
            },
            "total_transactions": len(transactions),
            "api_environment": "sandbox" if self.sandbox else "production",
            "client_id": self.client_id[-4:].rjust(len(self.client_id), '*'),  # Masked for security
            "pipeline_version": "1.0.0"
        }

        raw_data = {
            "metadata": metadata,
            "transactions": transactions
        }

        # Ensure directory exists
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, indent=2, ensure_ascii=False)

        self.logger.info(f"Saved {len(transactions)} transactions to {output_path}")
        return output_path

    def upload_to_gcs(self, local_path: str, bucket_name: str, blob_name: str) -> str:
        """Upload file to Google Cloud Storage with error handling"""
        try:
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)

            # Set metadata
            blob.metadata = {
                'pipeline': 'paypal-etl',
                'uploaded_at': datetime.now().isoformat(),
                'file_type': 'raw_transactions'
            }

            blob.upload_from_filename(local_path)
            self.logger.info(f"Uploaded to gs://{bucket_name}/{blob_name}")

            return f"gs://{bucket_name}/{blob_name}"

        except Exception as e:
            self.logger.error(f"Failed to upload to GCS: {str(e)}")
            raise


def main():
    """Main entry point for script execution"""
    parser = argparse.ArgumentParser(description='Fetch PayPal transactions')
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--client-id', help='PayPal Client ID')
    parser.add_argument('--client-secret', help='PayPal Client Secret')
    parser.add_argument('--sandbox', action='store_true', help='Use sandbox environment')
    parser.add_argument('--transaction-status', help='Transaction status filter')
    parser.add_argument('--output-path', required=True, help='Output file path')
    parser.add_argument('--gcs-bucket', help='GCS bucket name for upload')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')

    args = parser.parse_args()

    # Set up logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level)

    # Get credentials from args or environment
    client_id = args.client_id or os.environ.get('PAYPAL_CLIENT_ID')
    client_secret = args.client_secret or os.environ.get('PAYPAL_CLIENT_SECRET')

    if not client_id or not client_secret:
        raise ValueError("PayPal credentials must be provided via args or environment variables")

    # Create fetcher
    fetcher = PayPalTransactionFetcher(
        client_id=client_id,
        client_secret=client_secret,
        sandbox=args.sandbox
    )

    try:
        # Fetch transactions
        transactions = fetcher.fetch_transactions(
            start_date=args.start_date,
            end_date=args.end_date,
            transaction_status=args.transaction_status
        )

        # Save raw data
        local_path = fetcher.save_raw_data(
            transactions=transactions,
            start_date=args.start_date,
            end_date=args.end_date,
            output_path=args.output_path
        )

        # Upload to GCS if specified
        if args.gcs_bucket:
            blob_name = f"paypal/raw/{args.start_date}_to_{args.end_date}.json"
            fetcher.upload_to_gcs(local_path, args.gcs_bucket, blob_name)

        print(f"Successfully fetched {len(transactions)} transactions")
        return 0

    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")
        return 1


if __name__ == "__main__":
    exit(main())