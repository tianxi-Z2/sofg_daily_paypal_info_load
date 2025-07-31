# scripts/transform.py
"""
PayPal Transaction Transformer

Parses and transforms raw PayPal transaction data into structured format.
"""

import os
import json
import logging
import argparse
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path
from google.cloud import storage
import pandas as pd
from utils import setup_logging, validate_transaction_data


class PayPalTransactionParser:
    """Parse and transform PayPal transaction data"""

    def __init__(self):
        self.logger = setup_logging('PayPalTransactionParser')
        self.parsing_errors = []
        self.validation_errors = []

    def load_raw_data(self, input_path: str) -> Dict:
        """Load raw data from JSON file or GCS"""
        self.logger.info(f"Loading raw data from {input_path}")

        if input_path.startswith('gs://'):
            data = self._load_from_gcs(input_path)
        else:
            with open(input_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

        transactions = data.get('transactions', [])
        self.logger.info(f"Loaded {len(transactions)} transactions")

        # Log metadata if available
        if 'metadata' in data:
            metadata = data['metadata']
            self.logger.info(f"Data extracted at: {metadata.get('extraction_time')}")
            self.logger.info(f"Date range: {metadata.get('date_range')}")

        return data

    def _load_from_gcs(self, gcs_path: str) -> Dict:
        """Load data from Google Cloud Storage"""
        # Parse GCS path
        parts = gcs_path.replace('gs://', '').split('/', 1)
        bucket_name = parts[0]
        blob_name = parts[1] if len(parts) > 1 else ''

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        content = blob.download_as_text()
        return json.loads(content)

    def parse_transaction(self, transaction: Dict) -> Optional[Dict]:
        """Parse a single transaction into structured format"""
        try:
            transaction_info = transaction.get('transaction_info', {})
            payer_info = transaction.get('payer_info', {})
            shipping_info = transaction.get('shipping_info', {})
            cart_info = transaction.get('cart_info', {})

            # Parse amount information
            amount_info = transaction_info.get('transaction_amount', {})
            amount_value = self._safe_float(amount_info.get('value', 0))
            currency_code = amount_info.get('currency_code', 'USD')

            # Parse fee information
            fee_info = transaction_info.get('fee_amount', {})
            fee_value = self._safe_float(fee_info.get('value', 0)) if fee_info else 0

            # Parse dates
            transaction_date = self._parse_timestamp(
                transaction_info.get('transaction_initiation_date', '')
            )
            updated_date = self._parse_timestamp(
                transaction_info.get('transaction_updated_date', '')
            )

            # Create structured transaction
            parsed = {
                # Transaction basics
                'transaction_id': transaction_info.get('transaction_id', ''),
                'paypal_account_id': transaction_info.get('paypal_account_id', ''),
                'transaction_status': transaction_info.get('transaction_status', ''),
                'transaction_subject': transaction_info.get('transaction_subject', ''),
                'transaction_note': transaction_info.get('transaction_note', ''),
                'invoice_id': transaction_info.get('invoice_id', ''),

                # Amounts
                'amount': amount_value,
                'currency_code': currency_code,
                'fee_amount': fee_value,
                'net_amount': amount_value - fee_value,

                # Dates (as timestamps for BigQuery)
                'transaction_date': transaction_date,
                'updated_date': updated_date,

                # Payer information
                'payer_email': payer_info.get('email_address', ''),
                'payer_name': self._get_full_name(payer_info.get('payer_name', {})),
                'payer_country': payer_info.get('country_code', ''),
                'payer_id': payer_info.get('payer_id', ''),

                # Additional info
                'payment_method': self._extract_payment_method(transaction_info),
                'store_info': self._extract_store_info(transaction_info),
                'custom_field': transaction_info.get('custom_field', ''),

                # Shipping
                'shipping_method': shipping_info.get('method', ''),
                'shipping_name': self._get_full_name(shipping_info.get('name', {})),
                'shipping_address': self._format_address(shipping_info.get('address', {})),

                # Items
                'item_count': len(cart_info.get('item_details', [])) if cart_info else 0,
                'items': self._parse_items(cart_info.get('item_details', [])) if cart_info else [],

                # Metadata
                'parsed_at': datetime.now().isoformat()
            }

            # Validate parsed data
            is_valid, validation_errors = validate_transaction_data(parsed)
            if not is_valid:
                self.validation_errors.append({
                    'transaction_id': parsed.get('transaction_id', 'UNKNOWN'),
                    'errors': validation_errors
                })
                self.logger.warning(
                    f"Validation failed for transaction {parsed.get('transaction_id')}: {validation_errors}")

            return parsed

        except Exception as e:
            transaction_id = transaction.get('transaction_info', {}).get('transaction_id', 'UNKNOWN')
            self.logger.error(f"Error parsing transaction {transaction_id}: {str(e)}")
            self.parsing_errors.append({
                'transaction_id': transaction_id,
                'error': str(e),
                'transaction': transaction
            })
            return None

    def _safe_float(self, value: Any) -> float:
        """Safely convert value to float"""
        try:
            return float(value) if value is not None else 0.0
        except (ValueError, TypeError):
            return 0.0

    def _parse_timestamp(self, date_str: str) -> Optional[str]:
        """Parse and standardize timestamp format for BigQuery"""
        if not date_str:
            return None

        try:
            # PayPal format: 2025-07-15T10:23:00-0700
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            # Return in BigQuery TIMESTAMP format (UTC)
            return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
        except Exception:
            self.logger.warning(f"Could not parse date: {date_str}")
            return None

    def _get_full_name(self, name_obj: Dict) -> str:
        """Extract full name from name object"""
        if not name_obj:
            return ''

        parts = []
        if name_obj.get('given_name'):
            parts.append(str(name_obj.get('given_name')).strip())
        if name_obj.get('surname'):
            parts.append(str(name_obj.get('surname')).strip())

        return ' '.join(parts)

    def _format_address(self, address_obj: Dict) -> str:
        """Format address object into string"""
        if not address_obj:
            return ''

        parts = []
        for field in ['address_line_1', 'address_line_2', 'admin_area_2',
                      'admin_area_1', 'postal_code', 'country_code']:
            value = address_obj.get(field)
            if value:
                parts.append(str(value).strip())

        return ', '.join(parts)

    def _extract_payment_method(self, transaction_info: Dict) -> str:
        """Extract payment method from transaction info"""
        tracking_info = transaction_info.get('payment_tracking_info', [])
        if tracking_info and isinstance(tracking_info, list) and len(tracking_info) > 0:
            return tracking_info[0].get('payment_method', '')
        return ''

    def _extract_store_info(self, transaction_info: Dict) -> str:
        """Extract store information"""
        store_info = transaction_info.get('store_info', {})
        if store_info:
            return store_info.get('store_id', '')
        return ''

    def _parse_items(self, items: List[Dict]) -> List[Dict]:
        """Parse item details"""
        parsed_items = []
        for item in items:
            parsed_items.append({
                'item_name': str(item.get('item_name', '')).strip(),
                'item_quantity': str(item.get('item_quantity', '0')),
                'item_unit_price': self._safe_float(
                    item.get('item_unit_price', {}).get('value') if item.get('item_unit_price') else 0
                ),
                'item_amount': self._safe_float(
                    item.get('item_amount', {}).get('value') if item.get('item_amount') else 0
                ),
                'item_description': str(item.get('item_description', '')).strip(),
                'item_sku': str(item.get('sku', '')).strip(),
                'item_category': str(item.get('item_category', '')).strip()
            })
        return parsed_items

    def parse_transactions(self, raw_data: Dict) -> List[Dict]:
        """Parse all transactions with progress tracking"""
        transactions = raw_data.get('transactions', [])
        total = len(transactions)

        if total == 0:
            self.logger.warning("No transactions found in raw data")
            return []

        self.logger.info(f"Starting to parse {total} transactions")

        parsed_transactions = []
        for i, transaction in enumerate(transactions):
            if i % 100 == 0:  # Progress logging
                self.logger.info(f"Parsing progress: {i}/{total} ({i / total * 100:.1f}%)")

            parsed = self.parse_transaction(transaction)
            if parsed:
                parsed_transactions.append(parsed)

        success_rate = len(parsed_transactions) / total * 100 if total > 0 else 0
        self.logger.info(f"Parsing complete: {len(parsed_transactions)}/{total} ({success_rate:.1f}% success)")

        if self.parsing_errors:
            self.logger.warning(f"Failed to parse {len(self.parsing_errors)} transactions")

        if self.validation_errors:
            self.logger.warning(f"Validation failed for {len(self.validation_errors)} transactions")

        return parsed_transactions

    def save_parsed_data(self, parsed_transactions: List[Dict], output_path: str,
                         output_format: str = 'jsonl') -> str:
        """Save parsed data in specified format"""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        if output_format == 'jsonl':
            # Save as JSONL for BigQuery
            with open(output_path, 'w', encoding='utf-8') as f:
                for transaction in parsed_transactions:
                    f.write(json.dumps(transaction, ensure_ascii=False) + '\n')

        elif output_format == 'json':
            # Save as JSON with metadata
            output_data = {
                'metadata': {
                    'parsed_at': datetime.now().isoformat(),
                    'total_transactions': len(parsed_transactions),
                    'parsing_errors': len(self.parsing_errors),
                    'validation_errors': len(self.validation_errors)
                },
                'transactions': parsed_transactions,
                'parsing_errors': self.parsing_errors,
                'validation_errors': self.validation_errors
            }

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)

        elif output_format == 'csv':
            # Save as CSV (flattened)
            df = self._flatten_transactions(parsed_transactions)
            df.to_csv(output_path, index=False)

        self.logger.info(f"Saved {len(parsed_transactions)} transactions to {output_path} ({output_format})")
        return output_path

    def _flatten_transactions(self, transactions: List[Dict]) -> pd.DataFrame:
        """Flatten transactions for CSV output"""
        flattened = []
        for transaction in transactions:
            # Create base record without nested fields
            record = {k: v for k, v in transaction.items()
                      if k not in ['items']}

            # Add item summary
            items = transaction.get('items', [])
            if items:
                record['item_names'] = '; '.join([item.get('item_name', '') for item in items])
                record['total_item_amount'] = sum([item.get('item_amount', 0) for item in items])
            else:
                record['item_names'] = ''
                record['total_item_amount'] = 0

            flattened.append(record)

        return pd.DataFrame(flattened)

    def generate_statistics(self, parsed_transactions: List[Dict]) -> Dict:
        """Generate comprehensive statistics from parsed transactions"""
        if not parsed_transactions:
            return {'error': 'No transactions to analyze'}

        stats = {
            'summary': {
                'total_transactions': len(parsed_transactions),
                'parsing_errors': len(self.parsing_errors),
                'validation_errors': len(self.validation_errors),
                'success_rate': len(parsed_transactions) /
                                (len(parsed_transactions) + len(self.parsing_errors)) * 100
            },
            'status_distribution': {},
            'currency_distribution': {},
            'payment_method_distribution': {},
            'daily_summary': {},
            'amount_statistics': {}
        }

        amounts = []
        daily_data = {}

        for transaction in parsed_transactions:
            # Status distribution
            status = transaction.get('transaction_status', 'UNKNOWN')
            stats['status_distribution'][status] = stats['status_distribution'].get(status, 0) + 1

            # Currency distribution
            currency = transaction.get('currency_code', 'USD')
            stats['currency_distribution'][currency] = stats['currency_distribution'].get(currency, 0) + 1

            # Payment method distribution
            payment_method = transaction.get('payment_method', 'UNKNOWN')
            if payment_method:  # Only count non-empty payment methods
                stats['payment_method_distribution'][payment_method] = \
                    stats['payment_method_distribution'].get(payment_method, 0) + 1

            # Daily summary
            date = transaction.get('transaction_date', '')
            if date:
                date_key = date.split(' ')[0]  # Extract date part
                if date_key not in daily_data:
                    daily_data[date_key] = {
                        'count': 0,
                        'total_amount': 0,
                        'total_fee': 0,
                        'net_amount': 0
                    }
                daily_data[date_key]['count'] += 1
                daily_data[date_key]['total_amount'] += transaction.get('amount', 0)
                daily_data[date_key]['total_fee'] += transaction.get('fee_amount', 0)
                daily_data[date_key]['net_amount'] += transaction.get('net_amount', 0)

            # Amount statistics
            amount = transaction.get('amount', 0)
            if amount > 0:  # Only include positive amounts
                amounts.append(amount)

        # Sort daily summary
        stats['daily_summary'] = dict(sorted(daily_data.items()))

        # Calculate amount statistics
        if amounts:
            stats['amount_statistics'] = {
                'total': sum(amounts),
                'average': sum(amounts) / len(amounts),
                'median': sorted(amounts)[len(amounts) // 2],
                'min': min(amounts),
                'max': max(amounts),
                'count': len(amounts)
            }
        else:
            stats['amount_statistics'] = {
                'total': 0, 'average': 0, 'median': 0,
                'min': 0, 'max': 0, 'count': 0
            }

        return stats


def main():
    """Main entry point for script execution"""
    parser = argparse.ArgumentParser(description='Transform PayPal transactions')
    parser.add_argument('--input-path', required=True, help='Input file path (local or GCS)')
    parser.add_argument('--output-path', required=True, help='Output file path')
    parser.add_argument('--output-format', choices=['json', 'jsonl', 'csv'],
                        default='jsonl', help='Output format')
    parser.add_argument('--stats-output', help='Path to save statistics')
    parser.add_argument('--gcs-bucket', help='GCS bucket for upload')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')

    args = parser.parse_args()

    # Set up logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level)

    try:
        # Create parser
        parser_obj = PayPalTransactionParser()

        # Load raw data
        raw_data = parser_obj.load_raw_data(args.input_path)

        # Parse transactions
        parsed_transactions = parser_obj.parse_transactions(raw_data)

        if not parsed_transactions:
            print("No transactions to save")
            return 1

        # Save parsed data
        local_path = parser_obj.save_parsed_data(
            parsed_transactions=parsed_transactions,
            output_path=args.output_path,
            output_format=args.output_format
        )

        # Generate and save statistics
        if args.stats_output:
            stats = parser_obj.generate_statistics(parsed_transactions)
            with open(args.stats_output, 'w') as f:
                json.dump(stats, f, indent=2, default=str)
            parser_obj.logger.info(f"Saved statistics to {args.stats_output}")

        # Upload to GCS if specified
        if args.gcs_bucket:
            client = storage.Client()
            bucket = client.bucket(args.gcs_bucket)
            blob_name = f"paypal/parsed/{Path(args.output_path).name}"
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(local_path)
            parser_obj.logger.info(f"Uploaded to gs://{args.gcs_bucket}/{blob_name}")

        print(f"Successfully transformed {len(parsed_transactions)} transactions")
        return 0

    except Exception as e:
        logging.error(f"Transformation failed: {str(e)}")
        return 1


if __name__ == "__main__":
    exit(main())