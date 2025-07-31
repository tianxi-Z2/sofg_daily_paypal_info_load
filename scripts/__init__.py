# scripts/__init__.py
"""
PayPal Pipeline Scripts Package

This package contains the core ETL components for the PayPal data pipeline:
- fetch_transactions: Extract data from PayPal API
- transform: Parse and transform transaction data
- load_to_bq: Load data to BigQuery
- utils: Common utilities and helpers
"""

__version__ = "1.0.0"
__author__ = "Data Engineering Team"

# Import main classes for easy access
from .fetch_transactions import PayPalTransactionFetcher
from .transform import PayPalTransactionParser
from .load_to_bq import BigQueryLoader
from .utils import setup_logging, load_config

__all__ = [
    "PayPalTransactionFetcher",
    "PayPalTransactionParser",
    "BigQueryLoader",
    "setup_logging",
    "load_config"
]