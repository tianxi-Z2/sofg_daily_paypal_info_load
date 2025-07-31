# scripts/utils.py
"""
Utility functions for PayPal Pipeline

Common utilities and helpers used across the pipeline components.
"""

import os
import sys
import json
import yaml
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional, Callable
from functools import wraps


def setup_logging(name: str, level: str = None) -> logging.Logger:
    """Set up structured logging for pipeline components"""
    logger = logging.getLogger(name)

    # Set level from environment or parameter
    log_level = level or os.environ.get('LOG_LEVEL', 'INFO')
    logger.setLevel(getattr(logging, log_level.upper()))

    # Avoid duplicate handlers
    if logger.handlers:
        return logger

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)

    # Formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler if LOG_FILE is set
    log_file = os.environ.get('LOG_FILE')
    if log_file:
        try:
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            logger.warning(f"Could not create file handler for {log_file}: {e}")

    return logger


def load_config(config_path: str) -> Dict:
    """Load configuration from YAML or JSON file"""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, 'r', encoding='utf-8') as f:
        if config_path.endswith(('.yml', '.yaml')):
            config = yaml.safe_load(f)
        elif config_path.endswith('.json'):
            config = json.load(f)
        else:
            raise ValueError(f"Unsupported config file format: {config_path}")

    # Environment variable substitution
    config = substitute_env_vars(config)
    return config


def substitute_env_vars(data: Any) -> Any:
    """Recursively substitute environment variables in configuration"""
    if isinstance(data, dict):
        return {key: substitute_env_vars(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [substitute_env_vars(item) for item in data]
    elif isinstance(data, str) and data.startswith('${') and data.endswith('}'):
        env_var = data[2:-1]
        default_value = None

        # Handle default values: ${VAR_NAME:default}
        if ':' in env_var:
            env_var, default_value = env_var.split(':', 1)

        return os.environ.get(env_var, default_value)
    else:
        return data


def retry_on_failure(max_retries: int = 3, delay: float = 1.0,
                     backoff: float = 2.0, exceptions: Tuple = (Exception,)):
    """Decorator for retrying function calls on failure with exponential backoff"""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_retries:
                        break

                    logger = logging.getLogger(func.__module__)
                    logger.warning(f"Attempt {attempt + 1}/{max_retries + 1} failed for {func.__name__}: {e}")
                    logger.info(f"Retrying in {current_delay} seconds...")

                    time.sleep(current_delay)
                    current_delay *= backoff

            raise last_exception

        return wrapper

    return decorator


def validate_transaction_data(transaction: Dict) -> Tuple[bool, List[str]]:
    """Validate transaction data for completeness and correctness"""
    errors = []

    # Required fields
    required_fields = ['transaction_id']
    for field in required_fields:
        if not transaction.get(field):
            errors.append(f"Missing required field: {field}")

    # Data type validations
    if 'amount' in transaction:
        try:
            amount = float(transaction['amount'])
            if amount < 0:
                errors.append("Amount cannot be negative")
        except (ValueError, TypeError):
            errors.append("Invalid amount format")

    if 'transaction_date' in transaction and transaction['transaction_date']:
        if not isinstance(transaction['transaction_date'], str):
            errors.append("Transaction date must be a string")

    # Business rule validations
    if transaction.get('transaction_status') and transaction['transaction_status'] not in [
        'P', 'S', 'D', 'V', 'F', 'Pending', 'Success', 'Denied', 'Reversed', 'Failed'
    ]:
        errors.append(f"Invalid transaction status: {transaction.get('transaction_status')}")

    if transaction.get('currency_code') and len(transaction['currency_code']) != 3:
        errors.append("Currency code must be 3 characters")

    return len(errors) == 0, errors


def format_bytes(bytes_value: int) -> str:
    """Format bytes into human readable format"""
    if bytes_value == 0:
        return "0B"

    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while bytes_value >= 1024 and i < len(size_names) - 1:
        bytes_value /= 1024.0
        i += 1

    return f"{bytes_value:.1f}{size_names[i]}"


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human readable format"""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        remaining_seconds = seconds % 60
        return f"{minutes}m {remaining_seconds:.1f}s"
    else:
        hours = int(seconds // 3600)
        remaining_minutes = int((seconds % 3600) // 60)
        return f"{hours}h {remaining_minutes}m"


def get_date_range(days_back: int = 1) -> Tuple[str, str]:
    """Get date range for data extraction"""
    end_date = datetime.now() - timedelta(days=1)  # Yesterday
    start_date = end_date - timedelta(days=days_back - 1)

    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')


def parse_gcs_path(gcs_path: str) -> Tuple[str, str]:
    """Parse GCS path into bucket and object name"""
    if not gcs_path.startswith('gs://'):
        raise ValueError(f"Invalid GCS path: {gcs_path}")

    path_parts = gcs_path[5:].split('/', 1)
    bucket_name = path_parts[0]
    object_name = path_parts[1] if len(path_parts) > 1 else ''

    return bucket_name, object_name


def mask_sensitive_data(data: str, visible_chars: int = 4) -> str:
    """Mask sensitive data showing only last few characters"""
    if not data or len(data) <= visible_chars:
        return '*' * len(data) if data else ''

    return '*' * (len(data) - visible_chars) + data[-visible_chars:]


def create_run_id() -> str:
    """Create a unique run ID for pipeline execution"""
    return datetime.now().strftime('%Y%m%d_%H%M%S')


def check_required_env_vars(required_vars: List[str]) -> None:
    """Check if all required environment variables are set"""
    missing_vars = []
    for var in required_vars:
        if not os.environ.get(var):
            missing_vars.append(var)

    if missing_vars:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")


def safe_json_loads(json_string: str, default: Any = None) -> Any:
    """Safely parse JSON string with default fallback"""
    try:
        return json.loads(json_string)
    except (json.JSONDecodeError, TypeError):
        return default


def calculate_percentage(part: int, total: int, decimal_places: int = 1) -> float:
    """Calculate percentage with handling for zero division"""
    if total == 0:
        return 0.0
    return round((part / total) * 100, decimal_places)


def get_pipeline_metadata() -> Dict[str, str]:
    """Get standard pipeline metadata"""
    return {
        'pipeline_name': 'paypal-data-pipeline',
        'version': '1.0.0',
        'environment': os.environ.get('ENVIRONMENT', 'dev'),
        'run_id': create_run_id(),
        'timestamp': datetime.now().isoformat(),
        'hostname': os.environ.get('HOSTNAME', 'unknown')
    }


class ProgressLogger:
    """Utility class for logging progress of long-running operations"""

    def __init__(self, total: int, name: str = "Processing",
                 log_interval: int = 100, logger: logging.Logger = None):
        self.total = total
        self.name = name
        self.log_interval = log_interval
        self.logger = logger or logging.getLogger(__name__)
        self.processed = 0
        self.start_time = time.time()
        self.last_log_time = self.start_time

    def update(self, increment: int = 1) -> None:
        """Update progress and log if necessary"""
        self.processed += increment
        current_time = time.time()

        # Log at intervals or at completion
        if (self.processed % self.log_interval == 0 or
                self.processed >= self.total or
                current_time - self.last_log_time > 30):  # Also log every 30 seconds

            elapsed = current_time - self.start_time
            percentage = (self.processed / self.total) * 100
            rate = self.processed / elapsed if elapsed > 0 else 0

            if self.processed < self.total and rate > 0:
                eta_seconds = (self.total - self.processed) / rate
                eta_str = f", ETA: {format_duration(eta_seconds)}"
            else:
                eta_str = ""

            self.logger.info(
                f"{self.name}: {self.processed}/{self.total} "
                f"({percentage:.1f}%) - {rate:.1f}/sec{eta_str}"
            )
            self.last_log_time = current_time

    def finish(self) -> None:
        """Log completion"""
        elapsed = time.time() - self.start_time
        rate = self.processed / elapsed if elapsed > 0 else 0

        self.logger.info(
            f"{self.name} complete: {self.processed} items in "
            f"{format_duration(elapsed)} ({rate:.1f}/sec)"
        )