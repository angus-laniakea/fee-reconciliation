#!/usr/bin/env python3
"""
Daily Fee Digest Script

Reads exegy trade lists from S3, calculates total fees for options and futures,
and sends a webhook notification with the summary.
"""

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import boto3
import requests
import yaml


@dataclass
class FeeConfig:
    """Fee configuration for a single instrument type."""
    exchange_fee: float
    clearing_fee: float
    regulatory_fee: float

    @property
    def total_per_contract(self) -> float:
        return self.exchange_fee + self.clearing_fee + self.regulatory_fee


@dataclass
class TradeSummary:
    """Summary of trades and fees for a single instrument type."""
    instrument_type: str
    trade_count: int
    total_contracts: int
    total_fees: float


def load_config(config_path: str = "config.yaml") -> dict:
    """Load configuration from YAML file."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(path) as f:
        return yaml.safe_load(f)


def get_fee_config(config: dict, instrument_type: str) -> FeeConfig:
    """Get fee configuration for a specific instrument type."""
    fees = config["fees"].get(instrument_type, {})
    return FeeConfig(
        exchange_fee=fees.get("exchange_fee", 0),
        clearing_fee=fees.get("clearing_fee", 0),
        regulatory_fee=fees.get("regulatory_fee", 0),
    )


def get_trade_file_key(prefix: str, date: datetime) -> str:
    """
    Generate the S3 key for a trade file based on date.
    Adjust the pattern based on your actual file naming convention.
    """
    date_str = date.strftime("%Y%m%d")
    return f"{prefix}trades_{date_str}.csv"


def download_trade_file(
    s3_client,
    bucket: str,
    key: str,
) -> Optional[str]:
    """Download trade file from S3 and return its contents."""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read().decode("utf-8")
    except s3_client.exceptions.NoSuchKey:
        print(f"Trade file not found: s3://{bucket}/{key}")
        return None
    except Exception as e:
        print(f"Error downloading trade file: {e}")
        return None


def parse_trades(csv_content: str) -> list[dict]:
    """
    Parse CSV trade data into a list of trade dictionaries.
    
    Expected CSV format (adjust based on actual exegy format):
    timestamp,symbol,side,quantity,price,instrument_type,exchange
    
    Where instrument_type is 'option' or 'future'
    """
    lines = csv_content.strip().split("\n")
    if len(lines) < 2:
        return []
    
    header = lines[0].lower().split(",")
    trades = []
    
    for line in lines[1:]:
        if not line.strip():
            continue
        values = line.split(",")
        trade = dict(zip(header, values))
        trades.append(trade)
    
    return trades


def calculate_fees(
    trades: list[dict],
    options_config: FeeConfig,
    futures_config: FeeConfig,
) -> tuple[TradeSummary, TradeSummary]:
    """Calculate total fees for options and futures trades."""
    options_trades = [t for t in trades if t.get("instrument_type", "").lower() in ("option", "options")]
    futures_trades = [t for t in trades if t.get("instrument_type", "").lower() in ("future", "futures")]
    
    options_contracts = sum(int(t.get("quantity", 0)) for t in options_trades)
    futures_contracts = sum(int(t.get("quantity", 0)) for t in futures_trades)
    
    options_summary = TradeSummary(
        instrument_type="options",
        trade_count=len(options_trades),
        total_contracts=options_contracts,
        total_fees=options_contracts * options_config.total_per_contract,
    )
    
    futures_summary = TradeSummary(
        instrument_type="futures",
        trade_count=len(futures_trades),
        total_contracts=futures_contracts,
        total_fees=futures_contracts * futures_config.total_per_contract,
    )
    
    return options_summary, futures_summary


def format_currency(amount: float) -> str:
    """Format amount as currency string."""
    return f"${amount:,.2f}"


def build_webhook_payload(
    date: datetime,
    options_summary: TradeSummary,
    futures_summary: TradeSummary,
    options_config: FeeConfig,
    futures_config: FeeConfig,
) -> dict:
    """Build the webhook payload with fee summary."""
    total_fees = options_summary.total_fees + futures_summary.total_fees
    total_trades = options_summary.trade_count + futures_summary.trade_count
    total_contracts = options_summary.total_contracts + futures_summary.total_contracts
    
    return {
        "date": date.strftime("%Y-%m-%d"),
        "summary": {
            "total_fees": total_fees,
            "total_fees_formatted": format_currency(total_fees),
            "total_trades": total_trades,
            "total_contracts": total_contracts,
        },
        "options": {
            "trade_count": options_summary.trade_count,
            "total_contracts": options_summary.total_contracts,
            "total_fees": options_summary.total_fees,
            "total_fees_formatted": format_currency(options_summary.total_fees),
            "fee_per_contract": options_config.total_per_contract,
        },
        "futures": {
            "trade_count": futures_summary.trade_count,
            "total_contracts": futures_summary.total_contracts,
            "total_fees": futures_summary.total_fees,
            "total_fees_formatted": format_currency(futures_summary.total_fees),
            "fee_per_contract": futures_config.total_per_contract,
        },
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }


def send_webhook(
    url: str,
    payload: dict,
    headers: Optional[dict] = None,
) -> bool:
    """Send webhook notification with the fee summary."""
    default_headers = {"Content-Type": "application/json"}
    if headers:
        default_headers.update(headers)
    
    try:
        response = requests.post(
            url,
            json=payload,
            headers=default_headers,
            timeout=30,
        )
        response.raise_for_status()
        print(f"Webhook sent successfully. Status: {response.status_code}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Failed to send webhook: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Digest exegy trade list and send fee summary webhook"
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)",
    )
    parser.add_argument(
        "--date",
        help="Date to process (YYYY-MM-DD format, default: today)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print payload without sending webhook",
    )
    args = parser.parse_args()

    # Load configuration
    try:
        config = load_config(args.config)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)

    # Determine date to process
    if args.date:
        process_date = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        process_date = datetime.now()

    print(f"Processing trades for: {process_date.strftime('%Y-%m-%d')}")

    # Get fee configurations
    options_config = get_fee_config(config, "options")
    futures_config = get_fee_config(config, "futures")

    print(f"Options fee per contract: {format_currency(options_config.total_per_contract)}")
    print(f"Futures fee per contract: {format_currency(futures_config.total_per_contract)}")

    # Initialize S3 client
    s3_client = boto3.client("s3")
    bucket = config["s3"]["bucket"]
    prefix = config["s3"]["prefix"]

    # Download and parse trade file
    trade_file_key = get_trade_file_key(prefix, process_date)
    print(f"Fetching: s3://{bucket}/{trade_file_key}")

    csv_content = download_trade_file(s3_client, bucket, trade_file_key)
    if csv_content is None:
        print("No trade file found for this date. Exiting.")
        sys.exit(1)

    trades = parse_trades(csv_content)
    print(f"Parsed {len(trades)} trades")

    # Calculate fees
    options_summary, futures_summary = calculate_fees(
        trades, options_config, futures_config
    )

    # Build webhook payload
    payload = build_webhook_payload(
        process_date,
        options_summary,
        futures_summary,
        options_config,
        futures_config,
    )

    # Print summary
    print("\n--- Fee Summary ---")
    print(f"Options: {options_summary.trade_count} trades, "
          f"{options_summary.total_contracts} contracts, "
          f"{format_currency(options_summary.total_fees)} fees")
    print(f"Futures: {futures_summary.trade_count} trades, "
          f"{futures_summary.total_contracts} contracts, "
          f"{format_currency(futures_summary.total_fees)} fees")
    print(f"Total Fees: {payload['summary']['total_fees_formatted']}")
    print("-------------------\n")

    if args.dry_run:
        print("Dry run - webhook payload:")
        print(json.dumps(payload, indent=2))
        return

    # Send webhook
    webhook_config = config.get("webhook", {})
    webhook_url = webhook_config.get("url")
    webhook_headers = webhook_config.get("headers")

    if not webhook_url:
        print("Error: No webhook URL configured")
        sys.exit(1)

    success = send_webhook(webhook_url, payload, webhook_headers)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
