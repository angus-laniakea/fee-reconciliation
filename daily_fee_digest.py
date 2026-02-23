#!/usr/bin/env python3
"""
Daily Fee Digest Script

Reads exegy trade lists from S3, calculates total fees for options and futures,
and sends a webhook notification with the summary.
"""

import argparse
import json
import logging
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import boto3
import requests
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


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


def load_config(config_path: str = "/home/ec2-user/fees/config.yaml") -> dict:
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


def filter_trades_by_session(trades: list[dict], process_date: datetime) -> list[dict]:
    """
    Filter trades to only include those from the last trading session.
    
    Trading session starts at 5pm the previous calendar day.
    For example, if process_date is 2024-01-15, we include trades
    from 2024-01-14 17:00:00 onwards.
    """
    # Session starts at 5pm the previous day
    session_start = process_date.replace(hour=17, minute=0, second=0, microsecond=0) - timedelta(days=1)
    
    filtered_trades = []
    for trade in trades:
        # Skip expiration trades
        if trade.get("trade_source", "").upper() == "EXPIRATION":
            continue
        
        timestamp_str = trade.get("trade_datetime", "")
        if not timestamp_str:
            continue
        
        try:
            # Parse format: MM/DD/YYYY-HH:MM:SS (e.g., '02/12/2026-03:10:20')
            trade_time = datetime.strptime(timestamp_str, "%m/%d/%Y-%H:%M:%S")
            
            if trade_time >= session_start:
                filtered_trades.append(trade)
        except Exception as e:
            logging.warning(f"Error parsing trade timestamp '{timestamp_str}': {e}")
            continue
    
    return filtered_trades


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


def build_fee_message(
    date: datetime,
    options_summary: TradeSummary,
    futures_summary: TradeSummary,
    options_config: FeeConfig,
    futures_config: FeeConfig,
) -> tuple[str, dict]:
    """Build the fee summary message and data."""
    total_fees = options_summary.total_fees + futures_summary.total_fees
    total_trades = options_summary.trade_count + futures_summary.trade_count
    total_contracts = options_summary.total_contracts + futures_summary.total_contracts
    
    message_lines = [
        f"**Total Fees: {format_currency(total_fees)}**",
        "",
        f"📊 **Options**",
        f"  • Trades: {options_summary.trade_count:,}",
        f"  • Contracts: {options_summary.total_contracts:,}",
        f"  • Fees: {format_currency(options_summary.total_fees)} @ {format_currency(options_config.total_per_contract)}/contract",
        "",
        f"📈 **Futures**",
        f"  • Trades: {futures_summary.trade_count:,}",
        f"  • Contracts: {futures_summary.total_contracts:,}",
        f"  • Fees: {format_currency(futures_summary.total_fees)} @ {format_currency(futures_config.total_per_contract)}/contract",
        "",
        f"**Totals:** {total_trades:,} trades, {total_contracts:,} contracts",
    ]
    
    summary_data = {
        "total_fees": total_fees,
        "total_fees_formatted": format_currency(total_fees),
        "total_trades": total_trades,
        "total_contracts": total_contracts,
    }
    
    return "\n".join(message_lines), summary_data


def send_teams_message(
    webhook_url: str,
    message: str,
    title_suffix: str = "",
) -> bool:
    """Send an Adaptive Card message to Microsoft Teams."""
    today = datetime.now().strftime("%Y-%m-%d")
    title = f"Daily Fee Digest {today}"
    if title_suffix:
        title += f" ({title_suffix})"

    payload = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "contentUrl": None,
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": [
                    {"type": "TextBlock", "size": "Large", "weight": "Bolder", "text": title},
                    {"type": "TextBlock", "text": message, "wrap": True},
                    {"type": "TextBlock", "text": f"Timestamp (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}", "isSubtle": True, "spacing": "Small"}
                ]
            }
        }]
    }

    try:
        r = requests.post(webhook_url, json=payload, timeout=10)
        r.raise_for_status()
        logging.info("Adaptive Card successfully sent to Teams: %s", title)
        return True
    except Exception as e:
        logging.error("Failed to send Teams Adaptive Card: %s", e)
        logging.debug(traceback.format_exc())
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Digest exegy trade list and send fee summary webhook"
    )
    parser.add_argument(
        "--config",
        default="/home/ec2-user/fees/config.yaml",
        help="Path to configuration file (default: /home/ec2-user/fees/config.yaml)",
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

    all_trades = parse_trades(csv_content)
    print(f"Parsed {len(all_trades)} trades from file")
    
    # Filter to only include trades from the last trading session (since 5pm previous day)
    trades = filter_trades_by_session(all_trades, process_date)
    print(f"Filtered to {len(trades)} trades from last trading session (since 5pm previous day)")

    # Calculate fees
    options_summary, futures_summary = calculate_fees(
        trades, options_config, futures_config
    )

    # Build fee message
    message, summary_data = build_fee_message(
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
    print(f"Total Fees: {summary_data['total_fees_formatted']}")
    print("-------------------\n")

    if args.dry_run:
        print("Dry run - Teams message:")
        print(message)
        return

    # Send Teams webhook
    webhook_config = config.get("webhook", {})
    webhook_url = webhook_config.get("url")

    if not webhook_url:
        print("Error: No webhook URL configured")
        sys.exit(1)

    success = send_teams_message(webhook_url, message)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
