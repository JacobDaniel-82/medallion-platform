#!/usr/bin/env python3
"""Generate deterministic synthetic wealth-management ORC data and upload it to MinIO.

Bronze layout:
    <source>/<table>/organisation_id=<org>/processing_date=<date>/data.orc

organisation_id and processing_date are intentionally NOT stored as ORC columns.
They are represented by the object-store partition path.
"""

from __future__ import annotations

import argparse
import random
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from io import BytesIO
from typing import Callable, Dict, Iterable, List, Sequence
import os
import tzdata

os.environ.setdefault("TZDIR", os.path.join(os.path.dirname(tzdata.__file__), "zoneinfo"))
import boto3
import pyarrow as pa
import pyarrow.orc as orc
from botocore.exceptions import ClientError
from faker import Faker


SEED = 20261007
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
BUCKET = "bronze"

ORGANISATIONS = ["org_8812", "org_4421", "org_7305"]
PROCESSING_DATES = [date(2026, 8, 19) + timedelta(days=i) for i in range(7)]

SOURCE_MAPPING = {
    "advisors": "crm",
    "accounts": "crm",
    "contacts": "crm",
    "portfolios": "custodian",
    "holdings": "custodian",
    "trades": "custodian",
    "transactions": "custodian",
    "statements": "custodian",
    "securities": "market_data",
    "fee_schedules": "billing",
}

TABLE_ORDER = [
    "advisors",
    "accounts",
    "contacts",
    "securities",
    "portfolios",
    "holdings",
    "trades",
    "transactions",
    "fee_schedules",
    "statements",
]

# Exact logical ORC schemas from docs/schema.md, excluding partition columns.
SCHEMAS: Dict[str, pa.Schema] = {
    "advisors": pa.schema([
        ("advisor_id", pa.string()),
        ("first_name", pa.string()),
        ("last_name", pa.string()),
        ("email", pa.string()),
        ("phone", pa.string()),
        ("status", pa.string()),
        ("updated_at", pa.timestamp("us")),
    ]),
    "accounts": pa.schema([
        ("account_id", pa.string()),
        ("advisor_id", pa.string()),
        ("account_name", pa.string()),
        ("account_type", pa.string()),
        ("status", pa.string()),
        ("opened_date", pa.date32()),
        ("currency", pa.string()),
        ("updated_at", pa.timestamp("us")),
    ]),
    "contacts": pa.schema([
        ("contact_id", pa.string()),
        ("account_id", pa.string()),
        ("first_name", pa.string()),
        ("last_name", pa.string()),
        ("email", pa.string()),
        ("phone", pa.string()),
        ("contact_type", pa.string()),
        ("updated_at", pa.timestamp("us")),
    ]),
    "securities": pa.schema([
        ("security_id", pa.string()),
        ("ticker", pa.string()),
        ("security_name", pa.string()),
        ("security_type", pa.string()),
        ("currency", pa.string()),
        ("exchange", pa.string()),
        ("updated_at", pa.timestamp("us")),
    ]),
    "portfolios": pa.schema([
        ("portfolio_id", pa.string()),
        ("account_id", pa.string()),
        ("portfolio_name", pa.string()),
        ("portfolio_type", pa.string()),
        ("base_currency", pa.string()),
        ("status", pa.string()),
        ("updated_at", pa.timestamp("us")),
    ]),
    "holdings": pa.schema([
        ("holding_id", pa.string()),
        ("portfolio_id", pa.string()),
        ("security_id", pa.string()),
        ("quantity", pa.decimal128(18, 2)),
        ("average_cost", pa.decimal128(18, 2)),
        ("market_value", pa.decimal128(18, 2)),
        ("as_of_date", pa.date32()),
        ("updated_at", pa.timestamp("us")),
    ]),
    "trades": pa.schema([
        ("trade_id", pa.string()),
        ("portfolio_id", pa.string()),
        ("security_id", pa.string()),
        ("trade_date", pa.date32()),
        ("trade_type", pa.string()),
        ("quantity", pa.decimal128(18, 2)),
        ("price", pa.decimal128(18, 2)),
        ("total_amount", pa.decimal128(18, 2)),
        ("updated_at", pa.timestamp("us")),
    ]),
    "transactions": pa.schema([
        ("transaction_id", pa.string()),
        ("account_id", pa.string()),
        ("transaction_date", pa.date32()),
        ("transaction_type", pa.string()),
        ("amount", pa.decimal128(18, 2)),
        ("currency", pa.string()),
        ("description", pa.string()),
        ("updated_at", pa.timestamp("us")),
    ]),
    "fee_schedules": pa.schema([
        ("fee_schedule_id", pa.string()),
        ("account_id", pa.string()),
        ("fee_type", pa.string()),
        ("fee_rate", pa.decimal128(9, 4)),
        ("effective_date", pa.date32()),
        ("status", pa.string()),
        ("updated_at", pa.timestamp("us")),
    ]),
    "statements": pa.schema([
        ("statement_id", pa.string()),
        ("account_id", pa.string()),
        ("statement_date", pa.date32()),
        ("period_start", pa.date32()),
        ("period_end", pa.date32()),
        ("opening_balance", pa.decimal128(18, 2)),
        ("closing_balance", pa.decimal128(18, 2)),
        ("currency", pa.string()),
        ("updated_at", pa.timestamp("us")),
    ]),
}

DUPLICATE_SCHEDULE = {
    ("accounts", date(2026, 8, 21)),
    ("accounts", date(2026, 8, 24)),
    ("holdings", date(2026, 8, 20)),
    ("holdings", date(2026, 8, 23)),
    ("trades", date(2026, 8, 22)),
    ("statements", date(2026, 8, 25)),
}


class DataGenerator:
    def __init__(self, seed: int = SEED) -> None:
        self.seed = seed
        self.rng = random.Random(seed)
        self.fake = Faker("en_US")
        self.fake.seed_instance(seed)

        self.base: Dict[str, Dict[str, List[dict]]] = defaultdict(lambda: defaultdict(list))
        self._build_base_data()

    @staticmethod
    def _decimal(value: float | int, scale: int = 2) -> Decimal:
        return Decimal(str(round(value, scale))).quantize(Decimal("1." + "0" * scale))

    def _updated_at(self, processing_date: date, entity_index: int, version: int = 0) -> datetime:
        # Stable but varied timestamps. Version > 0 is always newer than version 0.
        minute = (entity_index * 7 + self.seed) % (24 * 60 - 10)
        base = datetime.combine(processing_date, time.min) + timedelta(minutes=minute)
        return base + timedelta(minutes=version + 1)

    def _name(self, organisation_id: str, index: int) -> tuple[str, str]:
        first = ["Aarav", "Maya", "Daniel", "Sophia", "Ethan", "Olivia", "Noah", "Emma"][index % 8]
        last = ["Patel", "Shah", "Wilson", "Carter", "Morgan", "Bennett", "Taylor", "Brooks"][
            (index + len(organisation_id)) % 8
        ]
        return first, last

    def _build_base_data(self) -> None:
        for org in ORGANISATIONS:
            self._build_advisors(org)
            self._build_securities(org)
            self._build_accounts(org)
            self._build_contacts(org)
            self._build_portfolios(org)
            self._build_holdings(org)
            self._build_trades(org)
            self._build_transactions(org)
            self._build_fee_schedules(org)
            self._build_statements(org)

    def _build_advisors(self, org: str) -> None:
        for i in range(1, 6):
            first, last = self._name(org, i)
            self.base[org]["advisors"].append({
                "advisor_id": f"ADV-{i:03d}",
                "first_name": first,
                "last_name": last,
                "email": f"{first.lower()}.{last.lower()}@example.com",
                "phone": self.fake.phone_number(),
                "status": "Active" if i < 5 else "Inactive",
            })

    def _build_securities(self, org: str) -> None:
        names = [
            ("AAPL", "Apple Inc.", "Stock", "NASDAQ"),
            ("MSFT", "Microsoft Corp.", "Stock", "NASDAQ"),
            ("AMZN", "Amazon.com Inc.", "Stock", "NASDAQ"),
            ("NVDA", "NVIDIA Corp.", "Stock", "NASDAQ"),
            ("GOOGL", "Alphabet Inc.", "Stock", "NASDAQ"),
            ("JPM", "JPMorgan Chase & Co.", "Stock", "NYSE"),
            ("V", "Visa Inc.", "Stock", "NYSE"),
            ("JNJ", "Johnson & Johnson", "Stock", "NYSE"),
            ("SPY", "SPDR S&P 500 ETF", "ETF", "NYSEARCA"),
            ("QQQ", "Invesco QQQ ETF", "ETF", "NASDAQ"),
            ("BND", "Vanguard Total Bond Market ETF", "ETF", "NASDAQ"),
            ("AGG", "iShares Core U.S. Aggregate Bond ETF", "ETF", "NYSEARCA"),
            ("TLT", "iShares 20+ Year Treasury Bond ETF", "ETF", "NASDAQ"),
            ("US10Y", "U.S. Treasury Note", "Bond", "OTC"),
            ("CORP01", "Global Corporate Bond", "Bond", "OTC"),
        ]
        for i, (ticker, name, security_type, exchange) in enumerate(names, 1):
            self.base[org]["securities"].append({
                "security_id": f"SEC-{i:04d}",
                "ticker": ticker,
                "security_name": name,
                "security_type": security_type,
                "currency": "USD",
                "exchange": exchange,
            })

    def _build_accounts(self, org: str) -> None:
        advisor_count = len(self.base[org]["advisors"])
        for i in range(1, 13):
            account_id = f"ACC-{1000 + i:04d}"
            advisor_id = f"ADV-{((i - 1) % advisor_count) + 1:03d}"
            first, _ = self._name(org, i)
            self.base[org]["accounts"].append({
                "account_id": account_id,
                "advisor_id": advisor_id,
                "account_name": f"{first} {['Growth', 'Balanced', 'Income', 'Wealth'][i % 4]} Account",
                "account_type": ["Brokerage", "IRA", "Trust", "Retirement"][i % 4],
                "status": "Active",
                "opened_date": date(2024, 1, 15) + timedelta(days=i * 17),
                "currency": "USD",
            })

        # Deliberate cross-tenant collision with different values.
        if org == "org_8812":
            self.base[org]["accounts"][0].update({
                "account_id": "ACC-1001",
                "account_name": "Northstar Growth Account",
                "advisor_id": "ADV-001",
            })
        elif org == "org_4421":
            self.base[org]["accounts"][0].update({
                "account_id": "ACC-1001",
                "account_name": "Meridian Wealth Account",
                "advisor_id": "ADV-002",
            })

    def _build_contacts(self, org: str) -> None:
        accounts = self.base[org]["accounts"]
        for i in range(1, 19):
            account = accounts[(i - 1) % len(accounts)]
            first, last = self._name(org, i + 10)
            self.base[org]["contacts"].append({
                "contact_id": f"CON-{i:04d}",
                "account_id": account["account_id"],
                "first_name": first,
                "last_name": last,
                "email": f"{first.lower()}.{last.lower()}{i}@example.com",
                "phone": self.fake.phone_number(),
                "contact_type": "Primary Client" if i % 3 else "Beneficiary",
            })

    def _build_portfolios(self, org: str) -> None:
        accounts = self.base[org]["accounts"]
        for i in range(1, 11):
            account = accounts[(i - 1) % len(accounts)]
            self.base[org]["portfolios"].append({
                "portfolio_id": f"PORT-{i:04d}",
                "account_id": account["account_id"],
                "portfolio_name": f"{account['account_name']} Portfolio",
                "portfolio_type": ["Growth", "Balanced", "Income", "Conservative"][i % 4],
                "base_currency": "USD",
                "status": "Active",
            })

    def _build_holdings(self, org: str) -> None:
        portfolios = self.base[org]["portfolios"]
        securities = self.base[org]["securities"]
        for i in range(1, 31):
            portfolio = portfolios[(i - 1) % len(portfolios)]
            security = securities[(i * 3 - 1) % len(securities)]
            quantity = self._decimal(50 + (i * 13) % 950)
            avg_cost = self._decimal(50 + (i * 17) % 350)
            market_value = self._decimal(float(quantity * avg_cost) * (0.94 + (i % 9) / 100))
            self.base[org]["holdings"].append({
                "holding_id": f"HLD-{i:05d}",
                "portfolio_id": portfolio["portfolio_id"],
                "security_id": security["security_id"],
                "quantity": quantity,
                "average_cost": avg_cost,
                "market_value": market_value,
                "as_of_date": date(2026, 8, 19),
            })

    def _build_trades(self, org: str) -> None:
        portfolios = self.base[org]["portfolios"]
        securities = self.base[org]["securities"]
        for i in range(1, 21):
            portfolio = portfolios[(i + 2) % len(portfolios)]
            security = securities[(i * 2) % len(securities)]
            quantity = self._decimal(5 + (i * 7) % 150)
            price = self._decimal(25 + (i * 19) % 450)
            self.base[org]["trades"].append({
                "trade_id": f"TRD-{i:05d}",
                "portfolio_id": portfolio["portfolio_id"],
                "security_id": security["security_id"],
                "trade_date": date(2026, 8, 19) - timedelta(days=i % 30),
                "trade_type": "Buy" if i % 2 else "Sell",
                "quantity": quantity,
                "price": price,
                "total_amount": self._decimal(float(quantity * price)),
            })

    def _build_transactions(self, org: str) -> None:
        accounts = self.base[org]["accounts"]
        transaction_types = ["Deposit", "Withdrawal", "Dividend", "Fee"]
        for i in range(1, 26):
            account = accounts[(i - 1) % len(accounts)]
            tx_type = transaction_types[i % len(transaction_types)]
            amount = self._decimal(100 + (i * 137) % 10000)
            self.base[org]["transactions"].append({
                "transaction_id": f"TXN-{i:05d}",
                "account_id": account["account_id"],
                "transaction_date": date(2026, 8, 19) - timedelta(days=i % 60),
                "transaction_type": tx_type,
                "amount": amount,
                "currency": "USD",
                "description": f"{tx_type} transaction for {account['account_id']}",
            })

    def _build_fee_schedules(self, org: str) -> None:
        accounts = self.base[org]["accounts"]
        for i in range(1, 13):
            account = accounts[(i - 1) % len(accounts)]
            self.base[org]["fee_schedules"].append({
                "fee_schedule_id": f"FEE-{i:04d}",
                "account_id": account["account_id"],
                "fee_type": "Management" if i % 3 else "Performance",
                "fee_rate": self._decimal(0.50 + (i % 6) * 0.10, 4),
                "effective_date": date(2026, 1, 1) + timedelta(days=i * 5),
                "status": "Active",
            })

    def _build_statements(self, org: str) -> None:
        accounts = self.base[org]["accounts"]
        for i in range(1, 13):
            account = accounts[(i - 1) % len(accounts)]
            opening = self._decimal(10000 + i * 1575.50)
            closing = self._decimal(float(opening) + ((-1) ** i) * (i * 175.25))
            self.base[org]["statements"].append({
                "statement_id": f"STM-{i:04d}",
                "account_id": account["account_id"],
                "statement_date": date(2026, 8, 19),
                "period_start": date(2026, 8, 1),
                "period_end": date(2026, 8, 19),
                "opening_balance": opening,
                "closing_balance": closing,
                "currency": "USD",
            })

    def _apply_date_changes(self, org: str, table: str, processing_date: date, rows: List[dict]) -> List[dict]:
        """Return that day's rows with deterministic changes and updated_at values."""
        day_index = (processing_date - PROCESSING_DATES[0]).days
        output: List[dict] = []

        for idx, base_row in enumerate(rows):
            row = dict(base_row)
            row["updated_at"] = self._updated_at(processing_date, idx)

            # Some records change across processing dates, giving Silver history to manage.
            if day_index > 0 and idx % 7 == day_index % 7:
                if table in {"advisors", "accounts", "portfolios", "transactions", "fee_schedules", "statements"}:
                    if "status" in row:
                        row["status"] = "Inactive" if day_index % 2 == 0 else "Active"
                elif table == "holdings":
                    row["market_value"] = self._decimal(float(row["market_value"]) * (1 + day_index * 0.004))
                    row["as_of_date"] = processing_date
                elif table == "trades":
                    row["price"] = self._decimal(float(row["price"]) * (1 + day_index * 0.003))
                    row["total_amount"] = self._decimal(float(row["quantity"] * row["price"]))
                elif table == "securities":
                    row["exchange"] = "NYSE" if row["exchange"] == "NASDAQ" else row["exchange"]
                elif table == "contacts":
                    row["phone"] = self.fake.phone_number()

                row["updated_at"] = self._updated_at(processing_date, idx, version=2)

            # Statements represent the processing date's reporting period.
            if table == "statements":
                row["statement_date"] = processing_date
                row["period_end"] = processing_date
                row["period_start"] = processing_date.replace(day=1)

            output.append(row)

        # Add an intentional same-file duplicate on selected dates. The later row has
        # the same business ID but a newer updated_at and a changed value.
        if (table, processing_date) in DUPLICATE_SCHEDULE and output:
            duplicate_index = (day_index * 3) % len(output)
            duplicate = dict(output[duplicate_index])
            duplicate["updated_at"] = duplicate["updated_at"] + timedelta(minutes=30)

            if table == "accounts":
                duplicate["account_name"] = duplicate["account_name"] + " - Updated"
            elif table == "holdings":
                duplicate["market_value"] = self._decimal(float(duplicate["market_value"]) * 1.015)
            elif table == "trades":
                duplicate["price"] = self._decimal(float(duplicate["price"]) * 1.01)
                duplicate["total_amount"] = self._decimal(float(duplicate["quantity"] * duplicate["price"]))
            elif table == "statements":
                duplicate["closing_balance"] = self._decimal(float(duplicate["closing_balance"]) + 250)

            output.append(duplicate)

        return output

    def rows_for(self, org: str, table: str, processing_date: date) -> List[dict]:
        return self._apply_date_changes(org, table, processing_date, self.base[org][table])



def ensure_bucket(s3_client) -> None:
    try:
        s3_client.head_bucket(Bucket=BUCKET)
    except ClientError as exc:
        error_code = str(exc.response.get("Error", {}).get("Code", ""))
        if error_code not in {"404", "NoSuchBucket", "NotFound"}:
            # MinIO may return 400/403 for some endpoint configurations; try creation
            # only when the bucket genuinely does not exist.
            try:
                s3_client.create_bucket(Bucket=BUCKET)
            except ClientError:
                raise
        else:
            s3_client.create_bucket(Bucket=BUCKET)


def upload_orc(s3_client, key: str, rows: Sequence[dict], schema: pa.Schema) -> None:
    table = pa.Table.from_pylist(list(rows), schema=schema)
    buffer = BytesIO()
    with orc.ORCWriter(buffer) as writer:
        writer.write(table)
    buffer.seek(0)
    s3_client.upload_fileobj(buffer, BUCKET, key)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic wealth-management ORC data and upload to MinIO.")
    parser.add_argument(
        "--tables",
        nargs="+",
        choices=TABLE_ORDER,
        help="Only generate the specified tables. Defaults to all tables.",
    )
    parser.add_argument("--seed", type=int, default=SEED, help=f"Random seed (default: {SEED}).")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    tables = args.tables or TABLE_ORDER

    # Validate table order so parent tables are generated/uploaded before children.
    tables = [table for table in TABLE_ORDER if table in tables]

    generator = DataGenerator(seed=args.seed)

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )

    ensure_bucket(s3)

    summary = {table: {"rows": 0, "files": 0} for table in tables}

    print(f"MinIO: {MINIO_ENDPOINT}")
    print(f"Bucket: {BUCKET}")
    print(f"Seed: {args.seed}")
    print(f"Tables: {', '.join(tables)}")
    print()

    for processing_date in PROCESSING_DATES:
        for org in ORGANISATIONS:
            for table in tables:
                rows = generator.rows_for(org, table, processing_date)
                source = SOURCE_MAPPING[table]
                key = (
                    f"{source}/{table}/"
                    f"organisation_id={org}/"
                    f"processing_date={processing_date.isoformat()}/"
                    "data.orc"
                )

                upload_orc(s3, key, rows, SCHEMAS[table])
                summary[table]["rows"] += len(rows)
                summary[table]["files"] += 1

    print("Upload summary")
    print("==============")
    print(f"{'Table':<18} {'Rows':>8} {'Files':>8}")
    print(f"{'-' * 18} {'-' * 8} {'-' * 8}")
    for table in tables:
        print(f"{table:<18} {summary[table]['rows']:>8} {summary[table]['files']:>8}")

    print()
    print(f"Total rows:  {sum(v['rows'] for v in summary.values())}")
    print(f"Total files: {sum(v['files'] for v in summary.values())}")


if __name__ == "__main__":
    main()
