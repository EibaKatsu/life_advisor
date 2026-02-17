#!/usr/bin/env python3
"""各明細の標準化CSVを統合し、分析用トランザクションを作成する。"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
from pathlib import Path

SOURCE_CONFIG = [
    {
        "source_name": "rakutenCard",
        "source_type": "credit_card",
        "path": Path("data/rakutenCard/normalized_transactions.csv"),
    },
    {
        "source_name": "bitflyerCard",
        "source_type": "credit_card",
        "path": Path("data/bitflyerCard/normalized_transactions.csv"),
    },
    {
        "source_name": "hokurikuBank",
        "source_type": "bank",
        "path": Path("data/hokurikuBank/normalized_transactions.csv"),
    },
]

OUTPUT_COLUMNS = [
    "source_name",
    "source_type",
    "transaction_id",
    "date",
    "date_raw",
    "merchant",
    "amount_jpy",
    "is_outflow",
    "cardholder",
    "category",
    "memo",
    "payment_method",
    "transaction_type",
    "debit_jpy",
    "credit_jpy",
    "balance_jpy",
    "card_number",
    "sale_type",
    "installments",
    "current_installment",
    "source_file",
    "source_row",
    "source_encoding",
    "imported_at",
    "merged_at",
]


def to_int(value: str) -> int:
    try:
        return int(value.strip())
    except Exception:
        return 0


def get(row: dict[str, str], key: str) -> str:
    return row.get(key, "").strip()


def load_records(source_name: str, source_type: str, path: Path, merged_at: str) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        records = []
        for row in reader:
            amount = to_int(get(row, "amount_jpy"))
            records.append(
                {
                    "source_name": source_name,
                    "source_type": source_type,
                    "transaction_id": get(row, "transaction_id"),
                    "date": get(row, "date"),
                    "date_raw": get(row, "date_raw"),
                    "merchant": get(row, "merchant"),
                    "amount_jpy": str(amount),
                    "is_outflow": "1" if amount > 0 else "0",
                    "cardholder": get(row, "cardholder"),
                    "category": get(row, "category"),
                    "memo": get(row, "memo"),
                    "payment_method": get(row, "payment_method"),
                    "transaction_type": get(row, "transaction_type"),
                    "debit_jpy": get(row, "debit_jpy"),
                    "credit_jpy": get(row, "credit_jpy"),
                    "balance_jpy": get(row, "balance_jpy"),
                    "card_number": get(row, "card_number"),
                    "sale_type": get(row, "sale_type"),
                    "installments": get(row, "installments"),
                    "current_installment": get(row, "current_installment"),
                    "source_file": get(row, "source_file"),
                    "source_row": get(row, "source_row"),
                    "source_encoding": get(row, "source_encoding"),
                    "imported_at": get(row, "imported_at"),
                    "merged_at": merged_at,
                }
            )
    return records


def write_output(output_path: Path, records: list[dict[str, str]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(records)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="標準化CSVを統合し、分析用CSVを作成します。")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/analysis/all_transactions.csv"),
        help="統合CSVの出力先",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    merged_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
    all_records: list[dict[str, str]] = []
    used_sources = 0

    for src in SOURCE_CONFIG:
        path = src["path"]
        if not path.exists():
            print(f"[WARN] missing source: {path}")
            continue
        try:
            records = load_records(src["source_name"], src["source_type"], path, merged_at)
            all_records.extend(records)
            used_sources += 1
            print(f"[OK] {src['source_name']} records={len(records)} path={path}")
        except Exception as exc:  # noqa: BLE001
            print(f"[NG] {src['source_name']} path={path}: {exc}")

    if used_sources == 0:
        print("有効な入力ソースがありませんでした。")
        return 1

    all_records.sort(
        key=lambda r: (
            r["date"] or "0000-00-00",
            r["source_name"],
            r["source_file"],
            to_int(r["source_row"]),
        )
    )

    write_output(args.output, all_records)
    print(f"[DONE] sources={used_sources} records={len(all_records)} output={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
