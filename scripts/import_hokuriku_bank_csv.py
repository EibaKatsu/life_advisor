#!/usr/bin/env python3
"""北陸銀行CSVを標準フォーマットに変換して取り込む。"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import re
import sys
from io import StringIO
from pathlib import Path

CANDIDATE_ENCODINGS = ("cp932", "shift_jis", "utf-8-sig", "utf-8")
CANDIDATE_DELIMITERS = (",", "\t", ";")

HEADER_ALIASES = {
    "date_raw": ["取扱日付", "取引日", "日付"],
    "debit_raw": ["お支払金額", "支払金額", "出金額"],
    "credit_raw": ["お預り金額", "預り金額", "入金額"],
    "transaction_type": ["取引区分", "取引内容"],
    "balance_raw": ["残高"],
    "merchant": ["摘要", "取引摘要", "内容"],
    "memo": ["メモ", "備考"],
    "base_date": ["起算日"],
}

REQUIRED_COLUMNS = ("date_raw", "debit_raw", "credit_raw", "merchant")
OUTPUT_COLUMNS = [
    "transaction_id",
    "date",
    "date_raw",
    "merchant",
    "amount_jpy",
    "cardholder",
    "category",
    "memo",
    "payment_method",
    "transaction_type",
    "debit_jpy",
    "credit_jpy",
    "balance_jpy",
    "source_file",
    "source_row",
    "source_encoding",
    "imported_at",
]


def normalize_header(value: str) -> str:
    return re.sub(r"[ \t\u3000]", "", value).strip().lower()


def normalize_cell(value: str) -> str:
    return value.strip().replace("\u3000", " ")


def decode_text(file_path: Path) -> tuple[str, str]:
    raw = file_path.read_bytes()
    for encoding in CANDIDATE_ENCODINGS:
        try:
            return raw.decode(encoding), encoding
        except UnicodeDecodeError:
            continue
    raise ValueError(f"文字コードを判定できませんでした: {file_path}")


def detect_structure(text: str) -> tuple[str, int, list[list[str]]]:
    alias_lookup = {
        key: {normalize_header(alias) for alias in aliases}
        for key, aliases in HEADER_ALIASES.items()
    }
    best_score = -1
    best_delimiter = ","
    best_header_index = -1
    best_rows: list[list[str]] = []

    for delimiter in CANDIDATE_DELIMITERS:
        rows = list(csv.reader(StringIO(text), delimiter=delimiter))
        for idx, row in enumerate(rows[:40]):
            headers = [normalize_header(cell) for cell in row]
            if not headers:
                continue
            score = 0
            for key, aliases in alias_lookup.items():
                if any(cell in aliases for cell in headers):
                    score += 3 if key in REQUIRED_COLUMNS else 1
            if score > best_score:
                best_score = score
                best_delimiter = delimiter
                best_header_index = idx
                best_rows = rows

    if best_score < 9 or best_header_index < 0:
        raise ValueError("ヘッダー行を検出できませんでした。CSVフォーマットを確認してください。")

    return best_delimiter, best_header_index, best_rows


def build_column_map(header: list[str]) -> dict[str, int]:
    normalized_header = [normalize_header(col) for col in header]
    column_map: dict[str, int] = {}

    for key, aliases in HEADER_ALIASES.items():
        alias_norm = [normalize_header(alias) for alias in aliases]

        for idx, col in enumerate(normalized_header):
            if col in alias_norm:
                column_map[key] = idx
                break
        if key in column_map:
            continue
        for idx, col in enumerate(normalized_header):
            if any(alias in col for alias in alias_norm):
                column_map[key] = idx
                break

    missing = [col for col in REQUIRED_COLUMNS if col not in column_map]
    if missing:
        raise ValueError(f"必須列が見つかりません: missing={missing}")

    return column_map


def parse_date(raw_value: str) -> str:
    value = normalize_cell(raw_value)
    if not value:
        return ""

    patterns = ("%Y/%m/%d", "%Y-%m-%d", "%Y年%m月%d日", "%Y%m%d")
    for fmt in patterns:
        try:
            return dt.datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    return ""


def parse_amount(raw_value: str) -> int | None:
    value = normalize_cell(raw_value)
    if not value:
        return None

    sign = 1
    if value.startswith("(") and value.endswith(")"):
        sign = -1
        value = value[1:-1]
    if value.startswith(("▲", "△", "-")):
        sign = -1
        value = value[1:]
    if value.startswith("+"):
        value = value[1:]

    value = value.replace("\\", "").replace("¥", "")
    value = value.replace(",", "").replace("円", "").strip()
    if not value:
        return None
    if not re.match(r"^\d+(\.\d+)?$", value):
        return None
    return sign * int(round(float(value)))


def cell_by_key(row: list[str], column_map: dict[str, int], key: str) -> str:
    idx = column_map.get(key)
    if idx is None or idx >= len(row):
        return ""
    return normalize_cell(row[idx])


def make_transaction_id(source_file: str, source_row: int, row: list[str]) -> str:
    payload = "\x1f".join([source_file, str(source_row), *row])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def discover_input_files(
    input_dir: Path,
    input_files: list[Path] | None,
    output_path: Path,
) -> list[Path]:
    if input_files:
        files = [path for path in input_files if path.exists() and path.is_file()]
    else:
        files = sorted(
            [path for path in input_dir.glob("*") if path.is_file() and path.suffix.lower() == ".csv"]
        )
    resolved_output = output_path.resolve()
    return [path for path in files if path.resolve() != resolved_output]


def infer_payment_method(transaction_type: str) -> str:
    t = transaction_type.strip()
    if t == "出金":
        return "口座引落/出金"
    if t:
        return t
    return ""


def transform_file(
    file_path: Path,
    imported_at: str,
) -> tuple[list[dict[str, str]], int, str, str]:
    text, encoding = decode_text(file_path)
    delimiter, header_index, rows = detect_structure(text)
    column_map = build_column_map(rows[header_index])

    records: list[dict[str, str]] = []
    skipped = 0

    for row_index, row in enumerate(rows[header_index + 1 :], start=header_index + 2):
        if not any(cell.strip() for cell in row):
            continue

        date_raw = cell_by_key(row, column_map, "date_raw")
        merchant = cell_by_key(row, column_map, "merchant")
        debit_raw = cell_by_key(row, column_map, "debit_raw")
        credit_raw = cell_by_key(row, column_map, "credit_raw")
        transaction_type = cell_by_key(row, column_map, "transaction_type")
        memo = cell_by_key(row, column_map, "memo")

        if not date_raw and not merchant and not debit_raw and not credit_raw:
            continue

        date_iso = parse_date(date_raw)
        debit = parse_amount(debit_raw) or 0
        credit = parse_amount(credit_raw) or 0

        if debit == 0 and credit == 0:
            skipped += 1
            continue

        # 支出を正、入金を負に統一して扱う。
        amount_jpy = debit - credit
        balance = parse_amount(cell_by_key(row, column_map, "balance_raw"))

        record = {
            "transaction_id": make_transaction_id(file_path.name, row_index, row),
            "date": date_iso,
            "date_raw": date_raw,
            "merchant": merchant,
            "amount_jpy": str(amount_jpy),
            "cardholder": "",
            "category": "",
            "memo": memo,
            "payment_method": infer_payment_method(transaction_type),
            "transaction_type": transaction_type,
            "debit_jpy": str(debit),
            "credit_jpy": str(credit),
            "balance_jpy": "" if balance is None else str(balance),
            "source_file": file_path.name,
            "source_row": str(row_index),
            "source_encoding": encoding,
            "imported_at": imported_at,
        }
        records.append(record)

    return records, skipped, encoding, delimiter


def write_output(output_path: Path, records: list[dict[str, str]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(records)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="北陸銀行CSVを標準化して取り込み、分析用CSVを出力します。"
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/hokurikuBank"),
        help="北陸銀行CSVを配置するディレクトリ",
    )
    parser.add_argument(
        "--input-file",
        type=Path,
        action="append",
        help="個別に取り込むCSVファイル（複数指定可）",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/hokurikuBank/normalized_transactions.csv"),
        help="標準化後CSVの出力先",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    files = discover_input_files(args.input_dir, args.input_file, args.output)
    if not files:
        print("取り込み対象CSVが見つかりませんでした。", file=sys.stderr)
        return 1

    imported_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
    all_records: list[dict[str, str]] = []
    skipped_rows = 0
    processed_files = 0

    for file_path in files:
        try:
            records, skipped, encoding, delimiter = transform_file(file_path, imported_at)
            all_records.extend(records)
            skipped_rows += skipped
            processed_files += 1
            print(
                f"[OK] {file_path} rows={len(records)} skipped={skipped} "
                f"encoding={encoding} delimiter={repr(delimiter)}"
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[NG] {file_path}: {exc}", file=sys.stderr)

    if processed_files == 0:
        print("有効なCSVを処理できませんでした。", file=sys.stderr)
        return 2

    write_output(args.output, all_records)
    print(
        f"[DONE] files={processed_files} records={len(all_records)} "
        f"skipped={skipped_rows} output={args.output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
