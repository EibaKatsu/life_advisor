#!/usr/bin/env python3
"""楽天カードCSVを標準フォーマットに変換して取り込む。"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import re
import sys
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

CANDIDATE_ENCODINGS = ("utf-8-sig", "cp932", "shift_jis", "utf-8")
CANDIDATE_DELIMITERS = (",", "\t", ";")

HEADER_ALIASES = {
    "date_raw": [
        "利用日",
        "利用日付",
        "利用年月日",
        "ご利用日",
        "取引日",
    ],
    "merchant": [
        "利用店名・商品名",
        "利用店名",
        "ご利用店名・商品名",
        "ご利用先",
        "加盟店名",
        "内容",
    ],
    "amount_raw": [
        "利用金額",
        "ご利用金額",
        "利用金額(円)",
        "利用金額（円）",
        "請求金額",
        "支払総額",
    ],
    "cardholder": [
        "利用者",
        "利用者名",
        "カード利用者",
        "名義",
    ],
    "category": [
        "カテゴリ",
        "業種",
        "分類",
    ],
    "memo": [
        "備考",
        "メモ",
        "摘要",
        "コメント",
    ],
    "payment_method": [
        "支払方法",
        "お支払方法",
        "お支払い方法",
    ],
}

REQUIRED_COLUMNS = ("date_raw", "merchant", "amount_raw")
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
    "source_file",
    "source_row",
    "source_encoding",
    "imported_at",
]


@dataclass
class CsvContext:
    file_path: Path
    encoding: str
    delimiter: str
    header_index: int
    header: list[str]
    rows: list[list[str]]
    column_map: dict[str, int]


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
    best_score = -1
    best_delimiter = ","
    best_header_index = -1
    best_rows: list[list[str]] = []

    alias_lookup = {
        key: {normalize_header(alias) for alias in aliases}
        for key, aliases in HEADER_ALIASES.items()
    }

    for delimiter in CANDIDATE_DELIMITERS:
        rows = list(csv.reader(StringIO(text), delimiter=delimiter))
        for idx, row in enumerate(rows[:40]):
            headers = [normalize_header(cell) for cell in row]
            if not headers:
                continue

            score = 0
            for column_key, aliases in alias_lookup.items():
                if any(cell in aliases for cell in headers):
                    score += 3 if column_key in REQUIRED_COLUMNS else 1
            if score > best_score:
                best_score = score
                best_delimiter = delimiter
                best_header_index = idx
                best_rows = rows

    if best_score < 6 or best_header_index < 0:
        raise ValueError("ヘッダー行を検出できませんでした。CSVフォーマットを確認してください。")

    return best_delimiter, best_header_index, best_rows


def build_column_map(header: list[str]) -> dict[str, int]:
    normalized_header = [normalize_header(col) for col in header]
    column_map: dict[str, int] = {}

    for key, aliases in HEADER_ALIASES.items():
        norm_aliases = [normalize_header(alias) for alias in aliases]

        for idx, col in enumerate(normalized_header):
            if col in norm_aliases:
                column_map[key] = idx
                break
        if key in column_map:
            continue

        for idx, col in enumerate(normalized_header):
            if any(alias in col for alias in norm_aliases):
                column_map[key] = idx
                break

    return column_map


def parse_date(raw_value: str, default_year: int | None) -> str:
    value = normalize_cell(raw_value)
    if not value:
        return ""

    patterns = (
        "%Y/%m/%d",
        "%Y-%m-%d",
        "%Y.%m.%d",
        "%Y年%m月%d日",
        "%Y%m%d",
        "%y/%m/%d",
    )
    for fmt in patterns:
        try:
            parsed = dt.datetime.strptime(value, fmt).date()
            return parsed.isoformat()
        except ValueError:
            continue

    m = re.match(r"^(\d{1,2})[/-](\d{1,2})$", value)
    if m and default_year is not None:
        month, day = int(m.group(1)), int(m.group(2))
        return dt.date(default_year, month, day).isoformat()

    m = re.match(r"^(\d{1,2})月(\d{1,2})日$", value)
    if m and default_year is not None:
        month, day = int(m.group(1)), int(m.group(2))
        return dt.date(default_year, month, day).isoformat()

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

    value = value.replace(",", "").replace("円", "")
    value = value.strip()
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


def load_csv_context(file_path: Path) -> CsvContext:
    text, encoding = decode_text(file_path)
    delimiter, header_index, rows = detect_structure(text)
    header = rows[header_index]
    column_map = build_column_map(header)

    missing = [col for col in REQUIRED_COLUMNS if col not in column_map]
    if missing:
        raise ValueError(f"必須列が見つかりません: {file_path} / missing={missing}")

    return CsvContext(
        file_path=file_path,
        encoding=encoding,
        delimiter=delimiter,
        header_index=header_index,
        header=header,
        rows=rows,
        column_map=column_map,
    )


def discover_input_files(
    input_dir: Path,
    input_files: list[Path] | None,
    output_path: Path,
) -> list[Path]:
    files: list[Path] = []
    if input_files:
        files = [path for path in input_files if path.exists() and path.is_file()]
    else:
        files = sorted(
            [
                path
                for path in input_dir.glob("*")
                if path.is_file() and path.suffix.lower() == ".csv"
            ]
        )

    resolved_output = output_path.resolve()
    return [path for path in files if path.resolve() != resolved_output]


def transform_file(
    ctx: CsvContext,
    default_year: int | None,
    imported_at: str,
) -> tuple[list[dict[str, str]], int]:
    records: list[dict[str, str]] = []
    skipped = 0

    for row_index, row in enumerate(ctx.rows[ctx.header_index + 1 :], start=ctx.header_index + 2):
        if not any(cell.strip() for cell in row):
            continue

        date_raw = cell_by_key(row, ctx.column_map, "date_raw")
        merchant = cell_by_key(row, ctx.column_map, "merchant")
        amount_raw = cell_by_key(row, ctx.column_map, "amount_raw")

        if not date_raw and not merchant and not amount_raw:
            continue
        if merchant.startswith("合計"):
            continue

        date_iso = parse_date(date_raw, default_year=default_year)
        amount = parse_amount(amount_raw)
        if amount is None:
            skipped += 1
            continue

        record = {
            "transaction_id": make_transaction_id(ctx.file_path.name, row_index, row),
            "date": date_iso,
            "date_raw": date_raw,
            "merchant": merchant,
            "amount_jpy": str(amount),
            "cardholder": cell_by_key(row, ctx.column_map, "cardholder"),
            "category": cell_by_key(row, ctx.column_map, "category"),
            "memo": cell_by_key(row, ctx.column_map, "memo"),
            "payment_method": cell_by_key(row, ctx.column_map, "payment_method"),
            "source_file": ctx.file_path.name,
            "source_row": str(row_index),
            "source_encoding": ctx.encoding,
            "imported_at": imported_at,
        }
        records.append(record)

    return records, skipped


def write_output(output_path: Path, records: list[dict[str, str]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(records)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="楽天カードCSVを標準化して取り込み、分析用CSVを出力します。"
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/rakutenCard"),
        help="楽天カードCSVを配置するディレクトリ",
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
        default=Path("data/rakutenCard/normalized_transactions.csv"),
        help="標準化後CSVの出力先",
    )
    parser.add_argument(
        "--default-year",
        type=int,
        default=None,
        help="利用日が月日のみの場合に補完する年（例: 2026）",
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
            ctx = load_csv_context(file_path)
            records, skipped = transform_file(ctx, args.default_year, imported_at)
            all_records.extend(records)
            skipped_rows += skipped
            processed_files += 1
            print(
                f"[OK] {file_path} rows={len(records)} skipped={skipped} "
                f"encoding={ctx.encoding} delimiter={repr(ctx.delimiter)}"
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
