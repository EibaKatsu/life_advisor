#!/usr/bin/env python3
"""personal配下の明細CSVを統合し、分析用CSVを作成する。"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
from pathlib import Path


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="個人明細CSVを統合して分析用CSVを作成します。")
    parser.add_argument("--root", type=Path, default=Path("data/personal"), help="個人データルート")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/personal/analysis/all_transactions.csv"),
        help="統合CSV出力先",
    )
    return parser.parse_args()


def to_int(value: str) -> int:
    text = (value or "").strip()
    if not text:
        return 0
    sign = -1 if text.startswith("-") else 1
    text = text.lstrip("+-").replace(",", "").replace("¥", "").replace("\\", "").replace("円", "").strip()
    if not text:
        return 0
    try:
        return sign * int(round(float(text)))
    except Exception:
        return 0


def parse_date(raw_value: str) -> str:
    value = (raw_value or "").strip()
    if not value:
        return ""
    fmts = ("%Y/%m/%d", "%Y-%m-%d", "%Y%m%d", "%Y年%m月%d日")
    for fmt in fmts:
        try:
            return dt.datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    return ""


def make_id(*values: str) -> str:
    payload = "\x1f".join(values)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def base_record(
    *,
    source_name: str,
    source_type: str,
    date_raw: str,
    merchant: str,
    amount_jpy: int,
    source_file: str,
    source_row: int,
    source_encoding: str,
    imported_at: str,
    merged_at: str,
) -> dict[str, str]:
    date_iso = parse_date(date_raw)
    return {
        "source_name": source_name,
        "source_type": source_type,
        "transaction_id": make_id(source_name, source_file, str(source_row), date_raw, merchant, str(amount_jpy)),
        "date": date_iso,
        "date_raw": date_raw,
        "merchant": merchant.strip(),
        "amount_jpy": str(amount_jpy),
        "is_outflow": "1" if amount_jpy > 0 else "0",
        "cardholder": "",
        "category": "",
        "memo": "",
        "payment_method": "",
        "transaction_type": "",
        "debit_jpy": str(amount_jpy) if amount_jpy > 0 else "0",
        "credit_jpy": str(-amount_jpy) if amount_jpy < 0 else "0",
        "balance_jpy": "",
        "card_number": "",
        "sale_type": "",
        "installments": "",
        "current_installment": "",
        "source_file": source_file,
        "source_row": str(source_row),
        "source_encoding": source_encoding,
        "imported_at": imported_at,
        "merged_at": merged_at,
    }


def decode_csv(path: Path) -> tuple[list[list[str]], str]:
    raw = path.read_bytes()
    for enc in ("utf-8-sig", "cp932", "shift_jis", "utf-8"):
        try:
            text = raw.decode(enc)
            return list(csv.reader(text.splitlines())), enc
        except UnicodeDecodeError:
            continue
    raise ValueError(f"decode failed: {path}")


def parse_dcard(root: Path, imported_at: str, merged_at: str) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for path in sorted((root / "dCard").glob("*.csv")):
        rows, enc = decode_csv(path)
        header_idx = None
        for i, row in enumerate(rows):
            if row and row[0] == "名前" and "ご利用年月日" in row and "利用店名" in row and "支払い金額" in row:
                header_idx = i
                break
        if header_idx is None:
            continue
        header = rows[header_idx]
        idx_date = header.index("ご利用年月日")
        idx_shop = header.index("利用店名")
        idx_amount = header.index("支払い金額")
        idx_name = header.index("名前")
        idx_card = header.index("カード番号")
        idx_pay = header.index("支払区分")
        idx_memo = header.index("摘要")
        for line_no, row in enumerate(rows[header_idx + 1 :], start=header_idx + 2):
            if len(row) <= idx_amount:
                continue
            raw_date = row[idx_date].strip()
            date_iso = parse_date(raw_date)
            if not date_iso:
                continue
            amount = to_int(row[idx_amount])
            if amount == 0:
                continue
            rec = base_record(
                source_name="dCard",
                source_type="credit_card",
                date_raw=raw_date,
                merchant=row[idx_shop],
                amount_jpy=amount,
                source_file=path.name,
                source_row=line_no,
                source_encoding=enc,
                imported_at=imported_at,
                merged_at=merged_at,
            )
            rec["cardholder"] = row[idx_name].replace("様", "").strip()
            rec["card_number"] = row[idx_card].strip()
            rec["payment_method"] = row[idx_pay].strip()
            rec["memo"] = row[idx_memo].strip()
            out.append(rec)
    return out


def parse_viewcard(root: Path, imported_at: str, merged_at: str) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for path in sorted((root / "viewCard").glob("*.csv")):
        rows, enc = decode_csv(path)
        header_idx = None
        for i, row in enumerate(rows):
            if "ご利用年月日" in row and "ご利用箇所" in row and "今回ご請求額・弁済金（うち手数料・利息）" in row:
                header_idx = i
                break
        if header_idx is None:
            continue
        header = rows[header_idx]
        idx_date = header.index("ご利用年月日")
        idx_shop = header.index("ご利用箇所")
        idx_bill = header.index("今回ご請求額・弁済金（うち手数料・利息）")
        idx_pay = header.index("支払区分（回数）") if "支払区分（回数）" in header else None
        cardholder = ""
        if header_idx + 1 < len(rows) and rows[header_idx + 1]:
            cardholder = rows[header_idx + 1][0].strip()
        for line_no, row in enumerate(rows[header_idx + 1 :], start=header_idx + 2):
            if len(row) <= idx_bill:
                continue
            raw_date = row[idx_date].strip()
            date_iso = parse_date(raw_date)
            if not date_iso:
                continue
            amount = to_int(row[idx_bill])
            if amount == 0:
                continue
            rec = base_record(
                source_name="viewCard",
                source_type="credit_card",
                date_raw=raw_date,
                merchant=row[idx_shop],
                amount_jpy=amount,
                source_file=path.name,
                source_row=line_no,
                source_encoding=enc,
                imported_at=imported_at,
                merged_at=merged_at,
            )
            rec["cardholder"] = cardholder
            if idx_pay is not None and idx_pay < len(row):
                rec["payment_method"] = row[idx_pay].strip()
            out.append(rec)
    return out


def parse_jre_bank(root: Path, imported_at: str, merged_at: str) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    path = root / "jreBank" / "RB-torihikimeisai.csv"
    if not path.exists():
        return out
    rows, enc = decode_csv(path)
    if not rows:
        return out
    header = rows[0]
    if "取引日" not in header or "入出金(円)" not in header:
        return out
    idx_date = header.index("取引日")
    idx_amount = header.index("入出金(円)")
    idx_balance = header.index("取引後残高(円)") if "取引後残高(円)" in header else None
    idx_desc = header.index("入出金内容") if "入出金内容" in header else None
    for line_no, row in enumerate(rows[1:], start=2):
        if len(row) <= idx_amount:
            continue
        raw_date = row[idx_date].strip()
        date_iso = parse_date(raw_date)
        if not date_iso:
            continue
        signed = to_int(row[idx_amount])
        if signed == 0:
            continue
        # jreBankは入金が正、出金が負。分析は支出正・入金負へ揃える。
        amount = -signed
        rec = base_record(
            source_name="jreBank",
            source_type="bank",
            date_raw=raw_date,
            merchant=row[idx_desc] if idx_desc is not None and idx_desc < len(row) else "",
            amount_jpy=amount,
            source_file=path.name,
            source_row=line_no,
            source_encoding=enc,
            imported_at=imported_at,
            merged_at=merged_at,
        )
        rec["transaction_type"] = "出金" if amount > 0 else "入金"
        rec["payment_method"] = "口座取引"
        rec["debit_jpy"] = str(amount if amount > 0 else 0)
        rec["credit_jpy"] = str(-amount if amount < 0 else 0)
        if idx_balance is not None and idx_balance < len(row):
            rec["balance_jpy"] = str(to_int(row[idx_balance]))
        out.append(rec)
    return out


def parse_shinsei_bank(root: Path, imported_at: str, merged_at: str) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for path in sorted((root / "shinseiBank").glob("*.csv")):
        rows, enc = decode_csv(path)
        if not rows:
            continue
        header = rows[0]
        if "取引日" not in header or "出金金額" not in header or "入金金額" not in header:
            continue
        idx_date = header.index("取引日")
        idx_desc = header.index("摘要") if "摘要" in header else None
        idx_debit = header.index("出金金額")
        idx_credit = header.index("入金金額")
        idx_balance = header.index("残高") if "残高" in header else None
        idx_memo = header.index("メモ") if "メモ" in header else None
        for line_no, row in enumerate(rows[1:], start=2):
            if len(row) <= max(idx_debit, idx_credit):
                continue
            raw_date = row[idx_date].strip()
            date_iso = parse_date(raw_date)
            if not date_iso:
                continue
            debit = to_int(row[idx_debit])
            credit = to_int(row[idx_credit])
            if debit == 0 and credit == 0:
                continue
            amount = debit - credit
            rec = base_record(
                source_name="shinseiBank",
                source_type="bank",
                date_raw=raw_date,
                merchant=row[idx_desc] if idx_desc is not None and idx_desc < len(row) else "",
                amount_jpy=amount,
                source_file=path.name,
                source_row=line_no,
                source_encoding=enc,
                imported_at=imported_at,
                merged_at=merged_at,
            )
            rec["transaction_type"] = "出金" if debit > 0 else "入金"
            rec["payment_method"] = "口座取引"
            rec["debit_jpy"] = str(debit)
            rec["credit_jpy"] = str(credit)
            if idx_balance is not None and idx_balance < len(row):
                rec["balance_jpy"] = str(to_int(row[idx_balance]))
            if idx_memo is not None and idx_memo < len(row):
                rec["memo"] = row[idx_memo].strip()
            out.append(rec)
    return out


def write_output(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    merged_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
    imported_at = merged_at

    records: list[dict[str, str]] = []
    records.extend(parse_dcard(args.root, imported_at, merged_at))
    records.extend(parse_viewcard(args.root, imported_at, merged_at))
    records.extend(parse_jre_bank(args.root, imported_at, merged_at))
    records.extend(parse_shinsei_bank(args.root, imported_at, merged_at))

    records.sort(
        key=lambda r: (
            r["date"] or "0000-00-00",
            r["source_name"],
            r["source_file"],
            int(r["source_row"] or "0"),
        )
    )
    write_output(args.output, records)
    print(f"[DONE] records={len(records)} output={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
