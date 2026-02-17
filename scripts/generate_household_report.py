#!/usr/bin/env python3
"""家計レポートをMarkdownで生成する。"""

from __future__ import annotations

import argparse
import csv
import html
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path


@dataclass
class SpendRecord:
    day: str
    source: str
    merchant: str
    amount: int


DRIVER_RULES: list[tuple[str, list[str]]] = [
    (
        "住居・不動産関連",
        [
            r"ニツシンカンザイ",
            r"エイブル",
            r"タウンハウジング",
            r"ホクリクデンリヨク",
            r"ニホンカイガス",
            r"ケ-ブルテレビ",
            r"ケーブルテレビ",
        ],
    ),
    (
        "食費・日用品",
        [
            r"アルビス",
            r"バロ",
            r"クスリノアオキ",
            r"オオサカヤ",
            r"セブンイレブン",
            r"ローソン",
            r"コメリ",
            r"カインズ",
            r"トヤマセイキヨウ",
        ],
    ),
    ("交通・車", [r"ENEOS", r"ETC", r"オ-トガレ-ジ", r"ガレ-ジ", r"オオウチセキユ", r"ジドウキ"]),
    ("保険", [r"SMBC\(プルデン", r"アイオイ"]),
    ("教育", [r"SMBC\(シガダイガク", r"ダイガク", r"学校"]),
    ("通信・サブスク", [r"NETFLIX", r"ネットフリックス", r"FAMILY CLUB", r"メルスプラン"]),
    ("EC・大型買物", [r"AMAZON", r"AMZ", r"ニトリ", r"サンキュ", r"ス-ツセレクト", r"LENOVO"]),
    ("税金・公金", [r"コテイシサン", r"ペイジエント", r"テスウリヨウ"]),
]

SPECIAL_RULES: list[tuple[str, list[str]]] = [
    ("住宅・不動産関連", [r"ニツシンカンザイ", r"エイブル", r"タウンハウジング"]),
    ("保険", [r"SMBC\(プルデン", r"アイオイ"]),
    ("教育", [r"SMBC\(シガダイガク", r"ダイガク"]),
    ("大型購入", [r"AMZ", r"LENOVO", r"ニトリ", r"サンキュ", r"ス-ツセレクト", r"AMAZON"]),
    ("税金・公金", [r"コテイシサン", r"ペイジエント"]),
    ("自動車関連", [r"ジドウキ", r"ガレ-ジ", r"オオウチセキユ"]),
    ("要確認(摘要空欄)", [r"^$"]),
]

BANK_EXCLUDE_PATTERNS = [r"APアプラス", r"ラクテンカ-ド", r"オリコ", r"エイバヤシ カヨコ", r"エイバヤシ カツロウ"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="家計レポート（Markdown）を生成します。")
    parser.add_argument("--year", type=int, default=2025, help="対象年（例: 2025）")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/analysis/all_transactions.csv"),
        help="統合CSVのパス",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="レポート出力先。未指定時は reports/{year}_household_cashflow.md",
    )
    return parser.parse_args()


def normalize_text(value: str) -> str:
    text = unicodedata.normalize("NFKC", value or "").upper()
    text = " ".join(text.split())
    return text


def to_int(value: str) -> int:
    try:
        return int((value or "").strip())
    except Exception:
        return 0


def format_yen(value: int) -> str:
    return f"{value:,}"


def classify(name: str, rules: list[tuple[str, list[str]]], fallback: str) -> str:
    for category, patterns in rules:
        if any(re.search(pattern, name) for pattern in patterns):
            return category
    return fallback


def load_rows(csv_path: Path, year: int) -> list[dict[str, str]]:
    if not csv_path.exists():
        raise FileNotFoundError(f"入力CSVが見つかりません: {csv_path}")

    start = date(year, 1, 1)
    end = date(year, 12, 31)
    rows: list[dict[str, str]] = []
    with csv_path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_date = (row.get("date") or "").strip()
            if not raw_date:
                continue
            y, m, d = map(int, raw_date.split("-"))
            day = date(y, m, d)
            if start <= day <= end:
                rows.append(row)
    return rows


def build_bank_monthly(rows: list[dict[str, str]], year: int) -> dict[str, dict[str, int]]:
    months = {f"{year}-{m:02d}": {"in": 0, "out": 0} for m in range(1, 13)}
    for row in rows:
        if row.get("source_name") != "hokurikuBank":
            continue
        ym = (row.get("date") or "")[:7]
        amount = to_int(row.get("amount_jpy", "0"))
        if ym not in months:
            continue
        if amount > 0:
            months[ym]["out"] += amount
        elif amount < 0:
            months[ym]["in"] += -amount
    return months


def build_spending_records(rows: list[dict[str, str]]) -> list[SpendRecord]:
    records: list[SpendRecord] = []
    for row in rows:
        source = row.get("source_name", "")
        amount = to_int(row.get("amount_jpy", "0"))
        if amount <= 0:
            continue

        merchant = normalize_text(row.get("merchant", ""))
        day = row.get("date", "")
        if source in ("rakutenCard", "bitflyerCard"):
            records.append(SpendRecord(day=day, source="card", merchant=merchant, amount=amount))
            continue

        if source == "hokurikuBank":
            if any(re.search(pattern, merchant) for pattern in BANK_EXCLUDE_PATTERNS):
                continue
            records.append(SpendRecord(day=day, source="bank", merchant=merchant, amount=amount))
    return records


def build_monthly_spending_split(
    year: int,
    spend_records: list[SpendRecord],
    special_records: list[SpendRecord],
) -> dict[str, dict[str, int]]:
    monthly = {
        f"{year}-{month:02d}": {"driver": 0, "special": 0, "total": 0}
        for month in range(1, 13)
    }
    for record in spend_records:
        ym = record.day[:7]
        if ym in monthly:
            monthly[ym]["total"] += record.amount
    for record in special_records:
        ym = record.day[:7]
        if ym in monthly:
            monthly[ym]["special"] += record.amount
    for ym in monthly:
        monthly[ym]["driver"] = monthly[ym]["total"] - monthly[ym]["special"]
    return monthly


def render_monthly_integrated_table(
    bank_monthly: dict[str, dict[str, int]],
    monthly_split: dict[str, dict[str, int]],
) -> list[str]:
    lines = [
        "| 月 | 入金(円) | 口座支出(円) | 収支(円) | 主要ドライバー(円) | 特別支出(円) | 分析支出合計(円) |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    total_in = 0
    total_out = 0
    total_net = 0
    total_driver = 0
    total_special = 0
    total_analysis = 0

    months = sorted(set(bank_monthly.keys()) | set(monthly_split.keys()))
    for ym in months:
        income = bank_monthly.get(ym, {}).get("in", 0)
        outflow = bank_monthly.get(ym, {}).get("out", 0)
        net = income - outflow
        driver = monthly_split.get(ym, {}).get("driver", 0)
        special = monthly_split.get(ym, {}).get("special", 0)
        analysis_total = monthly_split.get(ym, {}).get("total", 0)

        total_in += income
        total_out += outflow
        total_net += net
        total_driver += driver
        total_special += special
        total_analysis += analysis_total

        lines.append(
            f"| {ym} | {format_yen(income)} | {format_yen(outflow)} | {format_yen(net)} | "
            f"{format_yen(driver)} | {format_yen(special)} | {format_yen(analysis_total)} |"
        )

    lines.append(
        f"| 合計 | {format_yen(total_in)} | {format_yen(total_out)} | {format_yen(total_net)} | "
        f"{format_yen(total_driver)} | {format_yen(total_special)} | {format_yen(total_analysis)} |"
    )
    return lines


def write_monthly_stacked_svg(
    monthly_split: dict[str, dict[str, int]],
    output_path: Path,
    year: int,
) -> None:
    months = sorted(monthly_split.keys())
    max_total = max((monthly_split[m]["total"] for m in months), default=0)
    y_upper = ((max_total + 99_999) // 100_000) * 100_000 if max_total else 100_000

    width = 1200
    height = 620
    margin_left = 90
    margin_right = 40
    margin_top = 90
    margin_bottom = 120
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom
    x0 = margin_left
    y0 = margin_top
    y_bottom = margin_top + plot_height

    slot_w = plot_width / max(len(months), 1)
    bar_w = slot_w * 0.62

    def y_pos(value: int) -> float:
        if y_upper <= 0:
            return float(y_bottom)
        return y_bottom - (value / y_upper) * plot_height

    def esc(text: str) -> str:
        return html.escape(text, quote=True)

    lines: list[str] = []
    lines.append('<?xml version="1.0" encoding="UTF-8"?>')
    lines.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}">'
    )
    lines.append('<rect x="0" y="0" width="100%" height="100%" fill="#ffffff"/>')

    # Title
    lines.append(
        f'<text x="{width / 2}" y="42" text-anchor="middle" '
        f'font-size="26" font-family="Noto Sans JP, Hiragino Kaku Gothic ProN, sans-serif" '
        f'fill="#1f2937">{esc(f"{year}年 月次支出（主要ドライバー + 特別支出）")}</text>'
    )

    # Grid + Y-axis labels
    tick_count = 5
    for i in range(tick_count + 1):
        value = int(y_upper * i / tick_count)
        y = y_pos(value)
        lines.append(
            f'<line x1="{x0}" y1="{y:.2f}" x2="{x0 + plot_width}" y2="{y:.2f}" '
            f'stroke="#e5e7eb" stroke-width="1"/>'
        )
        lines.append(
            f'<text x="{x0 - 12}" y="{y + 5:.2f}" text-anchor="end" '
            f'font-size="12" font-family="Noto Sans JP, Hiragino Kaku Gothic ProN, sans-serif" '
            f'fill="#6b7280">{esc(format_yen(value))}</text>'
        )

    # Axes
    lines.append(
        f'<line x1="{x0}" y1="{y0}" x2="{x0}" y2="{y_bottom}" stroke="#9ca3af" stroke-width="1.4"/>'
    )
    lines.append(
        f'<line x1="{x0}" y1="{y_bottom}" x2="{x0 + plot_width}" y2="{y_bottom}" stroke="#9ca3af" stroke-width="1.4"/>'
    )

    color_driver = "#4e79a7"
    color_special = "#f28e2b"

    for idx, month in enumerate(months):
        x = x0 + idx * slot_w + (slot_w - bar_w) / 2
        driver = monthly_split[month]["driver"]
        special = monthly_split[month]["special"]
        total = monthly_split[month]["total"]

        y_driver_top = y_pos(driver)
        y_total_top = y_pos(total)

        driver_h = max(0.0, y_bottom - y_driver_top)
        special_h = max(0.0, y_driver_top - y_total_top)

        if driver_h > 0:
            lines.append(
                f'<rect x="{x:.2f}" y="{y_driver_top:.2f}" width="{bar_w:.2f}" height="{driver_h:.2f}" '
                f'fill="{color_driver}"/>'
            )
        if special_h > 0:
            lines.append(
                f'<rect x="{x:.2f}" y="{y_total_top:.2f}" width="{bar_w:.2f}" height="{special_h:.2f}" '
                f'fill="{color_special}"/>'
            )

        # X label (MM)
        mm = month[-2:]
        lines.append(
            f'<text x="{x + bar_w / 2:.2f}" y="{y_bottom + 24}" text-anchor="middle" '
            f'font-size="12" font-family="Noto Sans JP, Hiragino Kaku Gothic ProN, sans-serif" '
            f'fill="#374151">{esc(mm)}</text>'
        )

        # Total label
        if total > 0:
            lines.append(
                f'<text x="{x + bar_w / 2:.2f}" y="{max(y_total_top - 6, y0 + 10):.2f}" text-anchor="middle" '
                f'font-size="11" font-family="Noto Sans JP, Hiragino Kaku Gothic ProN, sans-serif" '
                f'fill="#111827">{esc(format_yen(total))}</text>'
            )

    # Axis labels
    lines.append(
        f'<text x="{x0 + plot_width / 2}" y="{height - 28}" text-anchor="middle" '
        f'font-size="13" font-family="Noto Sans JP, Hiragino Kaku Gothic ProN, sans-serif" fill="#4b5563">月</text>'
    )
    lines.append(
        f'<text x="{22}" y="{y0 + plot_height / 2}" text-anchor="middle" '
        f'font-size="13" font-family="Noto Sans JP, Hiragino Kaku Gothic ProN, sans-serif" fill="#4b5563" '
        f'transform="rotate(-90 22 {y0 + plot_height / 2})">支出（円）</text>'
    )

    # Legend
    lx = width - 360
    ly = 60
    lines.append(f'<rect x="{lx}" y="{ly}" width="18" height="18" fill="{color_driver}"/>')
    lines.append(
        f'<text x="{lx + 26}" y="{ly + 14}" font-size="13" '
        f'font-family="Noto Sans JP, Hiragino Kaku Gothic ProN, sans-serif" fill="#374151">主要ドライバー</text>'
    )
    lines.append(f'<rect x="{lx + 170}" y="{ly}" width="18" height="18" fill="{color_special}"/>')
    lines.append(
        f'<text x="{lx + 196}" y="{ly + 14}" font-size="13" '
        f'font-family="Noto Sans JP, Hiragino Kaku Gothic ProN, sans-serif" fill="#374151">特別支出</text>'
    )

    lines.append("</svg>")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")


def build_driver_summary(spend_records: list[SpendRecord]) -> tuple[dict[str, int], dict[str, dict[str, int]]]:
    totals: dict[str, int] = defaultdict(int)
    examples: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for record in spend_records:
        category = classify(record.merchant, DRIVER_RULES, "その他")
        totals[category] += record.amount
        examples[category][record.merchant] += record.amount
    return totals, examples


def render_driver_table(totals: dict[str, int], examples: dict[str, dict[str, int]]) -> list[str]:
    grand_total = sum(totals.values())
    lines = ["| 分類 | 金額(円) | 構成比 | 主な内容 |", "|---|---:|---:|---|"]
    for category, amount in sorted(totals.items(), key=lambda item: item[1], reverse=True):
        major_items = []
        for name, _ in sorted(examples[category].items(), key=lambda item: item[1], reverse=True)[:3]:
            major_items.append(name if name else "(摘要空欄)")
        major = ", ".join(major_items)
        ratio = (amount / grand_total * 100.0) if grand_total else 0.0
        lines.append(f"| {category} | {format_yen(amount)} | {ratio:.1f}% | {major} |")
    lines.append(f"| 合計 | {format_yen(grand_total)} | 100.0% |  |")
    return lines


def build_special_records(spend_records: list[SpendRecord]) -> list[SpendRecord]:
    months_by_merchant: dict[str, set[str]] = defaultdict(set)
    for record in spend_records:
        months_by_merchant[record.merchant].add(record.day[:7])

    specials: list[SpendRecord] = []
    for record in spend_records:
        merchant_months = len(months_by_merchant[record.merchant])
        if record.amount >= 100_000 or (record.amount >= 50_000 and merchant_months <= 2):
            specials.append(record)
    return specials


def render_special_tables(specials: list[SpendRecord]) -> tuple[list[str], list[str]]:
    category_sum: dict[str, int] = defaultdict(int)
    category_count: dict[str, int] = defaultdict(int)
    for record in specials:
        category = classify(record.merchant, SPECIAL_RULES, "その他特別")
        category_sum[category] += record.amount
        category_count[category] += 1

    class_table = ["| 分類 | 金額(円) | 件数 |", "|---|---:|---:|"]
    for category, amount in sorted(category_sum.items(), key=lambda item: item[1], reverse=True):
        class_table.append(f"| {category} | {format_yen(amount)} | {category_count[category]} |")
    class_table.append(f"| 合計 | {format_yen(sum(category_sum.values()))} | {len(specials)} |")

    top_table = ["| 日付 | ソース | 内容 | 金額(円) |", "|---|---|---|---:|"]
    for record in sorted(specials, key=lambda item: item.amount, reverse=True)[:10]:
        merchant = record.merchant if record.merchant else "(摘要空欄)"
        top_table.append(f"| {record.day} | {record.source} | {merchant} | {format_yen(record.amount)} |")
    return class_table, top_table


def sum_amount_by_patterns(records: list[SpendRecord], patterns: list[str]) -> int:
    total = 0
    for record in records:
        if any(re.search(pattern, record.merchant) for pattern in patterns):
            total += record.amount
    return total


def pattern_metrics(records: list[SpendRecord], pattern: str) -> tuple[int, int, int]:
    total = 0
    count = 0
    months: set[str] = set()
    for record in records:
        if re.search(pattern, record.merchant):
            total += record.amount
            count += 1
            months.add(record.day[:7])
    return total, count, len(months)


def render_improvement_actions(
    spend_records: list[SpendRecord],
    driver_totals: dict[str, int],
    special_records: list[SpendRecord],
) -> list[str]:
    ec_large = driver_totals.get("EC・大型買物", 0)
    grocery = driver_totals.get("食費・日用品", 0)
    insurance = driver_totals.get("保険", 0)
    car = driver_totals.get("交通・車", 0)
    amazon = sum_amount_by_patterns(spend_records, [r"AMAZON", r"AMZ"])
    special_total = sum(record.amount for record in special_records)
    special_monthly = special_total // 12 if special_total else 0

    stop_candidates = [
        ("リユクストレーニング", r"リユクストレ-ニング", "利用頻度が低い場合は停止"),
        ("メルスプラン", r"メルスプラン", "代替手段があれば停止"),
        ("NETFLIX", r"NETFLIX|ネットフリックス", "視聴頻度が低ければ停止"),
        ("FAMILY CLUB", r"FAMILY CLUB", "必要性が低ければ停止"),
        ("スイング", r"スイング", "必須でなければ停止"),
    ]
    reduce_candidates = [
        ("Amazon系", r"AMAZON|AMZ", 0.20, "月次上限を設定（例: 3万円）"),
        ("食費・日用品（主要店）", r"アルビス|バロ|クスリノアオキ|オオサカヤ", 0.05, "週次予算を固定"),
        ("保険（プルデン/あいおい）", r"SMBC\(プルデン|アイオイ", 0.10, "保障重複を見直す"),
        ("車関連", r"ENEOS|オ-トガレ-ジ|ガレ-ジ|ジドウキ|オオウチセキユ", 0.05, "給油・整備の見積比較"),
    ]

    lines: list[str] = []
    lines.append("### 1) やめる候補（停止で効果が出るもの）")
    lines.append("| 候補 | 年間金額(円) | 発生回数 | 発生月数 | 推奨アクション |")
    lines.append("|---|---:|---:|---:|---|")
    stop_total = 0
    for label, pattern, action in stop_candidates:
        amount, count, months = pattern_metrics(spend_records, pattern)
        if amount == 0:
            continue
        stop_total += amount
        lines.append(f"| {label} | {format_yen(amount)} | {count} | {months} | {action} |")
    lines.append(f"| 合計 | {format_yen(stop_total)} |  |  |  |")
    lines.append("")

    lines.append("### 2) 減らす候補（停止せず圧縮するもの）")
    lines.append("| 候補 | 現状金額(円) | 削減率 | 削減見込み(円/年) | 実行ルール |")
    lines.append("|---|---:|---:|---:|---|")
    reduce_total = 0
    for label, pattern, rate, rule in reduce_candidates:
        amount, _, _ = pattern_metrics(spend_records, pattern)
        if amount == 0:
            continue
        saving = int(amount * rate)
        reduce_total += saving
        lines.append(f"| {label} | {format_yen(amount)} | {int(rate * 100)}% | {format_yen(saving)} | {rule} |")
    lines.append(f"| 合計 |  |  | {format_yen(reduce_total)} |  |")
    lines.append("")

    lines.append("### 3) 特別支出の再発防止")
    lines.append(
        f"- 特別支出は `年間 {format_yen(special_total)}円`。"
        f" 毎月 `約{format_yen(special_monthly)}円` を特別費口座へ先取り積立。"
    )
    lines.append("- 5万円超の支出は「48時間保留 + 家族合意」を運用ルール化。")
    lines.append("")
    lines.append("### 4) 参考（カテゴリ規模）")
    lines.append(
        f"- EC・大型買物: `{format_yen(ec_large)}円/年`（Amazon系 `{format_yen(amazon)}円/年`）"
    )
    lines.append(f"- 食費・日用品: `{format_yen(grocery)}円/年`")
    lines.append(f"- 保険: `{format_yen(insurance)}円/年`")
    lines.append(f"- 交通・車: `{format_yen(car)}円/年`")
    return lines


def generate_report(year: int, rows: list[dict[str, str]], output_path: Path) -> tuple[str, Path]:
    bank_monthly = build_bank_monthly(rows, year)
    spend_records = build_spending_records(rows)
    driver_totals, driver_examples = build_driver_summary(spend_records)
    special_records = build_special_records(spend_records)
    monthly_split = build_monthly_spending_split(year, spend_records, special_records)
    special_class_table, special_top_table = render_special_tables(special_records)
    chart_filename = f"{year}_monthly_stacked_drivers_special.svg"
    chart_path = output_path.parent / chart_filename
    write_monthly_stacked_svg(monthly_split, chart_path, year)

    now_text = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    lines: list[str] = []
    lines.append(f"# 家計レポート {year}")
    lines.append("")
    lines.append(f"- 生成日時(UTC): `{now_text}`")
    lines.append("- 月次支出入: `hokurikuBank` ベース")
    lines.append("- 支出ドライバー: `カード支出 + 銀行直接支出(カード引落・家族間振替除外)`")
    lines.append("")
    lines.append("## 月次 支出入・支出内訳（統合表）")
    lines.extend(render_monthly_integrated_table(bank_monthly, monthly_split))
    lines.append("")
    lines.append("## 月次 支出積み上げグラフ（主要ドライバー + 特別支出）")
    lines.append(f"![月次支出積み上げグラフ]({chart_filename})")
    lines.append("")
    lines.append("## 主要支出ドライバー分類")
    lines.extend(render_driver_table(driver_totals, driver_examples))
    lines.append("")
    lines.append("## 特別支出分類")
    lines.extend(special_class_table)
    lines.append("")
    lines.append("## 特別支出 上位10件")
    lines.extend(special_top_table)
    lines.append("")
    lines.append("## 支出改善アクション（優先5項目）")
    lines.extend(render_improvement_actions(spend_records, driver_totals, special_records))
    lines.append("")
    return "\n".join(lines), chart_path


def main() -> int:
    args = parse_args()
    output_path = args.output or Path(f"reports/{args.year}_household_cashflow.md")

    rows = load_rows(args.input, args.year)
    if not rows:
        print(f"対象年データがありません: {args.year}")
        return 1

    report, chart_path = generate_report(args.year, rows, output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report, encoding="utf-8")
    print(f"[DONE] year={args.year} rows={len(rows)} output={output_path} chart={chart_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
