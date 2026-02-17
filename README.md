# life_advisor

家計・資産管理プロジェクトです。  
現在はクレジットカード明細を主データとして、支出見直しを進めます。

## 楽天カードCSV取込み

1. 楽天カードe-NAVIから月次CSVをダウンロード
2. CSVを `data/rakutenCard/` に保存
3. 以下を実行

```bash
python3 scripts/import_rakuten_csv.py
```

出力先:
- `data/rakutenCard/normalized_transactions.csv`

主なオプション:

```bash
# 利用日が「月/日」だけの行に年を補完
python3 scripts/import_rakuten_csv.py --default-year 2026

# 個別ファイルを指定（複数指定可）
python3 scripts/import_rakuten_csv.py \
  --input-file data/rakutenCard/2026-01.csv \
  --input-file data/rakutenCard/2026-02.csv
```

標準化カラム:
- `transaction_id`
- `date`
- `date_raw`
- `merchant`
- `amount_jpy`
- `cardholder`
- `category`
- `memo`
- `payment_method`
- `source_file`
- `source_row`
- `source_encoding`
- `imported_at`

## bitFlyerカードCSV取込み

1. bitFlyerクレカ（Aplus明細）CSVをダウンロード
2. CSVを `data/bitflyerCard/` に保存
3. 以下を実行

```bash
python3 scripts/import_bitflyer_csv.py
```

出力先:
- `data/bitflyerCard/normalized_transactions.csv`

主なオプション:

```bash
# 個別ファイルを指定（複数指定可）
python3 scripts/import_bitflyer_csv.py \
  --input-file data/bitflyerCard/aplus_meisai_9173_202601.csv \
  --input-file data/bitflyerCard/aplus_meisai_9173_202602.csv
```

## 北陸銀行CSV取込み

1. 北陸銀行の入出金明細CSVをダウンロード
2. CSVを `data/hokurikuBank/` に保存
3. 以下を実行

```bash
python3 scripts/import_hokuriku_bank_csv.py
```

出力先:
- `data/hokurikuBank/normalized_transactions.csv`

主なオプション:

```bash
# 個別ファイルを指定（複数指定可）
python3 scripts/import_hokuriku_bank_csv.py \
  --input-file data/hokurikuBank/ny20260217160703.csv
```

補足:
- 北陸銀行データの `amount_jpy` は「支出を正、入金を負」で出力します。

## 分析対象へ統合

各ソースの標準化CSVを統合して、分析入力を作成します。

```bash
python3 scripts/build_analysis_transactions.py
```

出力先:
- `data/analysis/all_transactions.csv`

実行順序（推奨）:

```bash
python3 scripts/import_rakuten_csv.py
python3 scripts/import_bitflyer_csv.py
python3 scripts/import_hokuriku_bank_csv.py
python3 scripts/build_analysis_transactions.py
```
