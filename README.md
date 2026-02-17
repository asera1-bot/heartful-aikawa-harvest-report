# heartful-aikawa-harvest-report

s-report-pipeline

ファーモス出力（Excel）を、正規化マスタ（企業名/収穫野菜名）に従って変換し、
1) 収穫データ愛川OO期（B〜F列）へ追記
2) 月初報告（3シート）へ集計反映
3) 未変換（企業名/野菜名）リストを出力

Docker不要。Python + openpyxl/pandas で最小構成。

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run (例)

```bash
python3 farmos_pipeline.py \
  --farmos farmos.xlsx \
  --master 正規化.xlsx \
  --harvest-template 収穫データ愛川59期.xlsx \
  --report-template 月初報告_59期企業別集計表＿愛川農園.xlsx \
  --ki 59 \
  --month 10 \
  --outdir out
```

### Outputs
- `out/harvest_59ki_10.xlsx` : 収穫データ（10月シートへ追記）
- `out/report_59ki_10.xlsx` : 月初報告（10月列へ反映）
- `out/unmapped_59ki_10.xlsx` : 未変換一覧（企業名/野菜名）とサンプル行

## Notes
- 収穫量は g → t（トン）に変換して月初報告へ入力します（t = g / 1,000,000）。
- 月初報告の行見出し（企業名/企業名/野菜名）はテンプレ側に存在する前提です。
  未登録の行がある場合は `unmapped_*.xlsx` に出ます（必要なら自動追記機能も追加可能）。

