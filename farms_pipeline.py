#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
import openpyxl
from openpyxl.utils import get_column_letter
import subprocess
import sys


# ----------------------------
# Text normalization
# ----------------------------

def norm_text(x):
    if not isinstance(x, str):
        return x
    x = unicodedata.normalize("NFKC", x).strip()
    x = re.sub(r"\s+", " ", x)
    return x

def key_norm(s: str) -> str:
    """
    月初報告の企業名キー一致専用。
    見た目から同じなら一致させるため、改行・空白を除去して比較する。
    """
    s = unicodedata.normalize("NFKC", str(s))
    s = s.replace("／", "/")
    s = s.replace("\n", "")
    s = re.sub(r"\s+", "", s) # 空白は全部消す

    return s

# ----------------------------
# Masters
# ----------------------------

@dataclass(frozen=True)
class VegRule:
    suffix: str
    bucket: str  # "べビーリーフ" | "べビーリーフ以外" | "加工用"


@dataclass
class Masters:
    company_map: Dict[str, str]
    veg_rules: Dict[str, VegRule]


def load_masters(master_path: Path) -> Masters:
    comp = pd.read_excel(master_path, sheet_name="企業名")
    if not {"元データ", "変換後名"}.issubset(set(comp.columns)):
        raise ValueError("master[企業名] must have columns: 元データ, 変換後名")

    company_map: Dict[str, str] = {}
    for _, r in comp.iterrows():
        src = norm_text(r["元データ"])
        dst = r["変換後名"]
        dst = norm_text(dst) if isinstance(dst, str) else dst
        company_map[src] = dst

    veg = pd.read_excel(master_path, sheet_name="収穫野菜名")
    need = {"元データ", "変換後名（企業名にくっつける）", "振り分け"}
    if not need.issubset(set(veg.columns)):
        raise ValueError("master[収穫野菜名] must have columns: 元データ, 変換後名（企業名にくっつける）, 振り分け")

    veg_rules: Dict[str, VegRule] = {}
    for _, r in veg.iterrows():
        src = norm_text(r["元データ"])
        suffix = r["変換後名（企業名にくっつける）"]
        bucket = r["振り分け"]
        suffix = norm_text(suffix) if isinstance(suffix, str) else ""
        bucket = norm_text(bucket) if isinstance(bucket, str) else "不明"
        veg_rules[src] = VegRule(suffix=suffix, bucket=bucket)

    return Masters(company_map=company_map, veg_rules=veg_rules)


# ----------------------------
# Mapping
# ----------------------------

def map_company(raw_company: str, masters: Masters) -> Optional[str]:
    if not isinstance(raw_company, str):
        return None
    raw_company = norm_text(raw_company)

    if raw_company in masters.company_map:
        v = masters.company_map[raw_company]
        if v in (None, "", "×") or (isinstance(v, float) and pd.isna(v)):
            return None
        return norm_text(v) if isinstance(v, str) else v

    base = raw_company.split("/")[0]
    if base in set(v for v in masters.company_map.values() if isinstance(v, str)):
        return base

    return None


def map_veg(raw_veg: str, masters: Masters) -> Tuple[str, str, str]:
    if not isinstance(raw_veg, str):
        return (str(raw_veg), "不明", "")

    raw_veg = norm_text(raw_veg)

    if raw_veg.startswith("BL"):
        cultivar = raw_veg.replace("BL", "", 1)
        return (f"ベビーリーフ　{cultivar}", "べビーリーフ", "")

    if raw_veg in masters.veg_rules:
        rule = masters.veg_rules[raw_veg]
        return (raw_veg, rule.bucket, rule.suffix)

    if raw_veg.startswith("ベビーリーフ"):
        return (raw_veg, "べビーリーフ", "")

    return (raw_veg, "不明", "")


def company_proc_for_bucket(company_norm: str, bucket: str, suffix: str) -> str:
    return company_norm if bucket == "べビーリーフ" else company_norm + (suffix or "")


def company_key_for_report(company_proc: str) -> str:
    if "/" not in company_proc:
        return company_proc
    base, suffix = company_proc.split("/", 1)
    return f"{base}\n/{suffix}"


# ----------------------------
# Read 正規化.xlsx
# ----------------------------

def read_norm_xlsx(norm_src: Path) -> pd.DataFrame:
    df = pd.read_excel(norm_src, sheet_name=0)
    df.columns = [norm_text(c) for c in df.columns]

    rename = {}
    for c in df.columns:
        if c in ("得意先名", "企業", "企業名"):
            rename[c] = "企業名"
        if c in ("収穫物", "野菜名", "品目", "作物", "作物名", "収穫野菜名"):
            rename[c] = "収穫野菜名"
        if c in ("収穫量(g)", "収穫量（g）", "収穫量（ｇ）", "収穫量", "重量(g)", "重量（g）", "重量"):
            rename[c] = "収穫量（ｇ）"
    df = df.rename(columns=rename)

    need = {"収穫日", "企業名", "収穫野菜名", "収穫量（ｇ）"}
    missing = need - set(df.columns)
    if missing:
        raise ValueError(f"正規化ファイルに必要列がありません: {missing}")

    df["収穫日_parsed"] = pd.to_datetime(df["収穫日"], errors="coerce")
    df["企業名"] = df["企業名"].map(norm_text)
    df["収穫野菜名"] = df["収穫野菜名"].map(norm_text)
    return df


def normalize_from_norm_df(df: pd.DataFrame, masters: Masters, month: int) -> pd.DataFrame:
    rows = []
    for _, r in df.iterrows():
        d = r["収穫日_parsed"]
        if pd.isna(d) or int(d.month) != int(month):
            continue

        comp_norm = map_company(r["企業名"], masters)
        veg_std, bucket, suffix = map_veg(r["収穫野菜名"], masters)

        company_proc = None
        if comp_norm is not None:
            company_proc = company_proc_for_bucket(comp_norm, bucket, suffix)

        rows.append({
            "month": int(d.month),
            "date": d.date(),
            "company_raw": r["企業名"],
            "veg_raw": r["収穫野菜名"],
            "company_proc": company_proc,
            "veg": veg_std,
            "bucket": bucket,
            "amount_g": float(r["収穫量（ｇ）"]) if r["収穫量（ｇ）"] not in (None, "") else 0.0,
        })

    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(["date", "company_proc", "veg"], na_position="last").reset_index(drop=True)
    return out

def is_nikken(company_proc: str) -> bool:
    """
    日建判定（要調整ポイント）
    company_proc 例: '日建/葉物野菜' や '日建' を想定
    """
    if not isinstance(company_proc, str):
        return False
    base = company_proc.split("/", 1)[0]
    base = norm_text(base)
    return ("日建" in base)  # ここは必要なら厳密一致に変えてOK


def find_total_block_rows(ws, label: str) -> dict:
    """
    合計ブロック（例: '企業月間生産量合計', '月間総合計'）の行を探して、
    58期/59期/増減 の行番号を返す。
    前提：B列にラベル、C列に '58期' '59期' '増　減' が並ぶ形式。
    """
    # B列でラベルを探す
    target_row = None
    for r in range(1, ws.max_row + 1):
        v = ws.cell(r, 2).value
        if isinstance(v, str) and norm_text(v) == label:
            target_row = r
            break
    if target_row is None:
        raise ValueError(f"[report] total label not found: {label} (sheet={ws.title})")

    # ラベル行の直下〜数行に 58期/59期/増減 がある前提で探す（柔軟に）
    rows = {}
    for r in range(target_row, min(target_row + 10, ws.max_row) + 1):
        v = ws.cell(r, 3).value
        if isinstance(v, str):
            vv = norm_text(v)
            if vv == "58期":
                rows["prev"] = r
            elif vv == "59期":
                rows["curr"] = r
            elif "増" in vv:
                rows["diff"] = r
    # 最低限 prev/curr は必須
    if "prev" not in rows or "curr" not in rows:
        raise ValueError(f"[report] total rows not found under label={label} (sheet={ws.title})")

    return rows

# ----------------------------
# Harvest rebuild: C~F clear from row 3, write from row 7
# ----------------------------

def clear_range(ws, start_row: int, start_col: int, end_col: int):
    r = start_row
    while True:
        # 終端判定：C列が空で、D〜Fも空なら終わり（無限ループ防止）
        if all(ws.cell(r, c).value in (None, "") for c in range(start_col, end_col + 1)):
            break
        for c in range(start_col, end_col + 1):
            ws.cell(r, c).value = None
        r += 1


def write_harvest_rebuild(
    harvest_template: Path,
    out_path: Path,
    norm: pd.DataFrame,
    month: int,
    farm_name: str = "愛川",
    write_start_row: int = 7,  # ★例(3〜6)は触らない
) -> None:
    wb = openpyxl.load_workbook(harvest_template)
    sheet_name = f"{month}月"
    if sheet_name not in wb.sheetnames:
        raise ValueError(f"収穫データテンプレにシート '{sheet_name}' がありません。存在: {wb.sheetnames}")
    ws = wb[sheet_name]

    # ★仕様：C〜Fの3行目以降を毎回削除（例は3〜6だが「値は入れない」方針なので、
    # クリアは3行目から行う。ただし書き込みは7行目から）
    clear_range(ws, start_row=3, start_col=3, end_col=6)

    sub = norm[norm["company_proc"].notna()].copy()
    sub = sub.sort_values(["date", "company_proc", "veg"]).reset_index(drop=True)

    row = write_start_row
    written = 0
    for rr in sub.itertuples(index=False):
        ws.cell(row, 2).value = farm_name         # B
        ws.cell(row, 3).value = rr.date           # C
        ws.cell(row, 4).value = rr.company_proc   # D
        ws.cell(row, 5).value = rr.veg            # E
        ws.cell(row, 6).value = float(rr.amount_g)# F
        row += 1
        written += 1

    wb.save(out_path)
    print(f"[harvest] rebuild_written={written} (write_from_row={write_start_row})")


# ----------------------------
# Report write (same as before): 58期キー→59期行、無ければ追加
# ----------------------------

def find_cell(ws, value) -> Optional[Tuple[int, int]]:
    for r in range(1, ws.max_row + 1):
        for c in range(1, ws.max_column + 1):
            if ws.cell(r, c).value == value:
                return r, c
    return None


def build_month_column_map(ws) -> Dict[int, int]:
    pos = find_cell(ws, "10月")
    if not pos:
        raise ValueError(f"月ヘッダ（例: '10月'）が見つかりません: sheet={ws.title}")
    header_row = pos[0]

    month_cols: Dict[int, int] = {}
    for c in range(1, ws.max_column + 1):
        v = ws.cell(header_row, c).value
        if isinstance(v, str) and re.fullmatch(r"\d{1,2}月", v):
            m = int(v.replace("月", ""))
            month_cols[m] = c
    return month_cols


def index_rows_for_curr_ki(ws, key_col: int, prev_ki: int, curr_ki: int) -> Dict[str, int]:
    prev = f"{prev_ki}期"
    curr = f"{curr_ki}期"

    out: Dict[str, int] = {}
    r = 1
    while r <= ws.max_row:
        if ws.cell(r, 3).value == prev:
            key = ws.cell(r, key_col).value
            if isinstance(key, str) and key.strip():
                if ws.cell(r + 1, 3).value == curr:
                    out[key_norm(key)] = r + 1
        r += 1
    return out


def next_no_value(ws) -> int:
    mx = 0
    for r in range(1, ws.max_row + 1):
        v = ws.cell(r, 1).value
        if isinstance(v, (int, float)):
            mx = max(mx, int(v))
    return mx + 1 if mx > 0 else 1


def append_company_block(ws, key: str, prev_ki: int, curr_ki: int) -> int:
    start_no = next_no_value(ws)
    r = ws.max_row + 1

    ws.cell(r, 1).value = start_no
    ws.cell(r, 2).value = key
    ws.cell(r, 3).value = f"{prev_ki}期"

    ws.cell(r + 1, 3).value = f"{curr_ki}期"
    ws.cell(r + 2, 3).value = "増　減"
    ws.cell(r + 3, 3).value = "稼働棟数"

    for c in range(4, ws.max_column + 1):
        col = get_column_letter(c)
        ws.cell(r + 2, c).value = f"={col}{r+1}-{col}{r}"

    return r + 1


def write_report(
    report_template: Path,
    out_path: Path,
    norm: pd.DataFrame,
    ki: int,
    month: int,
    allow_append_rows: bool = False, # 推奨：テンプレに全部入れるなら False
) -> Dict[str, float]:
    wb = openpyxl.load_workbook(report_template)

    sub = norm[norm["company_proc"].notna()].copy()
    sub["kg"] = sub["amount_g"].astype(float) / 1000.00

    sheets = {
        "べビーリーフ": "企業月間生産量（ベビーリーフ）",
        "べビーリーフ以外": "企業月間生産量（ベビーリーフ以外）",
        "加工用": "企業別月間生産量 (加工用） ",
    }

    prev_ki = ki - 1
    written_sum_by_bucket: Dict[str, float] = {}

    for bucket, sname in sheets.items():
        if sname not in wb.sheetnames:
            raise ValueError(f"月初報告テンプレにシート '{sname}' がありません。存在: {wb.sheetnames}")

        ws = wb[sname]
        month_cols = build_month_column_map(ws)
        if month not in month_cols:
            raise ValueError(f"{sname} に {month}月 がありません。検出月={sorted(month_cols.keys())}")
        month_col = month_cols[month]

        row_index = index_rows_for_curr_ki(ws, key_col=2, prev_ki=prev_ki, curr_ki=ki)

        agg = sub[sub["bucket"] == bucket].groupby("company_proc", as_index=False)["kg"].sum()

        wrote_kg = 0.0
        for _, r in agg.iterrows():
            company_proc = str(r["company_proc"])
            val = round(float(r["kg"]), 2)

            if bucket == "べビーリーフ":
                key = key_norm(company_proc)
            else:
                key = key_norm(company_key_for_report(company_proc))

            if key not in row_index:
                if not allow_append_rows:
                    raise ValueError(f"[report] row not found: sheet={sname}, key={key}")
                row_index[key] = append_company_block(ws, key, prev_ki=prev_ki, curr_ki=ki)

            ws.cell(row_index[key], month_col).value = val
            wrote_kg += float(r["kg"])

        # ---- 合計行（企業月間合計 / 月間総合計）を書き戻す ----
        # sub: normからbucketで絞ったデータ（kg）
        sub_bucket = sub[sub["bucket"] == bucket].copy()
        sub_bucket["is_nikken"] = sub_bucket["company_proc"].map(is_nikken)

        # 企業月間合計：日建除外
        total_excl = float(sub_bucket.loc[~sub_bucket["is_nikken"], "kg"].sum())
        # 月間総合計：日建含む
        total_all = float(sub_bucket["kg"].sum())

        # テンプレの合計ブロック行を探す（B列ラベル）
        rows_company_total = find_total_block_rows(ws, "企業月間生産量合計")
        rows_grand_total   = find_total_block_rows(ws, "月間総合計")

        # 今回は「対象月だけ」書く（month_col）
        # 58期/59期を両方同じ値で埋めるのではなく、本来は prev_ki/curr_ki を別データから出す。
        # いまは“59期の対象月”を作っているので curr に入れる。prev は空欄にするか、比較用に別実装。
        ws.cell(rows_company_total["curr"], month_col).value = round(total_excl, 2)
        ws.cell(rows_grand_total["curr"],   month_col).value = round(total_all, 2)

        # 増減はテンプレの式を活かす（あれば）。無ければ値を書いてもOK。
        # ここでは式がある前提で触らない（UI崩壊防止）


        written_sum_by_bucket[bucket] = wrote_tons

    wb.save(out_path)
    return written_sum_by_bucket


def write_unmapped(out_path: Path, norm: pd.DataFrame) -> None:
    unm_comp = sorted(norm.loc[norm["company_proc"].isna(), "company_raw"].dropna().unique().tolist())
    unm_veg = sorted(norm.loc[norm["bucket"] == "不明", "veg_raw"].dropna().unique().tolist())
    sample = norm[(norm["company_proc"].isna()) | (norm["bucket"] == "不明")].head(300)

    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        pd.DataFrame({"unmapped_company_raw": unm_comp}).to_excel(w, index=False, sheet_name="企業名(未変換)")
        pd.DataFrame({"unmapped_veg_raw": unm_veg}).to_excel(w, index=False, sheet_name="野菜名(未分類)")
        sample.to_excel(w, index=False, sheet_name="サンプル行(最大300)")


def validate_totals(norm: pd.DataFrame, written_sum_by_bucket: Dict[str, float], tol: float = 1e-3) -> None:
    sub = norm[norm["company_proc"].notna()].copy()
    sub["kg"] = sub["amount_g"].astype(float) / 1000.00

    expected = sub.groupby("bucket")["kg"].sum().to_dict()
    for bucket, exp in expected.items():
        got = written_sum_by_bucket.get(bucket, 0.0)
        if abs(float(exp) - float(got)) > tol:
            raise ValueError(f"[validate] bucket mismatch: {bucket}: expected={exp_all:.3f}kg, written={got_all:.3f}kg")

    exp_all = float(sub["tons"].sum())
    got_all = float(sum(written_sum_by_bucket.values()))
    if abs(exp_all - got_all) > tol:
        raise ValueError(f"[validate] total mismatch: expected={exp_all:.6f}t, written={got_all:.6f}t")


def run_suffix(report_xlsx: Path, month: int, out_path: Path) -> None:
    script = Path("fill_by_company_suffix.py")
    if not script.exists():
        return
    cmd = [
        sys.executable, str(script),
        "--src", str(report_xlsx),
        "--dst", str(out_path),
        "--month", str(month),
        "--out", str(out_path),
        "--mode", "add",
    ]
    subprocess.run(cmd, check=True)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--norm-src", type=Path, required=True)
    p.add_argument("--master", type=Path, required=True)
    p.add_argument("--harvest-template", type=Path, required=True)
    p.add_argument("--report-template", type=Path, required=True)
    p.add_argument("--ki", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    p.add_argument("--outdir", type=Path, default=Path("out"))
    p.add_argument("--farm-name", type=str, default="愛川")
    p.add_argument("--validate", action="store_true")

    args = p.parse_args()
    args.outdir.mkdir(parents=True, exist_ok=True)

    masters = load_masters(args.master)
    raw = read_norm_xlsx(args.norm_src)
    norm = normalize_from_norm_df(raw, masters, month=args.month)

    harvest_out = args.outdir / f"harvest_{args.ki}ki_{args.month}.xlsx"
    report_out = args.outdir / f"report_{args.ki}ki_{args.month}.xlsx"
    unmapped_out = args.outdir / f"unmapped_{args.ki}ki_{args.month}.xlsx"
    suffix_out = args.outdir / f"teisyutu_filled_{args.ki}ki_{args.month}.xlsx"

    # harvest: rebuild
    write_harvest_rebuild(
        args.harvest_template,
        harvest_out,
        norm,
        args.month,
        farm_name=args.farm_name,
        write_start_row=7,
    )

    # report: rebuild (row auto append)
    written_sum_by_bucket = write_report(
        args.report_template,
        report_out,
        norm,
        args.ki,
        args.month,
        allow_append_rows=True,
    )

    write_unmapped(unmapped_out, norm)

    if args.validate:
        validate_totals(norm, written_sum_by_bucket)

    suffix_msg = "(skip) fill_by_company_suffix.py not found"
    if Path("fill_by_company_suffix.py").exists():
        run_suffix(report_out, args.month, suffix_out)
        suffix_msg = str(suffix_out)

    print("OK")
    print(f"- harvest:  {harvest_out}")
    print(f"- report:   {report_out}")
    print(f"- unmapped: {unmapped_out}")
    print(f"- suffix:   {suffix_msg}")


if __name__ == "__main__":
    main()

