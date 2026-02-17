#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import openpyxl


# ----------------------------
# Utilities
# ----------------------------

def norm_text(x):
    if not isinstance(x, str):
        return x
    x = unicodedata.normalize("NFKC", x).strip()
    x = re.sub(r"\s+", " ", x)
    return x

def first_empty_row(ws, col: int, start_row: int = 3) -> int:
    """Find first empty row in a column."""
    r = start_row
    while ws.cell(r, col).value not in (None, ""):
        r += 1
    return r

def find_cell(ws, value) -> Optional[Tuple[int,int]]:
    """Find exact cell value (first hit)."""
    for r in range(1, ws.max_row + 1):
        for c in range(1, ws.max_column + 1):
            if ws.cell(r, c).value == value:
                return r, c
    return None

def find_header_row(ws, header_value: str) -> Tuple[int,int]:
    pos = find_cell(ws, header_value)
    if not pos:
        raise ValueError(f"Header '{header_value}' not found in sheet '{ws.title}'")
    return pos

def company_key_for_report(company_proc: str) -> str:
    """Template uses newline before suffix: '牧野フライス\\n/葉物野菜'."""
    if "/" not in company_proc:
        return company_proc
    base, suffix = company_proc.split("/", 1)
    return f"{base}\n/{suffix}"


# ----------------------------
# Master mappings
# ----------------------------

@dataclass(frozen=True)
class VegRule:
    suffix: str           # string to append to company for non-baby leaf (e.g. "/葉物野菜") or "/加工用"
    bucket: str           # 'べビーリーフ' | 'べビーリーフ以外' | '加工用'


@dataclass
class Masters:
    company_map: Dict[str, str]
    veg_rules: Dict[str, VegRule]


def load_masters(master_path: Path) -> Masters:
    # company
    comp = pd.read_excel(master_path, sheet_name="企業名")
    if not {"元データ", "変換後名"}.issubset(set(comp.columns)):
        raise ValueError("master[企業名] must have columns: 元データ, 変換後名")
    company_map = {}
    for _, r in comp.iterrows():
        src = norm_text(r["元データ"])
        dst = r["変換後名"]
        if isinstance(dst, str):
            dst = norm_text(dst)
        company_map[src] = dst

    # veg
    veg = pd.read_excel(master_path, sheet_name="収穫野菜名")
    need = {"元データ", "変換後名（企業名にくっつける）", "振り分け"}
    if not need.issubset(set(veg.columns)):
        raise ValueError("master[収穫野菜名] must have columns: 元データ, 変換後名（企業名にくっつける）, 振り分け")
    veg_rules: Dict[str, VegRule] = {}
    for _, r in veg.iterrows():
        src = norm_text(r["元データ"])
        suffix = r["変換後名（企業名にくっつける）"]
        bucket = r["振り分け"]
        if isinstance(suffix, str):
            suffix = norm_text(suffix)
        else:
            suffix = ""
        if isinstance(bucket, str):
            bucket = norm_text(bucket)
        else:
            bucket = "不明"
        veg_rules[src] = VegRule(suffix=suffix, bucket=bucket)

    return Masters(company_map=company_map, veg_rules=veg_rules)


# ----------------------------
# Farmos read + normalize
# ----------------------------

def read_farmos(farmos_path: Path) -> pd.DataFrame:
    xl = pd.ExcelFile(farmos_path)
    sheets = [s for s in xl.sheet_names if s != "原本"]
    parts = []
    for s in sheets:
        # many farmos exports have a title row; header=1 works for your current export
        df = pd.read_excel(farmos_path, sheet_name=s, header=1)
        df["__sheet"] = s
        parts.append(df)
    df = pd.concat(parts, ignore_index=True)
    # normalize columns
    df.columns = [norm_text(c) for c in df.columns]
    for col in ["企業名", "収穫野菜名"]:
        if col in df.columns:
            df[col] = df[col].map(norm_text)
    # dates
    df["収穫日_parsed"] = pd.to_datetime(df.get("収穫日"), errors="coerce")
    return df


def map_company(raw_company: str, masters: Masters) -> Optional[str]:
    if not isinstance(raw_company, str):
        return None

    raw_company = norm_text(raw_company)

    # 1) exact master mapping
    if raw_company in masters.company_map:
        v = masters.company_map[raw_company]
        if v in (None, "", "×") or (isinstance(v, float) and pd.isna(v)):
            return None
        return v

    # 2) if already converted form like 'QB/葉物野菜' -> take base and accept
    base = raw_company.split("/")[0]
    if base in set(masters.company_map.values()):
        return base

    # 3) small synonyms (optional; expand if needed)
    syn = {
        "バリュエンス": "バリュ",
        "サンセイランディック": "サンセイ",
        "サンテレホン": "サンテレ",
    }
    if base in syn:
        return syn[base]

    return None


def map_veg(raw_veg: str, masters: Masters) -> Tuple[str, str, str]:
    """
    returns (veg_std, bucket, suffix_to_company)
    - BL○○ => ベビーリーフ ○○
    - master match => use bucket + suffix from master
    """
    if not isinstance(raw_veg, str):
        return (str(raw_veg), "不明", "")

    raw_veg = norm_text(raw_veg)

    # patterns in your export
    if raw_veg.startswith("BL"):
        cultivar = raw_veg.replace("BL", "", 1)
        return (f"ベビーリーフ　{cultivar}", "べビーリーフ", "")

    if raw_veg in {"加工BL", "加工ＢＬ", "加工BL "}:
        return ("加工品　ベビーリーフ", "加工用", "/加工BL")

    # master
    if raw_veg in masters.veg_rules:
        rule = masters.veg_rules[raw_veg]
        return (raw_veg, rule.bucket, rule.suffix)

    # fallback
    if raw_veg.startswith("ベビーリーフ"):
        return (raw_veg, "べビーリーフ", "")

    return (raw_veg, "不明", "")


def normalize_farmos(df: pd.DataFrame, masters: Masters) -> pd.DataFrame:
    need_cols = {"企業名", "収穫日_parsed", "収穫野菜名", "収穫量（ｇ）"}
    missing = need_cols - set(df.columns)
    if missing:
        raise ValueError(f"Farmos export missing columns: {missing}")

    rows = []
    for _, r in df.iterrows():
        date = r["収穫日_parsed"]
        if pd.isna(date):
            continue
        month = int(date.month)

        comp_norm = map_company(r["企業名"], masters)
        veg_std, bucket, suffix = map_veg(r["収穫野菜名"], masters)

        if comp_norm is None:
            company_proc = None
        else:
            if bucket == "べビーリーフ":
                company_proc = comp_norm
            else:
                company_proc = comp_norm + (suffix or "")

        rows.append({
            "month": month,
            "date": date.date(),
            "company_raw": r["企業名"],
            "veg_raw": r["収穫野菜名"],
            "company_proc": company_proc,
            "veg": veg_std,
            "bucket": bucket,
            "amount_g": r["収穫量（ｇ）"],
            "sheet": r.get("__sheet"),
        })

    out = pd.DataFrame(rows)
    return out


# ----------------------------
# Write: harvest workbook
# ----------------------------

def write_harvest(harvest_template: Path, out_path: Path, norm: pd.DataFrame, month: int):
    wb = openpyxl.load_workbook(harvest_template)
    sheet_name = f"{month}月"
    if sheet_name not in wb.sheetnames:
        raise ValueError(f"Harvest template missing sheet '{sheet_name}'. Existing: {wb.sheetnames}")

    ws = wb[sheet_name]

    # append rows (A can be blank)
    insert_at = first_empty_row(ws, col=3, start_row=3)  # col C=収穫日
    sub = norm[(norm["month"] == month) & norm["company_proc"].notna()].copy()
    for i, r in enumerate(sub.itertuples(index=False), start=0):
        row = insert_at + i
        ws.cell(row, 1).value = None               # A: 収穫ID (空でOK)
        ws.cell(row, 2).value = "愛川"             # B: 農園名（固定）
        ws.cell(row, 3).value = r.date             # C: 収穫日
        ws.cell(row, 4).value = r.company_proc     # D: 企業名（変換・加工後）
        ws.cell(row, 5).value = r.veg              # E: 収穫野菜名（標準化）
        ws.cell(row, 6).value = float(r.amount_g)  # F: g

    wb.save(out_path)


# ----------------------------
# Write: report workbook
# ----------------------------

def build_month_column_map(ws) -> Dict[int, int]:
    """
    returns {month:int -> col_index}
    expects header row has cells like '10月', '11月', ...
    """
    # find header row containing '10月'
    pos = find_cell(ws, "10月")
    if not pos:
        raise ValueError(f"Cannot find month headers (e.g. '10月') in sheet '{ws.title}'")
    header_row = pos[0]
    month_cols = {}
    for c in range(1, ws.max_column + 1):
        v = ws.cell(header_row, c).value
        if isinstance(v, str) and re.fullmatch(r"\d{1,2}月", v):
            m = int(v.replace("月", ""))
            month_cols[m] = c
    return month_cols

def index_company_rows(ws, key_col: int, ki: int) -> Dict[str, int]:
    """
    Find rows where column C equals f'{ki}期' and map key_col string to that row.
    For BL sheet: key_col=2 ('企業名')
    For others: key_col=2 ('企業名/野菜名')
    """
    target = f"{ki}期"
    rows = {}
    for r in range(1, ws.max_row + 1):
        if ws.cell(r, 3).value == target:
            key = ws.cell(r, key_col).value
            if isinstance(key, str) and key.strip():
                rows[norm_text(key)] = r
    return rows

def write_report(report_template: Path, out_path: Path, norm: pd.DataFrame, ki: int, month: int):
    wb = openpyxl.load_workbook(report_template)

    # aggregate grams
    sub = norm[(norm["month"] == month) & norm["company_proc"].notna()].copy()
    sub["tons"] = sub["amount_g"].astype(float) / 1_000_000.0

    # split by bucket
    buckets = {
        "べビーリーフ": "企業月間生産量（ベビーリーフ）",
        "べビーリーフ以外": "企業月間生産量（ベビーリーフ以外）",
        "加工用": "企業別月間生産量 (加工用） ",
    }

    for bucket, sheet_name in buckets.items():
        if sheet_name not in wb.sheetnames:
            raise ValueError(f"Report template missing sheet '{sheet_name}'. Existing: {wb.sheetnames}")

        ws = wb[sheet_name]
        month_cols = build_month_column_map(ws)
        if month not in month_cols:
            raise ValueError(f"Month {month} not found in header of '{sheet_name}'. Found months={sorted(month_cols)}")
        month_col = month_cols[month]

        # row index
        if bucket == "べビーリーフ":
            key_col = 2
            row_index = index_company_rows(ws, key_col=key_col, ki=ki)
            agg = (sub[sub["bucket"] == bucket]
                   .groupby("company_proc", as_index=False)["tons"].sum())
            for _, r in agg.iterrows():
                key = norm_text(r["company_proc"])
                if key not in row_index:
                    # not in template; skip (can be enhanced to auto-add)
                    continue
                ws.cell(row_index[key], month_col).value = round(float(r["tons"]), 2)

        else:
            key_col = 2
            row_index = index_company_rows(ws, key_col=key_col, ki=ki)
            agg = (sub[sub["bucket"] == bucket]
                   .groupby("company_proc", as_index=False)["tons"].sum())
            for _, r in agg.iterrows():
                key = company_key_for_report(str(r["company_proc"]))
                key = norm_text(key)
                if key not in row_index:
                    continue
                ws.cell(row_index[key], month_col).value = round(float(r["tons"]), 2)

    wb.save(out_path)


# ----------------------------
# Unmapped output
# ----------------------------

def write_unmapped(out_path: Path, norm: pd.DataFrame):
    unm_comp = sorted(norm.loc[norm["company_proc"].isna(), "company_raw"].dropna().unique().tolist())
    unm_veg = sorted(norm.loc[norm["bucket"] == "不明", "veg_raw"].dropna().unique().tolist())
    sample = norm[(norm["company_proc"].isna()) | (norm["bucket"] == "不明")].head(300)

    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        pd.DataFrame({"unmapped_company_raw": unm_comp}).to_excel(w, index=False, sheet_name="企業名(未変換)")
        pd.DataFrame({"unmapped_veg_raw": unm_veg}).to_excel(w, index=False, sheet_name="野菜名(未分類)")
        sample.to_excel(w, index=False, sheet_name="サンプル行(最大300)")


# ----------------------------
# CLI
# ----------------------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--farmos", type=Path, required=True, help="ファーモス出力Excel")
    p.add_argument("--master", type=Path, required=True, help="正規化マスタExcel（企業名/収穫野菜名）")
    p.add_argument("--harvest-template", type=Path, required=True, help="収穫データ愛川OO期 Excel")
    p.add_argument("--report-template", type=Path, required=True, help="月初報告 Excel")
    p.add_argument("--ki", type=int, required=True, help="期（例: 59）")
    p.add_argument("--month", type=int, required=True, help="対象月（1-12）")
    p.add_argument("--outdir", type=Path, default=Path("out"))
    args = p.parse_args()

    args.outdir.mkdir(parents=True, exist_ok=True)

    masters = load_masters(args.master)
    raw = read_farmos(args.farmos)
    norm = normalize_farmos(raw, masters)

    # outputs
    harvest_out = args.outdir / f"harvest_{args.ki}ki_{args.month}.xlsx"
    report_out  = args.outdir / f"report_{args.ki}ki_{args.month}.xlsx"
    unmapped_out = args.outdir / f"unmapped_{args.ki}ki_{args.month}.xlsx"

    write_harvest(args.harvest_template, harvest_out, norm, args.month)
    write_report(args.report_template, report_out, norm, args.ki, args.month)
    write_unmapped(unmapped_out, norm)

    print("OK")
    print(f"- {harvest_out}")
    print(f"- {report_out}")
    print(f"- {unmapped_out}")


if __name__ == "__main__":
    main()

