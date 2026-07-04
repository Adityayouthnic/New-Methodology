"""
Sales Daily Pipeline — unified processor.

Replaces:
  - Sale_data_generator.py  (Flipkart + OMSguru master consolidator)
  - SOR/SOR_Automated.py    (Myntra SOR enrichment)

Folder layout:
  Source/          — daily sales data (changes every run)
      - Flipkart-*.xlsx / Flipkart-*.csv        → Flipkart sales (FBF sale rows)
      - 3299-*.csv (non-zip)                    → OMSguru order info
      - *Sales_Report_7568*.csv                 → Myntra SOR sales report
      - *Seller_Orders_Report_45833*.csv        → Myntra SJIT (Ethnic Junction)
      - *Seller_Orders_Report_10708*.csv        → Myntra SJIT (VB Export)
      - *ASIN_Manufacturing_Retail_India_Daily*.csv → Cocoblu FC daily (Amazon Vendor)
  Mapping Sheet/   — reference / mapping files (change rarely)
      - 3299-channel_listing_mapping*.zip → Channel listing mapping
      - *Seller_Listings_Report*.csv      → Myntra seller listings (VAN lookup)

  The report-ID numbers above (7568 / 45833 / 10708) are the stable anchors —
  the rest of each filename (dates, prefixes, IDs) can vary run to run.

Output:
  A single merged file is written next to this script (or next to the .exe,
  if run as a frozen build):
      New methodology master - <YYYY-MM-DD>.csv
      (Flipkart + OMSguru master rows, with Myntra SOR/SJIT/Cocoblu rows appended below.)

  After a successful write, each Source file that was actually used gets moved
  into Source/Old/<YYYY-MM-DD>/ (created if missing) so Source stays clean for
  the next run. Files belonging to a section that failed this run are left in
  place so re-running after a fix will pick them up again. Nothing is deleted —
  old batches under Source/Old/ can be cleaned up manually whenever you like.

Run:
    python Newmethodology_format.py
    python Newmethodology_format.py --source ./Source --lookup "./Mapping Sheet" --output .
"""

from __future__ import annotations

import argparse
import io
import logging
import shutil
import sys
import zipfile
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd


# ---------------- Paths ----------------
# When frozen into an .exe (e.g. via PyInstaller), __file__ resolves inside a
# temp extraction dir — anchor to the executable's location instead.
if getattr(sys, "frozen", False):
    BASE_DIR = Path(sys.executable).resolve().parent
else:
    BASE_DIR = Path(__file__).resolve().parent

DEFAULT_SOURCE_DIR = BASE_DIR / "Source"
DEFAULT_LOOKUP_DIR = BASE_DIR / "Mapping Sheet"
DEFAULT_OUTPUT_DIR = BASE_DIR


def final_output_name(run_date: Optional[date] = None) -> str:
    run_date = run_date or date.today()
    return f"New methodology master - {run_date:%Y-%m-%d}.csv"

MASTER_COLUMNS = [
    "Month", "Date", "Product Sku Code", "Listing Sku Code",
    "Channel Name", "Category Name", "Qty", "Total",
]

OUTPUT_DATE_FORMAT = "%d-%m-%Y"


def fill_blanks(df: pd.DataFrame) -> pd.DataFrame:
    """Fill NaN with '' in MASTER_COLUMNS while preserving the Date column dtype."""
    out = df[MASTER_COLUMNS].copy()
    non_date = [c for c in MASTER_COLUMNS if c != "Date"]
    out[non_date] = out[non_date].fillna("")
    return out


def format_output_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Convert the Date column to DD-MM-YYYY strings for the final output."""
    out = df.copy()
    out["Date"] = (
        pd.to_datetime(out["Date"], errors="coerce")
        .dt.strftime(OUTPUT_DATE_FORMAT)
        .fillna("")
    )
    return out


# ---------------- Logging ----------------
def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


# ---------------- Generic helpers ----------------
def clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip().str.replace("\n", " ", regex=True)
    return df


def find_col(df: pd.DataFrame, keyword: str) -> str:
    matches = [c for c in df.columns if keyword.lower() in c.lower()]
    if not matches:
        raise KeyError(
            f"Column containing '{keyword}' not found. Available: {list(df.columns)}"
        )
    return matches[0]


def clean_sku(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.replace(r'^"+', "", regex=True)
        .str.replace(r"^SKU:", "", regex=True)
        .str.replace(r'"+$', "", regex=True)
        .str.strip()
    )


def read_table(path: Path, sheet: Optional[str] = None) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, encoding="utf-8", low_memory=False)
    return pd.read_excel(path, sheet_name=sheet or 0, header=0, engine="openpyxl")


def read_lookup_zip(zip_path: Path, *, required: bool = False) -> Optional[pd.DataFrame]:
    """Read the first CSV inside a zip. Returns None on failure unless required."""
    if not zipfile.is_zipfile(zip_path):
        if required:
            raise ValueError(f"Not a valid ZIP: {zip_path}")
        logging.warning("Skipping non-zip file: %s", zip_path.name)
        return None
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            csv_files = [f for f in z.namelist() if f.lower().endswith(".csv")]
            if not csv_files:
                if required:
                    raise ValueError(f"No CSV inside ZIP: {zip_path}")
                logging.warning("No CSV inside ZIP: %s", zip_path.name)
                return None
            with z.open(csv_files[0]) as f:
                return pd.read_csv(io.BytesIO(f.read()), low_memory=False)
    except zipfile.BadZipFile as exc:
        if required:
            raise
        logging.warning("Unreadable ZIP %s: %s", zip_path.name, exc)
        return None


# ---------------- File discovery ----------------
# Filenames drift run to run (dates, prefixes, IDs) — these report-ID markers
# are the stable part each portal's export always contains, so detection keys
# off them instead of a fixed prefix like "EJSJIT"/"VBSJIT"/"SOR".
SOR_SALES_MARKER = "sales_report_7568"
EJSJIT_MARKER = "seller_orders_report_45833"
VBSJIT_MARKER = "seller_orders_report_10708"
COCOBLU_FC_MARKER = "asin_manufacturing_retail_india_daily"


def discover_sources(source_dir: Path, lookup_dir: Path) -> dict:
    """Sales inputs come from `source_dir`; mapping/reference files from `lookup_dir`."""
    source_files = [p for p in source_dir.iterdir() if p.is_file()]
    lookup_files = [p for p in lookup_dir.iterdir() if p.is_file()] if lookup_dir.is_dir() else []

    flipkart = [
        p for p in source_files
        if p.name.lower().startswith("flipkart")
        and p.suffix.lower() in (".csv", ".xlsx", ".xls")
    ]

    omsguru = [
        p for p in source_files
        if p.name.startswith("3299-")
        and p.suffix.lower() != ".zip"
        and "channel_listing_mapping" not in p.name.lower()
    ]

    sor_sales = next(
        (p for p in source_files
         if SOR_SALES_MARKER in p.name.lower()
         and p.suffix.lower() == ".csv"),
        None,
    )

    ejsjit_orders = next(
        (p for p in source_files
         if EJSJIT_MARKER in p.name.lower()
         and p.suffix.lower() == ".csv"),
        None,
    )

    vbsjit_orders = next(
        (p for p in source_files
         if VBSJIT_MARKER in p.name.lower()
         and p.suffix.lower() == ".csv"),
        None,
    )

    cocoblu_fc = sorted(
        p for p in source_files
        if COCOBLU_FC_MARKER in p.name.lower()
        and p.suffix.lower() == ".csv"
    )

    seller = next(
        (p for p in lookup_files
         if "seller_listings_report" in p.name.lower()
         and p.suffix.lower() == ".csv"),
        None,
    )

    mapping_zip = next(
        (p for p in lookup_files
         if p.suffix.lower() == ".zip"
         and "channel_listing_mapping" in p.name.lower()),
        None,
    )

    return {
        "flipkart": flipkart,
        "omsguru": omsguru,
        "sor_sales": sor_sales,
        "ejsjit_orders": ejsjit_orders,
        "vbsjit_orders": vbsjit_orders,
        "cocoblu_fc": cocoblu_fc,
        "seller": seller,
        "mapping_zip": mapping_zip,
    }


# ---------------- Master marketplace ----------------
def process_flipkart(path: Path) -> pd.DataFrame:
    logging.info("Flipkart: %s", path.name)
    sheet = "Sales Report" if path.suffix.lower() != ".csv" else None
    df = clean_cols(read_table(path, sheet=sheet))

    event_col = find_col(df, "event")
    fulfil_col = find_col(df, "fulfil")
    date_col = find_col(df, "buyer invoice date")
    sku_col = find_col(df, "sku")
    qty_col = find_col(df, "item quantity")
    total_col = find_col(df, "final invoice amount")

    df = df[
        (df[event_col].astype(str).str.lower().str.strip() == "sale")
        & (df[fulfil_col].astype(str).str.lower().str.strip() == "fbf")
    ]

    temp = pd.DataFrame({
        "Month": "",
        "Date": pd.to_datetime(df[date_col], errors="coerce").dt.normalize(),
        "Product Sku Code": "",
        "Listing Sku Code": clean_sku(df[sku_col]),
        "Channel Name": "ETHNIC JUNCTION - Flipkart FBA",
        "Category Name": "",
        "Qty": df[qty_col],
        "Total": df[total_col],
    })

    return temp.groupby(
        ["Date", "Listing Sku Code"], as_index=False, dropna=False
    ).agg({
        "Month": "first", "Product Sku Code": "first",
        "Channel Name": "first", "Category Name": "first",
        "Qty": "sum", "Total": "sum",
    })


def process_omsguru(path: Path) -> pd.DataFrame:
    logging.info("OMSguru: %s", path.name)
    df = clean_cols(read_table(path))

    order_date = find_col(df, "order date")
    prod_sku = find_col(df, "product sku code")
    list_sku = find_col(df, "listing sku code")
    channel = find_col(df, "channel name")
    category = find_col(df, "category name")
    qty = find_col(df, "qty")
    total = find_col(df, "total")

    converted_date = pd.to_datetime(df[order_date], errors="coerce").dt.normalize()

    temp = pd.DataFrame({
        "Month": "",
        "Date": converted_date,
        "Product Sku Code": df[prod_sku],
        "Listing Sku Code": df[list_sku],
        "Channel Name": df[channel],
        "Category Name": df[category],
        "Qty": df[qty],
        "Total": df[total],
    })

    return temp.groupby(
        ["Date", "Listing Sku Code", "Channel Name"], as_index=False
    ).agg({
        "Month": "first", "Product Sku Code": "first",
        "Category Name": "first", "Qty": "sum", "Total": "sum",
    })


def enrich_flipkart_rows(master: pd.DataFrame, mapping_zip: Path) -> pd.DataFrame:
    lookup = read_lookup_zip(mapping_zip)
    if lookup is None:
        return master

    lookup = clean_cols(lookup)
    lookup = lookup[lookup["Channel Name"] == "ETHNIC JUNCTION - Flipkart"]
    lookup = lookup[
        ["Channel Listing SKU Code", "Product SkuCode", "Product Category"]
    ].drop_duplicates(subset=["Channel Listing SKU Code"])

    merged = master.merge(
        lookup,
        left_on="Listing Sku Code",
        right_on="Channel Listing SKU Code",
        how="left",
    )

    merged["Product Sku Code"] = (
        merged["Product Sku Code"].replace("", pd.NA).fillna(merged["Product SkuCode"])
    )
    merged["Category Name"] = (
        merged["Category Name"].replace("", pd.NA).fillna(merged["Product Category"])
    )

    return fill_blanks(merged)


def build_master(sources: dict) -> tuple[Optional[pd.DataFrame], list[Path]]:
    pieces: list[pd.DataFrame] = []
    pieces.extend(process_flipkart(p) for p in sources["flipkart"])
    pieces.extend(process_omsguru(p) for p in sources["omsguru"])

    if not pieces:
        logging.info("No Flipkart/OMSguru inputs — skipping master output.")
        return None, []

    master = fill_blanks(pd.concat(pieces, ignore_index=True))

    if sources["mapping_zip"]:
        master = enrich_flipkart_rows(master, sources["mapping_zip"])
    else:
        logging.info("No mapping ZIP — master output not enriched.")

    used_files = list(sources["flipkart"]) + list(sources["omsguru"])
    return master, used_files


# ---------------- Myntra SOR ----------------
def process_sor(sources: dict) -> tuple[Optional[pd.DataFrame], list[Path]]:
    missing = [
        key for key in ("sor_sales", "seller", "mapping_zip") if not sources[key]
    ]
    if missing:
        logging.info("SOR inputs missing (%s) — skipping Myntra SOR rows.",
                     ", ".join(missing))
        return None, []

    logging.info("SOR sales: %s", sources["sor_sales"].name)
    df = clean_cols(pd.read_csv(sources["sor_sales"], low_memory=False))
    original_qty_total = df["qty"].sum()

    # ord_month is YYYYMMDD
    ord_str = df["ord_month"].astype(str).str.zfill(8)
    df["Date"] = pd.to_datetime(
        ord_str.str[6:8] + "-" + ord_str.str[4:6] + "-" + ord_str.str[0:4],
        format="%d-%m-%Y",
    )

    logging.info("Seller listings: %s", sources["seller"].name)
    seller = clean_cols(pd.read_csv(sources["seller"], low_memory=False))
    seller_lookup = seller[["sku code", "van"]].drop_duplicates(subset=["sku code"])
    df = df.merge(seller_lookup, left_on="sku_code", right_on="sku code", how="left")
    df = df.rename(columns={"van": "Listing Sku Code"})

    logging.info("Mapping ZIP: %s", sources["mapping_zip"].name)
    lookup = read_lookup_zip(sources["mapping_zip"], required=True)
    lookup = clean_cols(lookup)[
        ["Channel Listing SKU Code", "Product SkuCode", "Product Category"]
    ].drop_duplicates(subset=["Channel Listing SKU Code"])

    df = df.merge(
        lookup,
        left_on="Listing Sku Code",
        right_on="Channel Listing SKU Code",
        how="left",
    )

    final = pd.DataFrame({
        "Month": "",
        "Date": df["Date"],
        "Product Sku Code": df["Product SkuCode"],
        "Listing Sku Code": df["Listing Sku Code"],
        "Channel Name": "Myntra-SOR",
        "Category Name": df["Product Category"],
        "Qty": df["qty"],
        "Total": "",
    }).sort_values(by="Date")

    output_qty_total = final["Qty"].sum()
    if original_qty_total != output_qty_total:
        raise ValueError(
            f"SOR qty mismatch — source={original_qty_total}, output={output_qty_total}"
        )
    logging.info("SOR qty verified: %s", output_qty_total)

    return fill_blanks(final), [sources["sor_sales"]]


# ---------------- Myntra SJIT ----------------
SJIT_VARIANTS = (
    {
        "source_key": "ejsjit_orders",
        "label": "EJSJIT",
        "output_channel": "ETHNIC JUNCTION - Myntra-SJIT",
        "lookup_channel": "ETHNIC JUNCTION - Myntra Youthnic",
    },
    {
        "source_key": "vbsjit_orders",
        "label": "VBSJIT",
        "output_channel": "VB EXPORT - Myntra-SJIT",
        "lookup_channel": "VB EXPORT - Myntra PPMP",
    },
)


def process_sjit_file(
    sjit_path: Path,
    mapping_zip: Path,
    output_channel: str,
    lookup_channel: str,
    label: str,
) -> Optional[pd.DataFrame]:
    logging.info("%s orders: %s", label, sjit_path.name)
    df = clean_cols(pd.read_csv(sjit_path, low_memory=False))

    po_col = find_col(df, "po_type")
    df = df[df[po_col].astype(str).str.strip().str.upper() == "SJIT"]
    if df.empty:
        logging.info("%s: no SJIT rows found — skipping.", label)
        return None

    created_col = find_col(df, "created on")
    sku_col = find_col(df, "seller sku code")
    amount_col = find_col(df, "final amount")

    df = df.assign(
        Date=pd.to_datetime(df[created_col], errors="coerce").dt.normalize(),
        **{
            "Listing Sku Code": clean_sku(df[sku_col]),
            "Qty": 1,
            "Total": pd.to_numeric(df[amount_col], errors="coerce").fillna(0),
        },
    )

    logging.info("%s mapping ZIP: %s", label, mapping_zip.name)
    lookup = read_lookup_zip(mapping_zip, required=True)
    lookup = clean_cols(lookup)
    lookup = lookup[lookup["Channel Name"] == lookup_channel]
    lookup = lookup[
        ["Channel Listing SKU Code", "Product SkuCode", "Product Category"]
    ].drop_duplicates(subset=["Channel Listing SKU Code"])

    df = df.merge(
        lookup,
        left_on="Listing Sku Code",
        right_on="Channel Listing SKU Code",
        how="left",
    )

    temp = pd.DataFrame({
        "Month": "",
        "Date": df["Date"],
        "Product Sku Code": df["Product SkuCode"],
        "Listing Sku Code": df["Listing Sku Code"],
        "Channel Name": output_channel,
        "Category Name": df["Product Category"],
        "Qty": df["Qty"],
        "Total": df["Total"],
    })

    grouped = temp.groupby(
        ["Date", "Listing Sku Code", "Channel Name"], as_index=False, dropna=False
    ).agg({
        "Month": "first", "Product Sku Code": "first",
        "Category Name": "first", "Qty": "sum", "Total": "sum",
    })

    return fill_blanks(grouped)


def process_sjit(sources: dict) -> tuple[Optional[pd.DataFrame], list[Path]]:
    if not sources["mapping_zip"]:
        logging.info("Mapping ZIP missing — skipping all SJIT rows.")
        return None, []

    pieces: list[pd.DataFrame] = []
    used_files: list[Path] = []
    for variant in SJIT_VARIANTS:
        sjit_path = sources.get(variant["source_key"])
        if not sjit_path:
            logging.info("%s input missing — skipping.", variant["label"])
            continue
        result = process_sjit_file(
            sjit_path=sjit_path,
            mapping_zip=sources["mapping_zip"],
            output_channel=variant["output_channel"],
            lookup_channel=variant["lookup_channel"],
            label=variant["label"],
        )
        used_files.append(sjit_path)
        if result is not None and not result.empty:
            pieces.append(result)

    if not pieces:
        return None, used_files
    return fill_blanks(pd.concat(pieces, ignore_index=True)), used_files


# ---------------- Cocoblu FC (Amazon Vendor) ----------------
import re

COCOBLU_OUTPUT_CHANNEL = "VB EXPORT - Cocoblu FC"
COCOBLU_LOOKUP_CHANNEL = "VB EXPORT - Cocoblu"
COCOBLU_FC_FILENAME_DATE_RE = re.compile(r"_(\d{1,2})-(\d{1,2})-(\d{4})_")


def _cocoblu_date_from_filename(path: Path) -> Optional[pd.Timestamp]:
    m = COCOBLU_FC_FILENAME_DATE_RE.search(path.name)
    if not m:
        return None
    d, mth, y = (int(x) for x in m.groups())
    try:
        return pd.Timestamp(year=y, month=mth, day=d)
    except (ValueError, OverflowError):
        return None


def _cocoblu_omsguru_qty_by_date_asin(omsguru_paths: list[Path]) -> pd.DataFrame:
    """Return DataFrame[Date, ASIN, OmsQty] for VB EXPORT - Cocoblu rows in OMSguru."""
    frames: list[pd.DataFrame] = []
    for path in omsguru_paths:
        df = clean_cols(read_table(path))
        ch = find_col(df, "channel name")
        df = df[df[ch].astype(str).str.strip() == COCOBLU_LOOKUP_CHANNEL]
        if df.empty:
            continue
        order_date = find_col(df, "order date")
        list_sku = find_col(df, "listing sku code")
        qty = find_col(df, "qty")
        sub = pd.DataFrame({
            "Date": pd.to_datetime(df[order_date], errors="coerce").dt.normalize(),
            "ASIN": clean_sku(df[list_sku]),
            "OmsQty": pd.to_numeric(df[qty], errors="coerce").fillna(0),
        })
        frames.append(sub)
    if not frames:
        return pd.DataFrame(columns=["Date", "ASIN", "OmsQty"])
    combined = pd.concat(frames, ignore_index=True)
    return combined.groupby(["Date", "ASIN"], as_index=False, dropna=False)["OmsQty"].sum()


def process_cocoblu_fc(sources: dict) -> tuple[Optional[pd.DataFrame], list[Path]]:
    fc_files = sources.get("cocoblu_fc") or []
    if not fc_files:
        logging.info("No Cocoblu FC (Sales_ASIN_*) files — skipping.")
        return None, []
    if not sources["mapping_zip"]:
        logging.info("Mapping ZIP missing — skipping Cocoblu FC.")
        return None, []
    if not sources["omsguru"]:
        logging.info("OMSguru missing — cannot compute Cocoblu FC qty (needs OMSguru Cocoblu).")
        return None, []

    # Reject duplicate dates across Sales_ASIN_* files before doing any work.
    by_date: dict[pd.Timestamp, Path] = {}
    for fc_path in fc_files:
        fd = _cocoblu_date_from_filename(fc_path)
        if fd is None:
            continue
        if fd in by_date:
            raise ValueError(
                f"Cocoblu FC: duplicate date {fd.date()} found in two files — "
                f"'{by_date[fd].name}' and '{fc_path.name}'. "
                f"Remove one before re-running."
            )
        by_date[fd] = fc_path

    logging.info("Cocoblu FC: building OMSguru Cocoblu (date, ASIN) qty index")
    oms_qty = _cocoblu_omsguru_qty_by_date_asin(sources["omsguru"])
    cocoblu_dates = set(oms_qty["Date"].dropna().unique())
    if not cocoblu_dates:
        logging.info("Cocoblu FC: OMSguru has no VB EXPORT - Cocoblu rows — skipping all FC files.")
        return None, []
    logging.info("Cocoblu FC: OMSguru covers %d Cocoblu date(s).", len(cocoblu_dates))

    logging.info("Cocoblu FC mapping ZIP: %s", sources["mapping_zip"].name)
    lookup = read_lookup_zip(sources["mapping_zip"], required=True)
    lookup = clean_cols(lookup)
    lookup = lookup[lookup["Channel Name"] == COCOBLU_LOOKUP_CHANNEL]
    lookup = lookup[
        ["Channel Listing SKU Code", "Product SkuCode", "Product Category"]
    ].drop_duplicates(subset=["Channel Listing SKU Code"])

    pieces: list[pd.DataFrame] = []
    used_files: list[Path] = []
    for fc_path in fc_files:
        file_date = _cocoblu_date_from_filename(fc_path)
        if file_date is None:
            logging.warning("Cocoblu FC: cannot parse date from %s — skipping.", fc_path.name)
            continue
        if file_date not in cocoblu_dates:
            logging.info(
                "Cocoblu FC: %s (date=%s) — no OMSguru Cocoblu rows for this date, skipping.",
                fc_path.name, file_date.date(),
            )
            continue
        logging.info("Cocoblu FC: %s (date=%s)", fc_path.name, file_date.date())
        used_files.append(fc_path)

        # First row is metadata banner; second row is the real header.
        df = pd.read_csv(fc_path, skiprows=1, low_memory=False)
        df = clean_cols(df)

        asin_col = find_col(df, "asin")
        ordered_col = find_col(df, "ordered units")

        df = pd.DataFrame({
            "ASIN": clean_sku(df[asin_col]),
            "Ordered": pd.to_numeric(df[ordered_col], errors="coerce").fillna(0),
        })
        df = df[df["Ordered"] > 0]
        if df.empty:
            continue

        df = df.groupby("ASIN", as_index=False)["Ordered"].sum()
        df["Date"] = file_date

        merged = df.merge(
            oms_qty[oms_qty["Date"] == file_date][["ASIN", "OmsQty"]],
            on="ASIN", how="left",
        )
        merged["OmsQty"] = merged["OmsQty"].fillna(0)
        merged["FCQty"] = merged["Ordered"] - merged["OmsQty"]
        merged = merged[merged["FCQty"] > 0]
        if merged.empty:
            continue

        merged = merged.merge(
            lookup,
            left_on="ASIN", right_on="Channel Listing SKU Code", how="left",
        )

        out = pd.DataFrame({
            "Month": "",
            "Date": merged["Date"],
            "Product Sku Code": merged["Product SkuCode"],
            "Listing Sku Code": merged["ASIN"],
            "Channel Name": COCOBLU_OUTPUT_CHANNEL,
            "Category Name": merged["Product Category"],
            "Qty": merged["FCQty"],
            "Total": "",
        })
        pieces.append(out)

    if not pieces:
        logging.info("Cocoblu FC: no positive-FC rows after subtraction — nothing to add.")
        return None, used_files

    combined = pd.concat(pieces, ignore_index=True)
    grouped = combined.groupby(
        ["Date", "Listing Sku Code", "Channel Name"], as_index=False, dropna=False
    ).agg({
        "Month": "first", "Product Sku Code": "first",
        "Category Name": "first", "Qty": "sum", "Total": "first",
    })
    return fill_blanks(grouped), used_files


SOURCE_README_TEXT = """\
This "Source" folder was auto-created because it was missing.

Drop today's daily sales-data files in here, then re-run the pipeline.
The report-ID numbers below are what the script looks for — the rest of
each filename (dates, prefixes, IDs) can vary freely.

Required files:

  1. Flipkart sales
     Filename starts with "Flipkart", extension .csv/.xlsx/.xls
     (if .xlsx, the sheet must be named "Sales Report")

  2. OMSguru order info
     Filename starts with "3299-" (and is not the mapping ZIP)

  3. Myntra SOR sales report
     Filename contains "Sales_Report_7568", extension .csv

  4. Myntra SJIT — Ethnic Junction
     Filename contains "Seller_Orders_Report_45833", extension .csv

  5. Myntra SJIT — VB Export
     Filename contains "Seller_Orders_Report_10708", extension .csv

  6. Cocoblu FC (Amazon Vendor) daily
     Filename contains "ASIN_Manufacturing_Retail_India_Daily", extension .csv
     Filename must include a date like "_27-6-2026_" (one file per day)

Reference/mapping files (channel listing mapping ZIP, Myntra seller
listings report) go in the "Mapping Sheet" folder instead, not here.

After each successful run, files that were used get moved into
Source/Old/<run-date>/ automatically, so this folder stays clean for the
next day. Old batches are never auto-deleted — clean them up manually
whenever you like.
"""


MAPPING_SHEET_README_TEXT = """\
This "Mapping Sheet" folder was auto-created because it was missing.

Drop the reference/mapping files in here, then re-run the pipeline.
These files change rarely — update them only when the mapping itself changes.

Required files:

  1. Channel listing mapping
     Filename contains "channel_listing_mapping", extension .zip
     (a ZIP containing one CSV with columns: Channel Name,
     Channel Listing SKU Code, Product SkuCode, Product Category)

  2. Myntra seller listings report
     Filename contains "seller_listings_report", extension .csv
     (columns: sku code, van)

Without these files, Flipkart/OMSguru rows won't be enriched with
Product Sku Code / Category, and Myntra SOR, Myntra SJIT, and Cocoblu FC
rows will all be skipped.

Daily sales-data files go in the "Source" folder instead, not here.
"""


def _create_folder_with_readme(folder: Path, readme_text: str) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "README.txt").write_text(readme_text, encoding="utf-8")


def create_source_folder_with_readme(source_dir: Path) -> None:
    _create_folder_with_readme(source_dir, SOURCE_README_TEXT)


def create_mapping_sheet_folder_with_readme(lookup_dir: Path) -> None:
    _create_folder_with_readme(lookup_dir, MAPPING_SHEET_README_TEXT)


def archive_used_files(source_dir: Path, used_files: list[Path], run_date: date) -> None:
    """Move successfully processed Source files into Source/Old/<run_date>/."""
    existing = [p for p in used_files if p.exists()]
    if not existing:
        return
    old_dir = source_dir / "Old" / f"{run_date:%Y-%m-%d}"
    old_dir.mkdir(parents=True, exist_ok=True)
    for path in existing:
        dest = old_dir / path.name
        if dest.exists():
            dest = old_dir / f"{path.stem}__{datetime.now():%H%M%S}{path.suffix}"
        shutil.move(str(path), str(dest))
        logging.info("Archived: %s -> Source/Old/%s/%s", path.name, run_date, dest.name)


# ---------------- Entry point ----------------
def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sales Daily unified pipeline")
    parser.add_argument(
        "--source", type=Path, default=DEFAULT_SOURCE_DIR,
        help=f"Daily sales-data folder (default: {DEFAULT_SOURCE_DIR})",
    )
    parser.add_argument(
        "--lookup", type=Path, default=DEFAULT_LOOKUP_DIR,
        help=f"Lookup/mapping folder (default: {DEFAULT_LOOKUP_DIR})",
    )
    parser.add_argument(
        "--output", type=Path, default=DEFAULT_OUTPUT_DIR,
        help=f"Output folder (default: {DEFAULT_OUTPUT_DIR})",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    setup_logging()
    args = parse_args(argv)

    source_dir: Path = args.source.resolve()
    lookup_dir: Path = args.lookup.resolve()
    output_dir: Path = args.output.resolve()

    logging.info("Source: %s", source_dir)
    logging.info("Lookup: %s", lookup_dir)
    logging.info("Output: %s", output_dir)

    if not source_dir.is_dir():
        create_source_folder_with_readme(source_dir)
        logging.error(
            "Source folder was missing — created it at %s with a README.txt "
            "listing the required files. Add today's files there and re-run.",
            source_dir,
        )
        return 1
    if not lookup_dir.is_dir():
        create_mapping_sheet_folder_with_readme(lookup_dir)
        logging.warning(
            "Mapping Sheet folder was missing — created it at %s with a README.txt "
            "listing required files. Flipkart/OMSguru enrichment, Myntra SOR, Myntra "
            "SJIT, and Cocoblu FC will all be skipped until those files are added.",
            lookup_dir,
        )
    output_dir.mkdir(parents=True, exist_ok=True)

    sources = discover_sources(source_dir, lookup_dir)
    for key, val in sources.items():
        shown = [p.name for p in val] if isinstance(val, list) else (val.name if val else None)
        logging.info("Discovered %-12s : %s", key, shown)

    exit_code = 0
    master_df: Optional[pd.DataFrame] = None
    sor_df: Optional[pd.DataFrame] = None
    sjit_df: Optional[pd.DataFrame] = None
    cocoblu_fc_df: Optional[pd.DataFrame] = None
    used_files: list[Path] = []

    # --- Master marketplace consolidation (Flipkart + OMSguru) ---
    try:
        master_df, master_used = build_master(sources)
        used_files.extend(master_used)
    except Exception:
        logging.exception("Master marketplace build failed")
        exit_code = 1

    # --- Myntra SOR (returns rows already shaped to MASTER_COLUMNS) ---
    try:
        sor_df, sor_used = process_sor(sources)
        used_files.extend(sor_used)
    except Exception:
        logging.exception("Myntra SOR build failed")
        exit_code = 1

    # --- Myntra SJIT (returns rows already shaped to MASTER_COLUMNS) ---
    try:
        sjit_df, sjit_used = process_sjit(sources)
        used_files.extend(sjit_used)
    except Exception:
        logging.exception("Myntra SJIT build failed")
        exit_code = 1

    # --- Cocoblu FC (Amazon Vendor leftover after OMSguru Cocoblu) ---
    try:
        cocoblu_fc_df, cocoblu_used = process_cocoblu_fc(sources)
        used_files.extend(cocoblu_used)
    except Exception:
        logging.exception("Cocoblu FC build failed")
        exit_code = 1

    # --- Merge & write the single final file ---
    pieces = [df for df in (master_df, sor_df, sjit_df, cocoblu_fc_df) if df is not None and not df.empty]
    if not pieces:
        logging.error("No data produced — nothing to write.")
        return max(exit_code, 1)

    master_rows = len(master_df) if master_df is not None else 0
    sor_rows = len(sor_df) if sor_df is not None else 0
    sjit_rows = len(sjit_df) if sjit_df is not None else 0
    cocoblu_rows = len(cocoblu_fc_df) if cocoblu_fc_df is not None else 0
    logging.info(
        "Merging datasets — master=%d rows, myntra_sor=%d rows, myntra_sjit=%d rows, cocoblu_fc=%d rows",
        master_rows, sor_rows, sjit_rows, cocoblu_rows,
    )

    combined = format_output_dates(
        fill_blanks(pd.concat(pieces, ignore_index=True))
    )

    final_path = output_dir / final_output_name()
    combined.to_csv(final_path, index=False)
    logging.info("Final output written: %s (%d rows)", final_path.name, len(combined))

    archive_used_files(source_dir, used_files, date.today())

    logging.info("Done (exit=%d)", exit_code)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
