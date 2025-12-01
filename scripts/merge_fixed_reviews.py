# scripts/merge_fixed_reviews.py
import os
import pandas as pd
import shutil
from html import unescape  # robust HTML entity decoding

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "../data/raw")
PROC_DIR = os.path.join(BASE_DIR, "../data/processed")
FIXES_PATH = os.path.join(RAW_DIR, "metacritic_missing_fixed_reviews.csv")

COLS = ["movie_title","release_year","metascore","critic_publication","critic_author","critic_score"]

def norm(s):
    """Decode HTML entities, collapse whitespace, lowercase."""
    if pd.isna(s):
        return ""
    s = unescape(str(s))
    return " ".join(s.split()).lower()

def movie_key(df: pd.DataFrame) -> pd.Series:
    return df["movie_title"].map(norm) + "|" + pd.to_numeric(df["release_year"], errors="coerce").astype("Int64").astype(str)

def review_key(df: pd.DataFrame) -> pd.Series:
    score_str = df["critic_score"].astype("Int64").astype("string").fillna("")
    return (
        df["movie_title"].map(norm) + "|" +
        pd.to_numeric(df["release_year"], errors="coerce").astype("Int64").astype(str) + "|" +
        df["critic_publication"].map(norm) + "|" +
        df["critic_author"].map(norm) + "|" +
        score_str
    )

def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = df[COLS].copy()
    for c in ["release_year","metascore","critic_score"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").round().astype("Int64")
    return df

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def main():
    ensure_dir(PROC_DIR)

    # If fix-file is missing, copy all raw files as-is to processed/
    if not os.path.isfile(FIXES_PATH):
        print(f"No fixes file found at {FIXES_PATH}. Copying all raw files to processed...")
        for f in os.listdir(RAW_DIR):
            if f.startswith("metacritic_movies_") and f.endswith(".csv"):
                shutil.copyfile(os.path.join(RAW_DIR, f), os.path.join(PROC_DIR, f))
                print(f"Copied: {f}")
        return

    fixes = load_csv(FIXES_PATH)
    years_with_fixes = sorted(pd.to_numeric(fixes["release_year"], errors="coerce").dropna().astype(int).unique().tolist())

    # Find all yearly raw files
    all_files = [f for f in os.listdir(RAW_DIR) if f.startswith("metacritic_movies_") and f.endswith(".csv")]
    all_years = sorted(int(f.split("_")[-1].split(".")[0]) for f in all_files)

    for year in all_years:
        base_path = os.path.join(RAW_DIR, f"metacritic_movies_{year}.csv")
        out_path  = os.path.join(PROC_DIR, f"metacritic_movies_{year}.csv")

        # No fixes for this year → copy directly
        if year not in years_with_fixes:
            shutil.copyfile(base_path, out_path)
            print(f"[{year}] No fixes → copied raw file to processed.")
            continue

        # Merge for this year
        year_fix = fixes[fixes["release_year"].astype("Int64") == year].copy()
        base = load_csv(base_path)

        base["_mkey"], base["_rkey"] = movie_key(base), review_key(base)
        year_fix["_mkey"], year_fix["_rkey"] = movie_key(year_fix), review_key(year_fix)

        # 1) Backfill metascore (if NaN or 0) from fixes
        fix_meta = year_fix.groupby("_mkey")["metascore"].max()
        base_valid = base["metascore"].between(1, 100)
        base["metascore"] = base["metascore"].where(base_valid, base["_mkey"].map(fix_meta))

        # 2) Remove placeholder rows (no pub, no author, no score)
        mask_empty = (
            base["critic_publication"].fillna("").eq("") &
            base["critic_author"].fillna("").eq("") &
            base["critic_score"].isna()   # <— fixed: no fillna("") on Int64
        )
        base = base.loc[~mask_empty, :].copy()

        # 3) Append only new critic rows from fixes
        existing = set(base["_rkey"])
        new_rows = year_fix.loc[~year_fix["_rkey"].isin(existing), COLS].copy()
        merged = pd.concat([base[COLS], new_rows], ignore_index=True)

        # 4) Enforce correct year
        merged["release_year"] = year

        # Ensure integer columns remain integers (no decimals in CSV)
        for c in ["release_year","metascore","critic_score"]:
            merged[c] = pd.to_numeric(merged[c], errors="coerce").round().astype("Int64")

        merged.to_csv(out_path, index=False)
        print(f"[{year}] Added {len(new_rows)} new rows → {out_path}")

if __name__ == "__main__":
    main()
