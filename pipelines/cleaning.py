from scipy import stats
import pandas as pd
import numpy as np
import json

# ─────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────

YOUTUBE_EXTRA_LANGS = {
    "yue", "yue-hk", "bh", "bho", "mai", "sat", "bgc",
    "chr", "mni", "vro", "ase", "mo", "bi", "und", "zxx", "sdp"
}

COLUMNS_TO_DROP = [
    "thumbnail_link", "chapters", "cards", "badge_labels",
    "contentDetails.contentRating.ytRating",
    "contentDetails.regionRestriction.allowed",
    "contentDetails.regionRestriction.blocked",
    "trending_date", "favoriteCount"
]

INT_COLUMNS  = ["view_count", "likes", "categoryId", "comment_count", "card_count", "is_trending", "chapter_count"]
BOOL_COLUMNS = ["embeddable", "madeForKids", "supports_miniplayer", "is_verified", "has_paid_promotion", "comments_disabled"]
LOG_COLUMNS  = ["view_count", "likes", "comment_count"]
CAP_COLUMNS  = ["view_count", "likes", "comment_count"]

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def extract_hl_list_from_file(file_path):
    """Load valid language codes from a YouTube hl_list JSON file."""
    with open(file_path, "r", encoding="utf-8") as f:
        json_data = json.load(f)
    return {
        item["snippet"]["hl"].split("-")[0].lower()
        for item in json_data.get("items", [])
    }


def process_tags(x):
    """Normalize tags to a list regardless of input type."""
    if isinstance(x, list):
        return x
    if pd.isna(x):
        return []
    if isinstance(x, str):
        x = x.strip().strip("[]")
        return [tag.strip() for tag in x.split(",") if tag.strip()]
    return []

# ─────────────────────────────────────────────
# Cleaning Steps
# ─────────────────────────────────────────────

def remove_duplicates(df):
    """Remove duplicate rows and duplicate video IDs."""
    return df.drop_duplicates().drop_duplicates(subset=["video_id"])


def filter_invalid_rows(df):
    """Remove rows with logically inconsistent values."""
    comment_count = pd.to_numeric(df["comment_count"], errors="coerce")

    comments_disabled = df["comments_disabled"].apply(lambda x: str(x).lower() == "true")

    df = df[df["likes"] <= df["view_count"]]

    valid_comments = (
        comment_count.isna() |
        ((comment_count >= 0) & ~comments_disabled) |
        ((comment_count == 0) &  comments_disabled)
    )
    return df[valid_comments]


def fix_comment_count(df):
    """Fill missing comment_count with 0 where comments are disabled."""
    mask = df["comment_count"].isna() & (df["comments_disabled"].apply(lambda x: str(x).lower()) == "true")
    df.loc[mask, "comment_count"] = 0
    return df


def cast_types(df):
    """Cast columns to their correct data types."""
    for col in INT_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in BOOL_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: str(x).lower() == "true")

    return df


def clean_default_language(df, hl_file_path="data/youtube/hl_list.json"):
    """Replace invalid defaultLanguage values with 'unknown'."""
    if "defaultLanguage" not in df.columns:
        return df

    hl_set = extract_hl_list_from_file(hl_file_path) | YOUTUBE_EXTRA_LANGS
    series = df["defaultLanguage"].dropna()
    invalid_mask = ~series.str.split("-").str[0].str.lower().isin(hl_set)
    df.loc[invalid_mask.index[invalid_mask], "defaultLanguage"] = "unknown"
    return df


def apply_log_transformation(df, columns, base="natural"):
    """Apply log(x+1) transformation to handle zeros. Clips negatives to 0 first."""
    for col in columns:
        if col not in df.columns:
            print(f"Column '{col}' not found, skipping.")
            continue
        df[col] = df[col].clip(lower=0)
        if base == "natural":
            df[col] = np.log1p(df[col])
        elif base == "log2":
            df[col] = np.log2(df[col] + 1)
        elif base == "log10":
            df[col] = np.log10(df[col] + 1)
    return df


def apply_sqrt_transformation(df, columns):
    """Apply sqrt transformation. Clips negatives to 0 first."""
    for col in columns:
        if col not in df.columns:
            print(f"Column '{col}' not found, skipping.")
            continue
        df[col] = df[col].clip(lower=0)
        df[col] = np.sqrt(df[col])
    return df


def cap_outliers(df, columns, method="iqr", iqr_multiplier=1.5, z_threshold=3):
    """Cap outliers using IQR or Z-score method."""
    for col in columns:
        if col not in df.columns:
            print(f"Column '{col}' not found, skipping.")
            continue

        if method == "iqr":
            Q1, Q3 = df[col].quantile(0.25), df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_cap = Q1 - iqr_multiplier * IQR
            upper_cap = Q3 + iqr_multiplier * IQR
        elif method == "zscore":
            mean, std = df[col].mean(), df[col].std()
            lower_cap = mean - z_threshold * std
            upper_cap = mean + z_threshold * std
        else:
            print(f"Unknown method '{method}', use 'iqr' or 'zscore'.")
            continue

        df[col] = df[col].clip(lower=lower_cap, upper=upper_cap)
        print(f"── {col} capped at [{lower_cap:.2f}, {upper_cap:.2f}] using {method}")

    return df

# ─────────────────────────────────────────────
# Main Pipeline
# ─────────────────────────────────────────────

def cleaning(df):
    df = remove_duplicates(df)
    df = filter_invalid_rows(df)
    df = df.drop(columns=[c for c in COLUMNS_TO_DROP if c in df.columns])
    df["tags"] = df["tags"].apply(process_tags)
    df["description"] = df["description"].fillna("").astype(str)
    df = fix_comment_count(df)
    df = df.dropna()
    df = cast_types(df)
    df = clean_default_language(df)
    df = apply_log_transformation(df, columns=LOG_COLUMNS, base="natural")
    df = cap_outliers(df, columns=CAP_COLUMNS, method="zscore")
    return df


# ─────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────

if __name__ == "__main__":
    df = pd.read_csv("data/youtube/dataset.csv")
    cleaned_df = cleaning(df)
    cleaned_df.to_csv("data/youtube/cleaned_dataset.csv", index=False)
    print(f"Done. Shape: {cleaned_df.shape}")