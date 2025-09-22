"""
Online Library – Synthetic Event Stream Generator (~100 MB CSV)

Notes:
- Uses Zipf-like sampling for user_id and book_id to create popularity skew.
- Groups events into sessions (geometric length) so a session_id appears across multiple rows for the same user.
- event_metadata varies by event_type and is stored as a JSON string (Snowflake VARIANT-like).
"""

import os
import uuid
import json
from io import StringIO
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
from faker import Faker


TARGET_SIZE_MB = 100
TARGET_BYTES = TARGET_SIZE_MB * 1024 * 1024

USER_POOL_SIZE = 10_000
BOOK_POOL_SIZE = 5_000

EVENT_TYPES = np.array(["read_online", "download", "review", "search"])
EVENT_PROBS = np.array([0.45, 0.15, 0.05, 0.35])

PLATFORMS = np.array(["web_browser", "mobile_app"])
PLATFORM_PROBS = np.array([0.6, 0.4])

DL_FORMATS = np.array(["epub", "pdf", "mobi", "audiobook"])
DL_FORMAT_PROBS = np.array([0.5, 0.3, 0.15, 0.05])

DEVICE_OSES = np.array(["iOS", "Android", "Windows", "macOS", "Linux"])
DEVICE_OS_PROBS = np.array([0.35, 0.45, 0.08, 0.09, 0.03])

RATINGS = np.array([1, 2, 3, 4, 5])
RATING_PROBS = np.array([0.05, 0.10, 0.25, 0.35, 0.25])

SEARCH_VOCAB = np.array(
    [
        "dystopian",
        "sci-fi",
        "fantasy",
        "mystery",
        "romance",
        "historical",
        "self-help",
        "philosophy",
        "classic",
        "thriller",
        "nonfiction",
        "biography",
        "young-adult",
        "horror",
        "poetry",
        "business",
        "economics",
        "psychology",
        "productivity",
        "data-science",
        "machine-learning",
        "ai",
        "programming",
        "python",
        "java",
        "networks",
        "security",
        "cooking",
        "travel",
        "photography",
        "art",
        "music",
        "health",
        "fitness",
        "mindfulness",
        "education",
        "children",
        "graphic-novel",
    ]
)

GEOM_P = 0.3
MAX_SESSION_LEN = 20

DEFAULT_CHUNK_ROWS = 200_000

SEED = 42
np.random.seed(SEED)
Faker.seed(SEED)
fake = Faker()


def _now_utc():
    return datetime.now(timezone.utc)


NOW = _now_utc()
START = NOW - timedelta(days=730)
START_TS = int(START.timestamp())
END_TS = int(NOW.timestamp())

USER_ID_MAP = np.random.permutation(np.arange(1, USER_POOL_SIZE + 1))
BOOK_ID_MAP = np.random.permutation(np.arange(1, BOOK_POOL_SIZE + 1))


def rand_ts_last_two_years() -> datetime:
    """Uniform random timestamp in the last 2 years (UTC)."""
    ts = np.random.randint(START_TS, END_TS + 1)
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def draw_zipf_id(max_id: int, a: float, mapping: np.ndarray) -> int:
    """
    Draw a rank from Zipf(a), clip to max_id, then map that rank to an ID via a permutation.
    Produces a popularity-skewed but nontrivial id distribution.
    """
    r = np.random.zipf(a)
    while r > max_id:
        r = np.random.zipf(a)
    return int(mapping[r - 1])


def draw_session_lengths(
    n_target: int, p: float = GEOM_P, max_len: int = MAX_SESSION_LEN
):
    """
    Keep drawing geometric lengths until we meet/exceed n_target events.
    Geometric(k) ~ number of trials to first success; bounded by max_len for realism.
    """
    lengths = []
    total = 0
    while total < n_target:
        lenght = int(np.random.geometric(p))
        if lenght > max_len:
            lenght = max_len
        lengths.append(lenght)
        total += lenght
    return lengths, total


def generate_chunk(n_events: int) -> pd.DataFrame:
    """
    Generate a chunk of events with session structure and type-specific event_metadata.
    """
    cols = {
        "event_id": [],
        "event_timestamp": [],
        "event_type": [],
        "user_id": [],
        "book_id": [],
        "session_id": [],
        "event_metadata": [],
    }

    sess_lengths, _ = draw_session_lengths(n_events)
    for slen in sess_lengths:
        session_uuid = str(uuid.uuid4())
        user_id = draw_zipf_id(USER_POOL_SIZE, a=1.25, mapping=USER_ID_MAP)
        current_ts = rand_ts_last_two_years()

        etypes = np.random.choice(EVENT_TYPES, size=slen, p=EVENT_PROBS)

        for et in etypes:
            cols["event_id"].append(str(uuid.uuid4()))
            cols["event_timestamp"].append(current_ts.strftime("%Y-%m-%dT%H:%M:%SZ"))
            cols["event_type"].append(et)
            cols["user_id"].append(user_id)
            cols["book_id"].append(
                draw_zipf_id(BOOK_POOL_SIZE, a=1.35, mapping=BOOK_ID_MAP)
            )
            cols["session_id"].append(session_uuid)

            if et == "read_online":
                duration = int(np.clip(np.random.gamma(shape=2.0, scale=20.0), 1, 300))
                platform = str(np.random.choice(PLATFORMS, p=PLATFORM_PROBS))
                meta_obj = {"reading_duration_minutes": duration, "platform": platform}

            elif et == "download":
                fmt = str(np.random.choice(DL_FORMATS, p=DL_FORMAT_PROBS))
                os_name = str(np.random.choice(DEVICE_OSES, p=DEVICE_OS_PROBS))
                app_version = f"{np.random.randint(1, 4)}.{np.random.randint(0, 10)}.{np.random.randint(0, 10)}"
                meta_obj = {
                    "format": fmt,
                    "device_os": os_name,
                    "app_version": app_version,
                }

            elif et == "review":
                rating = int(np.random.choice(RATINGS, p=RATING_PROBS))
                review_text = fake.sentence(nb_words=int(np.random.randint(8, 20)))
                meta_obj = {"rating": rating, "review_text": review_text}

            else:
                k = int(np.random.randint(1, 4))
                terms = list(np.random.choice(SEARCH_VOCAB, size=k, replace=False))
                results_count = int(np.clip(np.random.poisson(lam=12), 0, 500))
                meta_obj = {"search_terms": terms, "results_count": results_count}

            cols["event_metadata"].append(
                json.dumps(meta_obj, separators=(",", ":"), ensure_ascii=False)
            )

            current_ts = current_ts + timedelta(
                seconds=int(np.random.randint(15, 1801))
            )
            if current_ts > NOW:
                current_ts = NOW

    df = pd.DataFrame(cols)

    if len(df) > n_events:
        df = df.iloc[:n_events].copy()
    return df


def estimate_avg_row_bytes(n_sample: int = 2000) -> float:
    """
    Generate a small sample to estimate average bytes per CSV row (no header).
    Used to compute the total number of rows to hit ~100 MB.
    """
    df = generate_chunk(n_sample)
    buf = StringIO()

    df.to_csv(buf, index=False, header=False)
    data = buf.getvalue().encode("utf-8")
    return len(data) / max(1, len(df))


def main():
    out_path = "data/online_library_events.csv"

    if os.path.exists(out_path):
        os.remove(out_path)

    avg_row_bytes = estimate_avg_row_bytes(n_sample=2000)
    target_rows = int(TARGET_BYTES / avg_row_bytes)

    rows_written = 0
    first_write = True

    while rows_written < target_rows:
        remaining = target_rows - rows_written
        n_chunk = min(DEFAULT_CHUNK_ROWS, remaining)
        df = generate_chunk(n_chunk)
        df.to_csv(
            out_path,
            index=False,
            mode="w" if first_write else "a",
            header=first_write,
        )
        rows_written += len(df)
        first_write = False

    final_size = os.path.getsize(out_path)
    if final_size < TARGET_BYTES:
        extra_rows = int((TARGET_BYTES - final_size) / avg_row_bytes) + 1
        if extra_rows > 0:
            df = generate_chunk(extra_rows)
            df.to_csv(out_path, index=False, mode="a", header=False)
            rows_written += len(df)
            final_size = os.path.getsize(out_path)

    print(
        f"Wrote {rows_written:,} rows to {out_path} "
        f"({final_size / 1024 / 1024:.2f} MB)."
    )


if __name__ == "__main__":
    main()
