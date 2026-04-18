"""
Market News — Overview Row 3 Card 3

Aggregates news from Investing.com India RSS feeds. Dedupes by URL,
sorts by recency, optionally classifies sentiment via Groq LLM.

Feeds used (all free, no auth):
- news.rss              — all news
- news_25.rss           — stock market news
- news_14.rss           — economic news
- news_11.rss           — commodities news

Cache: 10 min in-memory.
Sentiment: optional Groq classification, 30-min extra cache on top.
"""
import logging
import time
import json
import hashlib
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
DB_PATH = Path(__file__).parent / "breadth_data.db"

# In-memory caches
_news_cache: Dict[str, Any] = {"data": None, "fetched_at": 0}
_sentiment_cache: Dict[str, Dict[str, Any]] = {}  # headline_hash -> {label, cached_at}

NEWS_CACHE_TTL = 600           # 10 min
SENTIMENT_CACHE_TTL = 1800     # 30 min

# Category → RSS feeds. Multiple per category for coverage.
FEED_CATALOG = {
    "all": [
        ("Stock Market", "https://in.investing.com/rss/news_25.rss"),
        ("Economy",      "https://in.investing.com/rss/news_14.rss"),
        ("Commodities",  "https://in.investing.com/rss/news_11.rss"),
        ("Latest",       "https://in.investing.com/rss/news_477.rss"),
    ],
    "stocks": [
        ("Stock Market", "https://in.investing.com/rss/news_25.rss"),
        ("Earnings",     "https://in.investing.com/rss/news_1062.rss"),
    ],
    "macro": [
        ("Economy",      "https://in.investing.com/rss/news_14.rss"),
        ("Indicators",   "https://in.investing.com/rss/news_95.rss"),
        ("Commodities",  "https://in.investing.com/rss/news_11.rss"),
    ],
}

MAX_ITEMS_PER_FEED = 8
MAX_ITEMS_RETURNED = 12


def _get_groq_key() -> str:
    """Reuse the Groq API key from ai_insights' DB settings."""
    try:
        import sqlite3
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS app_settings "
            "(key TEXT PRIMARY KEY, value TEXT, updated TEXT)"
        )
        row = conn.execute(
            "SELECT value FROM app_settings WHERE key='groq_api_key'"
        ).fetchone()
        conn.close()
        return row[0].strip() if row and row[0] else ""
    except Exception as e:
        logger.warning(f"[news] could not load Groq key: {e}")
        return ""


def _fetch_single_feed(source_label: str, url: str) -> List[Dict[str, Any]]:
    """Fetch one RSS feed. Returns list of news items or [] on failure."""
    try:
        import feedparser
        # feedparser handles User-Agent + gzip etc. by itself
        d = feedparser.parse(url, request_headers={
            "User-Agent": "Mozilla/5.0 (compatible; QB360/1.0)"
        })
        if d.bozo and not d.entries:
            logger.warning(f"[news] feed {url} parsed with error: {d.bozo_exception}")
            return []
        items = []
        for entry in d.entries[:MAX_ITEMS_PER_FEED]:
            title = (entry.get("title") or "").strip()
            link = (entry.get("link") or "").strip()
            if not title or not link:
                continue
            # Parse pubDate → epoch for sorting
            pub_epoch = 0
            if entry.get("published_parsed"):
                pub_epoch = int(time.mktime(entry.published_parsed))
            elif entry.get("updated_parsed"):
                pub_epoch = int(time.mktime(entry.updated_parsed))
            items.append({
                "title": title,
                "link": link,
                "source": source_label,
                "published_epoch": pub_epoch,
                "summary": _strip_html(entry.get("summary", ""))[:200],
            })
        return items
    except Exception as e:
        logger.warning(f"[news] fetch failed {url}: {e}")
        return []


def _strip_html(s: str) -> str:
    """Remove HTML tags from summary text."""
    if not s:
        return ""
    return re.sub(r"<[^>]+>", "", s).strip()


def _dedupe_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Dedupe by URL, keeping the first occurrence."""
    seen = set()
    out = []
    for it in items:
        url = it["link"].split("?")[0]  # strip query strings for better dedup
        if url in seen:
            continue
        seen.add(url)
        out.append(it)
    return out


def _time_ago(epoch: int) -> str:
    if not epoch:
        return ""
    now = int(time.time())
    diff = now - epoch
    if diff < 0:
        return "just now"
    if diff < 60:
        return f"{diff}s ago"
    if diff < 3600:
        return f"{diff // 60}m ago"
    if diff < 86400:
        return f"{diff // 3600}h ago"
    return f"{diff // 86400}d ago"


def _hash_headline(title: str) -> str:
    return hashlib.md5(title.strip().lower().encode("utf-8")).hexdigest()


def _classify_sentiment_batch(titles: List[str], api_key: str) -> Dict[str, str]:
    """
    Classify up to ~20 headlines in one Groq call.
    Returns dict: {title: "BULL" | "BEAR" | "NEUTRAL"}.
    Failures return NEUTRAL for everything — never breaks the news feed.
    """
    if not api_key or not titles:
        return {t: "NEUTRAL" for t in titles}

    # Build numbered list for LLM
    numbered = "\n".join(f"{i+1}. {t}" for i, t in enumerate(titles))
    system = (
        "You are a financial news sentiment classifier for Indian equity markets. "
        "Output strict JSON only — no prose, no markdown."
    )
    user = f"""Classify each of the following headlines as BULL (positive for Indian stocks / risk-on), BEAR (negative / risk-off), or NEUTRAL (unclear or mixed).

Rules:
- Earnings beat, upgrades, growth, rate cuts, FII inflows → BULL
- Earnings miss, downgrades, contraction, rate hikes, FII outflows, geopolitical tension → BEAR
- Mixed signals, macro explainers without directional bias → NEUTRAL

Headlines:
{numbered}

Output format (JSON array of uppercase strings, same order, same length):
["BULL","NEUTRAL","BEAR",...]"""

    try:
        import httpx
        resp = httpx.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user",   "content": user},
                ],
                "max_tokens": 300,
                "temperature": 0.1,
                "stream": False,
            },
            timeout=20,
        )
        if resp.status_code != 200:
            logger.warning(f"[news] Groq sentiment HTTP {resp.status_code}")
            return {t: "NEUTRAL" for t in titles}
        content = resp.json()["choices"][0]["message"]["content"].strip()
        # Strip <think> if present
        content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL).strip()
        # Strip code-fence markdown if present
        content = re.sub(r"^```(?:json)?\s*|\s*```$", "", content, flags=re.MULTILINE).strip()
        # Extract first JSON array
        m = re.search(r"\[.*\]", content, flags=re.DOTALL)
        if not m:
            logger.warning("[news] Groq sentiment: no JSON array in response")
            return {t: "NEUTRAL" for t in titles}
        labels = json.loads(m.group(0))
        # Pad/truncate to match
        if len(labels) < len(titles):
            labels = labels + ["NEUTRAL"] * (len(titles) - len(labels))
        labels = labels[:len(titles)]
        # Normalize
        out = {}
        for t, label in zip(titles, labels):
            label_norm = str(label).upper().strip()
            if label_norm not in ("BULL", "BEAR", "NEUTRAL"):
                label_norm = "NEUTRAL"
            out[t] = label_norm
        return out
    except Exception as e:
        logger.warning(f"[news] Groq sentiment error: {e}")
        return {t: "NEUTRAL" for t in titles}


def _apply_sentiment(items: List[Dict[str, Any]], enable: bool) -> List[Dict[str, Any]]:
    """Fill in sentiment label for each item. Uses per-headline cache first."""
    if not enable:
        for it in items:
            it["sentiment"] = "NEUTRAL"
        return items

    api_key = _get_groq_key()
    if not api_key:
        for it in items:
            it["sentiment"] = "NEUTRAL"
        return items

    now = time.time()
    uncached: List[str] = []
    for it in items:
        h = _hash_headline(it["title"])
        cached = _sentiment_cache.get(h)
        if cached and (now - cached["cached_at"]) < SENTIMENT_CACHE_TTL:
            it["sentiment"] = cached["label"]
        else:
            uncached.append(it["title"])
            it["sentiment"] = None  # placeholder

    # Batch-classify the uncached ones
    if uncached:
        labels = _classify_sentiment_batch(uncached, api_key)
        for title, label in labels.items():
            _sentiment_cache[_hash_headline(title)] = {"label": label, "cached_at": now}

    # Fill in the pending ones
    for it in items:
        if it["sentiment"] is None:
            h = _hash_headline(it["title"])
            cached = _sentiment_cache.get(h)
            it["sentiment"] = cached["label"] if cached else "NEUTRAL"

    return items


def get_market_news(category: str = "all", enable_sentiment: bool = True) -> Dict[str, Any]:
    """
    Main entry point.
    Returns: { items: [...], updated_at, category, count, sentiment_enabled }
    """
    category = category if category in FEED_CATALOG else "all"
    cache_key = f"news_{category}_{enable_sentiment}"

    # In-memory cache check
    cached = _news_cache.get(cache_key)
    if cached and (time.time() - cached["fetched_at"]) < NEWS_CACHE_TTL:
        return cached["data"]

    feeds = FEED_CATALOG[category]

    # Parallel fetch with small threadpool
    all_items: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=min(4, len(feeds))) as ex:
        futures = {ex.submit(_fetch_single_feed, label, url): label for label, url in feeds}
        for fut in as_completed(futures):
            items = fut.result()
            all_items.extend(items)

    # Dedupe + sort by recency
    all_items = _dedupe_items(all_items)
    all_items.sort(key=lambda x: x["published_epoch"], reverse=True)
    all_items = all_items[:MAX_ITEMS_RETURNED]

    # Add time_ago and sentiment
    for it in all_items:
        it["time_ago"] = _time_ago(it["published_epoch"])
    all_items = _apply_sentiment(all_items, enable_sentiment)

    payload = {
        "items": all_items,
        "count": len(all_items),
        "category": category,
        "sentiment_enabled": enable_sentiment and bool(_get_groq_key()),
        "updated_at": datetime.now(IST).isoformat(),
    }
    _news_cache[cache_key] = {"data": payload, "fetched_at": time.time()}
    return payload
