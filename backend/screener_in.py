"""
screener_in.py — Scrape screener.in for quarterly results, ownership, fund count.
Caches per-ticker to avoid repeated hits. Returns structured dicts.
"""

import logging, time, re
from typing import Optional
from functools import lru_cache

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

# In-memory cache: ticker → {data, timestamp}
_cache = {}
_CACHE_TTL = 3600 * 6  # 6 hours


def _fetch_page(ticker: str) -> Optional[str]:
    """Fetch screener.in company page HTML."""
    import httpx
    url = f"https://www.screener.in/company/{ticker}/consolidated/"
    try:
        r = httpx.get(url, headers=_HEADERS, follow_redirects=True, timeout=15)
        if r.status_code == 200:
            return r.text
        # Try standalone if consolidated fails
        url2 = f"https://www.screener.in/company/{ticker}/"
        r2 = httpx.get(url2, headers=_HEADERS, follow_redirects=True, timeout=15)
        if r2.status_code == 200:
            return r2.text
        logger.warning(f"screener.in: {ticker} returned {r.status_code}/{r2.status_code}")
        return None
    except Exception as e:
        logger.warning(f"screener.in fetch error for {ticker}: {e}")
        return None


def _parse_quarterly(html: str) -> list:
    """Extract quarterly results table: [{quarter, sales, expenses, profit, eps}, ...]"""
    results = []
    try:
        # Find quarterly results section
        match = re.search(r'id="quarters".*?<table.*?>(.*?)</table>', html, re.DOTALL)
        if not match:
            return results
        table = match.group(1)

        # Parse header for quarter dates
        hdr_match = re.search(r'<thead>(.*?)</thead>', table, re.DOTALL)
        if not hdr_match:
            return results
        headers = re.findall(r'<th[^>]*>(.*?)</th>', hdr_match.group(1), re.DOTALL)
        # headers[0] is blank, rest are like "Mar 2025", "Dec 2024" etc.
        quarter_dates = [h.strip() for h in headers[1:] if h.strip()]

        # Parse body rows
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table, re.DOTALL)
        row_data = {}
        for row in rows:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            if not cells:
                continue
            label = re.sub(r'<[^>]+>', '', cells[0]).strip()
            vals = []
            for c in cells[1:]:
                txt = re.sub(r'<[^>]+>', '', c).strip().replace(',', '')
                try:
                    vals.append(float(txt))
                except ValueError:
                    vals.append(None)
            if label:
                row_data[label.lower()] = vals

        sales = row_data.get('sales', row_data.get('revenue', []))
        expenses = row_data.get('expenses', [])
        profit = row_data.get('net profit', row_data.get('profit', []))
        eps_row = row_data.get('eps', row_data.get('eps (rs)', []))

        for i, qd in enumerate(quarter_dates):
            entry = {"quarter": qd}
            if i < len(sales) and sales[i] is not None:
                entry["sales"] = sales[i]
            if i < len(expenses) and expenses[i] is not None:
                entry["expenses"] = expenses[i]
            if i < len(profit) and profit[i] is not None:
                entry["profit"] = profit[i]
            if i < len(eps_row) and eps_row[i] is not None:
                entry["eps"] = eps_row[i]
            if len(entry) > 1:  # has more than just quarter name
                results.append(entry)

    except Exception as e:
        logger.debug(f"Quarterly parse error: {e}")
    return results


def _parse_annual(html: str) -> list:
    """Extract annual P&L: [{year, sales, profit, eps}, ...]"""
    results = []
    try:
        match = re.search(r'id="profit-loss".*?<table.*?>(.*?)</table>', html, re.DOTALL)
        if not match:
            return results
        table = match.group(1)

        hdr_match = re.search(r'<thead>(.*?)</thead>', table, re.DOTALL)
        if not hdr_match:
            return results
        headers = re.findall(r'<th[^>]*>(.*?)</th>', hdr_match.group(1), re.DOTALL)
        years = [h.strip() for h in headers[1:] if h.strip()]

        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table, re.DOTALL)
        row_data = {}
        for row in rows:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            if not cells:
                continue
            label = re.sub(r'<[^>]+>', '', cells[0]).strip().lower()
            vals = []
            for c in cells[1:]:
                txt = re.sub(r'<[^>]+>', '', c).strip().replace(',', '')
                try:
                    vals.append(float(txt))
                except ValueError:
                    vals.append(None)
            if label:
                row_data[label] = vals

        sales = row_data.get('sales', row_data.get('revenue', []))
        profit = row_data.get('net profit', row_data.get('profit', []))
        eps_row = row_data.get('eps', row_data.get('eps (rs)', []))

        for i, yr in enumerate(years):
            entry = {"year": yr}
            if i < len(sales) and sales[i] is not None:
                entry["sales"] = sales[i]
            if i < len(profit) and profit[i] is not None:
                entry["profit"] = profit[i]
            if i < len(eps_row) and eps_row[i] is not None:
                entry["eps"] = eps_row[i]
            if len(entry) > 1:
                results.append(entry)

    except Exception as e:
        logger.debug(f"Annual parse error: {e}")
    return results


def _parse_shareholding(html: str) -> list:
    """Extract shareholding pattern: [{quarter, promoters, fii, dii, public, num_funds}, ...]"""
    results = []
    try:
        match = re.search(r'id="shareholding".*?<table.*?>(.*?)</table>', html, re.DOTALL)
        if not match:
            return results
        table = match.group(1)

        hdr_match = re.search(r'<thead>(.*?)</thead>', table, re.DOTALL)
        if not hdr_match:
            return results
        headers = re.findall(r'<th[^>]*>(.*?)</th>', hdr_match.group(1), re.DOTALL)
        quarters = [h.strip() for h in headers[1:] if h.strip()]

        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table, re.DOTALL)
        row_data = {}
        for row in rows:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            if not cells:
                continue
            label = re.sub(r'<[^>]+>', '', cells[0]).strip().lower()
            vals = []
            for c in cells[1:]:
                txt = re.sub(r'<[^>]+>', '', c).strip().replace(',', '').replace('%', '')
                try:
                    vals.append(float(txt))
                except ValueError:
                    vals.append(None)
            if label:
                row_data[label] = vals

        promoters = row_data.get('promoters', row_data.get('promoter', []))
        fii = row_data.get('fiis', row_data.get('fii', []))
        dii = row_data.get('diis', row_data.get('dii', []))
        public = row_data.get('public', [])
        # No. of shareholders or MF schemes
        num_sh = row_data.get('no. of shareholders', [])

        for i, q in enumerate(quarters):
            entry = {"quarter": q}
            if i < len(promoters) and promoters[i] is not None:
                entry["promoters"] = promoters[i]
            if i < len(fii) and fii[i] is not None:
                entry["fii"] = fii[i]
            if i < len(dii) and dii[i] is not None:
                entry["dii"] = dii[i]
            if i < len(public) and public[i] is not None:
                entry["public"] = public[i]
            if i < len(num_sh) and num_sh[i] is not None:
                entry["num_shareholders"] = int(num_sh[i])
            if len(entry) > 1:
                results.append(entry)

    except Exception as e:
        logger.debug(f"Shareholding parse error: {e}")
    return results


def _parse_ratios(html: str) -> dict:
    """Extract key ratios from the ratios section."""
    ratios = {}
    try:
        # Look for the ratios list (top section with ROE, PE, etc.)
        ul_match = re.search(r'id="top-ratios".*?<ul[^>]*>(.*?)</ul>', html, re.DOTALL)
        if not ul_match:
            # Try alternate: look for ratio items in the header area
            items = re.findall(r'<li[^>]*class="[^"]*flex[^"]*"[^>]*>.*?<span[^>]*class="name"[^>]*>(.*?)</span>.*?<span[^>]*class="[^"]*number[^"]*"[^>]*>(.*?)</span>', html, re.DOTALL)
            for name, val in items:
                name = re.sub(r'<[^>]+>', '', name).strip()
                val = re.sub(r'<[^>]+>', '', val).strip().replace(',', '').replace('%', '').replace('₹', '')
                try:
                    ratios[name] = float(val)
                except ValueError:
                    ratios[name] = val
            return ratios

        list_html = ul_match.group(1)
        items = re.findall(r'<li[^>]*>(.*?)</li>', list_html, re.DOTALL)
        for item in items:
            name_m = re.search(r'<span[^>]*class="[^"]*name[^"]*"[^>]*>(.*?)</span>', item, re.DOTALL)
            val_m = re.search(r'<span[^>]*class="[^"]*number[^"]*"[^>]*>(.*?)</span>', item, re.DOTALL)
            if name_m and val_m:
                name = re.sub(r'<[^>]+>', '', name_m.group(1)).strip()
                val = re.sub(r'<[^>]+>', '', val_m.group(1)).strip().replace(',', '').replace('%', '').replace('₹', '')
                try:
                    ratios[name] = float(val)
                except ValueError:
                    ratios[name] = val

    except Exception as e:
        logger.debug(f"Ratios parse error: {e}")
    return ratios


def _parse_num_funds(html: str) -> list:
    """Extract number of mutual fund schemes holding the stock over quarters."""
    results = []
    try:
        # Look for mutual fund section
        match = re.search(r'Mutual Funds.*?(\d+)\s*(?:scheme|fund)', html, re.IGNORECASE)
        if match:
            # Simple: just extract the current count
            pass

        # Better: look for the shareholding table and extract DII/MF details
        # screener.in shows "No. of Mutual Fund Schemes" in shareholding
        sh_match = re.search(r'id="shareholding".*?</section>', html, re.DOTALL)
        if sh_match:
            sh_html = sh_match.group(0)
            # Find rows with "Mutual Funds" or "No. of" in shareholding
            fund_match = re.findall(r'No\.\s*of\s*(?:Mutual\s*Fund\s*)?(?:Schemes|shareholders)[^<]*</.*?(?:<td[^>]*>(.*?)</td>)', sh_html, re.DOTALL | re.IGNORECASE)
            # Fallback: look for any row mentioning funds
            mf_rows = re.findall(r'(?:Mutual\s*Fund|MF)\s*Schemes?\s*</.*?(<td.*?</tr>)', sh_html, re.DOTALL | re.IGNORECASE)
            for row in mf_rows:
                vals = re.findall(r'<td[^>]*>(.*?)</td>', row)
                for v in vals:
                    txt = re.sub(r'<[^>]+>', '', v).strip().replace(',', '')
                    try:
                        results.append(int(float(txt)))
                    except ValueError:
                        pass
    except Exception as e:
        logger.debug(f"Fund count parse error: {e}")
    return results


def get_screener_in_data(ticker: str) -> dict:
    """
    Main entry point — fetch and parse all screener.in data for a ticker.
    Returns cached data if fresh enough.
    """
    ticker = ticker.upper().strip()

    # Check cache
    if ticker in _cache:
        age = time.time() - _cache[ticker]["ts"]
        if age < _CACHE_TTL:
            return _cache[ticker]["data"]

    html = _fetch_page(ticker)
    if not html:
        return {"ticker": ticker, "error": "Could not fetch screener.in page",
                "quarterly": [], "annual": [], "shareholding": [], "ratios": {}}

    data = {
        "ticker": ticker,
        "quarterly": _parse_quarterly(html),
        "annual": _parse_annual(html),
        "shareholding": _parse_shareholding(html),
        "ratios": _parse_ratios(html),
        "source": "screener.in",
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    # Cache it
    _cache[ticker] = {"data": data, "ts": time.time()}
    logger.info(f"screener.in: {ticker} — {len(data['quarterly'])} quarters, "
                f"{len(data['shareholding'])} shareholding periods, "
                f"{len(data['ratios'])} ratios")
    return data
