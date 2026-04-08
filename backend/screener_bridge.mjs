#!/usr/bin/env node
/**
 * screener_bridge.mjs — Direct screener.in scraper (no npm dependency)
 * Usage: node screener_bridge.mjs RELIANCE
 * Scrapes https://www.screener.in/company/TICKER/consolidated/
 * Parses HTML tables, outputs JSON to stdout
 */

const ticker = process.argv[2];
if (!ticker) {
  console.log(JSON.stringify({ status: "error", error: "Usage: node screener_bridge.mjs TICKER" }));
  process.exit(1);
}

const urls = [
  `https://www.screener.in/company/${ticker.toUpperCase()}/consolidated/`,
  `https://www.screener.in/company/${ticker.toUpperCase()}/`,
];

async function scrape(url) {
  const res = await fetch(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml',
      'Accept-Language': 'en-US,en;q=0.9',
    }
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return await res.text();
}

function parseTable(html, sectionId) {
  // Find section by id or heading text
  const regex = new RegExp(`id="${sectionId}"[^>]*>[\\s\\S]*?<table[^>]*>([\\s\\S]*?)</table>`, 'i');
  let match = html.match(regex);
  if (!match) {
    // Try by section class/heading
    const regex2 = new RegExp(`<section[^>]*id="${sectionId}"[^>]*>[\\s\\S]*?<table[^>]*>([\\s\\S]*?)</table>`, 'i');
    match = html.match(regex2);
  }
  if (!match) return null;

  const tableHtml = match[1];

  // Parse headers
  const headerMatch = tableHtml.match(/<thead[^>]*>([\s\S]*?)<\/thead>/i);
  const headers = [];
  if (headerMatch) {
    const ths = headerMatch[1].matchAll(/<th[^>]*>([\s\S]*?)<\/th>/gi);
    for (const th of ths) {
      const text = th[1].replace(/<[^>]+>/g, '').trim();
      if (text) headers.push(text);
    }
  }

  // Parse rows
  const bodyMatch = tableHtml.match(/<tbody[^>]*>([\s\S]*?)<\/tbody>/i);
  const data = {};
  if (bodyMatch) {
    const rows = bodyMatch[1].matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi);
    for (const row of rows) {
      const cells = [];
      const tds = row[1].matchAll(/<td[^>]*>([\s\S]*?)<\/td>/gi);
      for (const td of tds) {
        let text = td[1].replace(/<[^>]+>/g, '').replace(/&nbsp;/g, ' ').replace(/,/g, '').trim();
        cells.push(text);
      }
      if (cells.length >= 2) {
        const rowName = cells[0];
        data[rowName] = {};
        for (let i = 1; i < cells.length && i < headers.length; i++) {
          const val = cells[i];
          const num = parseFloat(val);
          data[rowName][headers[i]] = isNaN(num) ? val : (val.includes('%') ? val : num);
        }
      }
    }
  }

  return { headers: headers.slice(1), data };
}

function parseAnalysis(html) {
  const pros = [];
  const cons = [];
  const prosMatch = html.match(/<div[^>]*class="[^"]*pros[^"]*"[^>]*>([\s\S]*?)<\/div>/i);
  if (prosMatch) {
    const lis = prosMatch[1].matchAll(/<li[^>]*>([\s\S]*?)<\/li>/gi);
    for (const li of lis) pros.push(li[1].replace(/<[^>]+>/g, '').trim());
  }
  const consMatch = html.match(/<div[^>]*class="[^"]*cons[^"]*"[^>]*>([\s\S]*?)<\/div>/i);
  if (consMatch) {
    const lis = consMatch[1].matchAll(/<li[^>]*>([\s\S]*?)<\/li>/gi);
    for (const li of lis) cons.push(li[1].replace(/<[^>]+>/g, '').trim());
  }
  return { pros, cons };
}

function parseCAGRs(html) {
  const cagrs = {};
  // Find the CAGR section — it has specific headings like "Compounded Sales Growth"
  const sections = html.matchAll(/<span[^>]*class="[^"]*rate-perf[^"]*"[^>]*>([\s\S]*?)<\/span>/gi);
  // Alternative: parse from the ranges section
  const rangesMatch = html.match(/<div[^>]*id="ranges"[^>]*>([\s\S]*?)<\/div>\s*<\/div>/i);
  if (!rangesMatch) return cagrs;

  const tables = rangesMatch[1].matchAll(/<table[^>]*>([\s\S]*?)<\/table>/gi);
  const cagrNames = ["Compounded Sales Growth", "Compounded Profit Growth", "Stock Price CAGR", "Return on Equity"];
  let idx = 0;
  for (const table of tables) {
    if (idx >= cagrNames.length) break;
    const rows = table[1].matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi);
    const vals = {};
    for (const row of rows) {
      const cells = row[1].matchAll(/<td[^>]*>([\s\S]*?)<\/td>/gi);
      const parts = [];
      for (const c of cells) parts.push(c[1].replace(/<[^>]+>/g, '').trim());
      if (parts.length >= 2) vals[parts[0]] = parts[1];
    }
    if (Object.keys(vals).length) {
      cagrs[cagrNames[idx]] = vals;
    }
    idx++;
  }
  return cagrs;
}

function parseShareholding(html) {
  return parseTable(html, 'shareholding');
}

function buildSummary(data) {
  const s = {};
  // Latest quarter
  if (data.quarters?.headers?.length) {
    const last = data.quarters.headers[data.quarters.headers.length - 1];
    s.latest_quarter = last;
    s.latest_q_sales = data.quarters.data?.Sales?.[last] ?? null;
    s.latest_q_profit = data.quarters.data?.["Net Profit"]?.[last] ?? null;
    s.latest_q_eps = data.quarters.data?.["EPS in Rs"]?.[last] ?? null;
    s.latest_q_opm = data.quarters.data?.["OPM %"]?.[last] ?? null;
  }
  // TTM
  if (data.profitLoss?.data?.Sales?.TTM) {
    s.ttm_sales = data.profitLoss.data.Sales.TTM;
    s.ttm_profit = data.profitLoss.data["Net Profit"]?.TTM ?? null;
    s.ttm_eps = data.profitLoss.data["EPS in Rs"]?.TTM ?? null;
  }
  // CAGRs
  if (data.cagrs) {
    s.sales_cagr_3y = data.cagrs["Compounded Sales Growth"]?.["3 Years"] ?? null;
    s.sales_cagr_5y = data.cagrs["Compounded Sales Growth"]?.["5 Years"] ?? null;
    s.profit_cagr_3y = data.cagrs["Compounded Profit Growth"]?.["3 Years"] ?? null;
    s.profit_cagr_5y = data.cagrs["Compounded Profit Growth"]?.["5 Years"] ?? null;
    s.price_cagr_1y = data.cagrs["Stock Price CAGR"]?.["1 Year"] ?? null;
    s.price_cagr_5y = data.cagrs["Stock Price CAGR"]?.["5 Years"] ?? null;
    s.roe_3y = data.cagrs["Return on Equity"]?.["3 Years"] ?? null;
  }
  // Shareholding
  if (data.shareholding?.data?.Promoters) {
    const sh = data.shareholding;
    const validH = sh.headers.filter(h => h && sh.data.Promoters[h]);
    if (validH.length) {
      const last = validH[validH.length - 1];
      s.shareholding_date = last;
      s.promoter_pct = sh.data.Promoters?.[last] ?? null;
      s.fii_pct = sh.data.FIIs?.[last] ?? null;
      s.dii_pct = sh.data.DIIs?.[last] ?? null;
      s.public_pct = sh.data.Public?.[last] ?? null;
    }
  }
  // Pros/Cons
  s.pros = data.analysis?.pros ?? [];
  s.cons = data.analysis?.cons ?? [];
  return s;
}

async function main() {
  let html = null;
  for (const url of urls) {
    try {
      html = await scrape(url);
      if (html && html.length > 1000) break;
    } catch (e) { /* try next URL */ }
  }

  if (!html || html.length < 1000) {
    console.log(JSON.stringify({ status: "error", ticker: ticker.toUpperCase(), error: "Could not fetch screener.in page" }));
    return;
  }

  const quarters = parseTable(html, 'quarters');
  const profitLoss = parseTable(html, 'profit-loss');
  const balanceSheet = parseTable(html, 'balance-sheet');
  const cashFlow = parseTable(html, 'cash-flow');
  const ratios = parseTable(html, 'ratios');
  const shareholding = parseShareholding(html);
  const analysis = parseAnalysis(html);
  const cagrs = parseCAGRs(html);

  const result = {
    status: "ok",
    ticker: ticker.toUpperCase(),
    quarters, profitLoss, balanceSheet, cashFlow, ratios,
    shareholding, analysis, cagrs,
  };
  result.summary = buildSummary(result);

  console.log(JSON.stringify(result));
}

main().catch(e => {
  console.log(JSON.stringify({ status: "error", ticker: ticker.toUpperCase(), error: e.message || String(e) }));
});
