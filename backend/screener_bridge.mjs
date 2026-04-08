#!/usr/bin/env node
/**
 * screener_bridge.mjs — Node.js bridge for screener-scraper-pro
 * Called from Python via subprocess: node screener_bridge.mjs RELIANCE
 * Outputs JSON to stdout
 */

import { ScreenerScraperPro } from "screener-scraper-pro";

const ticker = process.argv[2];
if (!ticker) {
  console.error(JSON.stringify({ error: "Usage: node screener_bridge.mjs TICKER" }));
  process.exit(1);
}

// Build screener.in URL
const url = `https://www.screener.in/company/${ticker.toUpperCase()}/consolidated/`;

try {
  const data = await ScreenerScraperPro(url);

  // Extract key fields into a flat summary for quick access
  const summary = {};

  // Latest quarterly EPS
  if (data.quarters?.headers?.length) {
    const lastQ = data.quarters.headers[data.quarters.headers.length - 1];
    summary.latest_quarter = lastQ;
    summary.latest_q_sales = data.quarters.data?.Sales?.[lastQ] ?? null;
    summary.latest_q_profit = data.quarters.data?.["Net Profit"]?.[lastQ] ?? null;
    summary.latest_q_eps = data.quarters.data?.["EPS in Rs"]?.[lastQ] ?? null;
    summary.latest_q_opm = data.quarters.data?.["OPM %"]?.[lastQ] ?? null;
  }

  // Annual P&L TTM
  if (data.profitLoss?.data?.Sales?.TTM) {
    summary.ttm_sales = data.profitLoss.data.Sales.TTM;
    summary.ttm_profit = data.profitLoss.data["Net Profit"]?.TTM ?? null;
    summary.ttm_eps = data.profitLoss.data["EPS in Rs"]?.TTM ?? null;
  }

  // CAGRs
  if (data.CAGRs) {
    summary.sales_cagr_3y = data.CAGRs["Compounded Sales Growth"]?.["3 Years"] ?? null;
    summary.sales_cagr_5y = data.CAGRs["Compounded Sales Growth"]?.["5 Years"] ?? null;
    summary.profit_cagr_3y = data.CAGRs["Compounded Profit Growth"]?.["3 Years"] ?? null;
    summary.profit_cagr_5y = data.CAGRs["Compounded Profit Growth"]?.["5 Years"] ?? null;
    summary.price_cagr_1y = data.CAGRs["Stock Price CAGR"]?.["1 Year"] ?? null;
    summary.price_cagr_3y = data.CAGRs["Stock Price CAGR"]?.["3 Years"] ?? null;
    summary.price_cagr_5y = data.CAGRs["Stock Price CAGR"]?.["5 Years"] ?? null;
    summary.roe_3y = data.CAGRs["Return on Equity"]?.["3 Years"] ?? null;
    summary.roe_5y = data.CAGRs["Return on Equity"]?.["5 Years"] ?? null;
  }

  // Latest shareholding
  if (data.shareholding?.headers?.length && data.shareholding?.data) {
    const sh = data.shareholding;
    // Find the last non-empty header with Promoter data
    const validHeaders = sh.headers.filter(h => h && sh.data.Promoters?.[h]);
    if (validHeaders.length) {
      const lastH = validHeaders[validHeaders.length - 1];
      summary.shareholding_date = lastH;
      summary.promoter_pct = sh.data.Promoters?.[lastH] ?? null;
      summary.fii_pct = sh.data.FIIs?.[lastH] ?? null;
      summary.dii_pct = sh.data.DIIs?.[lastH] ?? null;
      summary.public_pct = sh.data.Public?.[lastH] ?? null;
      summary.num_shareholders = sh.data["No. of Shareholders"]?.[lastH] ?? null;
    }
  }

  // Pros/Cons
  summary.pros = data.analysis?.pros ?? [];
  summary.cons = data.analysis?.cons ?? [];

  // Latest ROCE
  if (data.ratios?.data?.["ROCE %"]) {
    const roceHeaders = data.ratios.headers || [];
    const lastRoce = roceHeaders[roceHeaders.length - 1];
    if (lastRoce) summary.roce = data.ratios.data["ROCE %"][lastRoce];
  }

  const output = {
    status: "ok",
    ticker: ticker.toUpperCase(),
    summary,
    quarters: data.quarters || null,
    profitLoss: data.profitLoss || null,
    balanceSheet: data.balanceSheet || null,
    cashFlow: data.cashFlow || null,
    ratios: data.ratios || null,
    shareholding: data.shareholding || null,
    cagrs: data.CAGRs || null,
    analysis: data.analysis || null,
    documents: data.documents || null,
  };

  console.log(JSON.stringify(output));
} catch (e) {
  // Try standalone (non-consolidated) URL as fallback
  try {
    const url2 = `https://www.screener.in/company/${ticker.toUpperCase()}/`;
    const data = await ScreenerScraperPro(url2);
    const output = {
      status: "ok",
      ticker: ticker.toUpperCase(),
      consolidated: false,
      summary: {},
      quarters: data.quarters || null,
      profitLoss: data.profitLoss || null,
      balanceSheet: data.balanceSheet || null,
      cashFlow: data.cashFlow || null,
      ratios: data.ratios || null,
      shareholding: data.shareholding || null,
      cagrs: data.CAGRs || null,
      analysis: data.analysis || null,
      documents: data.documents || null,
    };
    console.log(JSON.stringify(output));
  } catch (e2) {
    console.log(JSON.stringify({ status: "error", ticker: ticker.toUpperCase(), error: e2.message || String(e2) }));
  }
}
