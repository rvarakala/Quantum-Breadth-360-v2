// ════════════════════════════════════════════════════════════════════════════
// BILLING TAB — Subscription Management
// Shows: current plan, trial status, plan cards, payment history,
//        cancel/reactivate, Razorpay checkout flow
// ════════════════════════════════════════════════════════════════════════════

let _billingData = null;
let _rzpLoaded   = false;

// ── Entry point ───────────────────────────────────────────────────────────────
async function initBillingTab() {
  const root = document.getElementById('billing-root');
  if (!root) return;
  root.innerHTML = _skeletonTable(4, 3, 'Loading subscription…');
  await _fetchBilling();
}

async function _fetchBilling() {
  try {
    const res  = await fetch(`${API}/api/billing/subscription`);
    _billingData = await res.json();
    if (_billingData.error) throw new Error(_billingData.error);
    _renderBilling(_billingData);
  } catch (e) {
    document.getElementById('billing-root').innerHTML =
      `<div style="text-align:center;padding:60px;color:var(--red);font-family:var(--font-mono)">
        ⚠ ${e.message}<br>
        <button onclick="initBillingTab()" style="margin-top:12px" class="sm-export-btn">↺ Retry</button>
      </div>`;
  }
}

// ── Main renderer ─────────────────────────────────────────────────────────────
function _renderBilling(d) {
  const root = document.getElementById('billing-root');
  if (!root) return;

  const eff          = d.effective_tier;
  const isTrial      = d.trial_active;
  const isPaid       = ['trader','pro','elite'].includes(d.tier);
  const isCancelling = d.cancel_at_period_end;
  const subEnd       = d.subscription_end ? _fmtDate(d.subscription_end) : null;
  const trialEnd     = d.trial_ends_at    ? _fmtDate(d.trial_ends_at)    : null;

  const tierColors = {
    explorer:'#94a3b8', trader:'#06b6d4', pro:'#a855f7', elite:'#f59e0b', admin:'#ef4444'
  };
  const tierColor  = tierColors[eff] || '#94a3b8';
  const tierLabel  = isTrial ? `PRO TRIAL (${d.trial_days_left}d left)` : eff.toUpperCase();

  // ── Status banner ────────────────────────────────────────────────────────
  let bannerHtml = '';
  if (isTrial) {
    const urgency = d.trial_days_left <= 3 ? 'var(--red)' : d.trial_days_left <= 7 ? 'var(--amber)' : 'var(--green)';
    bannerHtml = `
      <div style="background:rgba(99,102,241,.08);border:1px solid rgba(99,102,241,.2);
        border-radius:12px;padding:16px 20px;margin-bottom:24px;
        display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px">
        <div>
          <div style="font-size:13px;font-weight:700;color:#e2e8f0;font-family:var(--font-mono)">
            ✨ Pro Trial Active
          </div>
          <div style="font-size:11px;color:var(--text2);margin-top:4px">
            <b style="color:${urgency}">${d.trial_days_left} day${d.trial_days_left!==1?'s':''} remaining</b>
            ${trialEnd ? `· Expires ${trialEnd}` : ''} · Upgrade to keep full access
          </div>
        </div>
        <button onclick="_billingScrollToPlans()"
          style="padding:8px 20px;border-radius:8px;background:#6366f1;border:none;color:#fff;
          font-family:var(--font-mono);font-size:11px;font-weight:700;cursor:pointer">
          Upgrade Now →
        </button>
      </div>`;
  } else if (isPaid && isCancelling) {
    bannerHtml = `
      <div style="background:var(--amber-dim);border:1px solid rgba(245,158,11,.3);
        border-radius:12px;padding:16px 20px;margin-bottom:24px;
        display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px">
        <div>
          <div style="font-size:13px;font-weight:700;color:var(--amber);font-family:var(--font-mono)">
            ⚠ Subscription Cancelling
          </div>
          <div style="font-size:11px;color:var(--text2);margin-top:4px">
            Your plan ends ${subEnd || 'at period end'}. You keep full access until then.
          </div>
        </div>
        <button onclick="_billingReactivate()"
          style="padding:8px 20px;border-radius:8px;background:var(--amber);border:none;color:#0f1628;
          font-family:var(--font-mono);font-size:11px;font-weight:700;cursor:pointer">
          Keep Subscription
        </button>
      </div>`;
  } else if (isPaid) {
    bannerHtml = `
      <div style="background:var(--green-dim);border:1px solid rgba(34,197,94,.2);
        border-radius:12px;padding:14px 20px;margin-bottom:24px;
        display:flex;align-items:center;gap:12px">
        <span style="font-size:20px">✅</span>
        <div>
          <div style="font-size:13px;font-weight:700;color:var(--green);font-family:var(--font-mono)">
            ${eff.charAt(0).toUpperCase()+eff.slice(1)} Plan Active
          </div>
          <div style="font-size:11px;color:var(--text2);margin-top:2px">
            ${d.subscription_cycle === 'annual' ? 'Annual' : 'Monthly'} billing
            ${subEnd ? `· Renews ${subEnd}` : ''}
          </div>
        </div>
      </div>`;
  }

  // ── Plan cards ───────────────────────────────────────────────────────────
  const plans = [
    {
      key: 'trader', label: 'Trader', color: '#06b6d4',
      monthly_inr: d.plan_prices?.trader?.monthly_inr || 2499,
      monthly_usd: d.plan_prices?.trader?.monthly_usd || 29,
      annual_inr:  d.plan_prices?.trader?.annual_inr  || 23990,
      annual_usd:  d.plan_prices?.trader?.annual_usd  || 290,
      features: ['Smart Money · RS Rankings', 'Charts · Scanner · Leaders', 'Stockbee Monitor'],
    },
    {
      key: 'pro', label: 'Pro', color: '#a855f7', popular: true,
      monthly_inr: d.plan_prices?.pro?.monthly_inr || 6799,
      monthly_usd: d.plan_prices?.pro?.monthly_usd || 79,
      annual_inr:  d.plan_prices?.pro?.annual_inr  || 65990,
      annual_usd:  d.plan_prices?.pro?.annual_usd  || 790,
      features: ['Everything in Trader', 'F-Value · AI Screener', 'Insider · FII/DII · Journal'],
    },
    {
      key: 'elite', label: 'Elite', color: '#f59e0b',
      monthly_inr: d.plan_prices?.elite?.monthly_inr || 12990,
      monthly_usd: d.plan_prices?.elite?.monthly_usd || 149,
      annual_inr:  d.plan_prices?.elite?.annual_inr  || 124990,
      annual_usd:  d.plan_prices?.elite?.annual_usd  || 1490,
      features: ['Everything in Pro', 'API Access · Alerts', 'Peep Into Past · All Tabs'],
    },
  ];

  const planCardsHtml = plans.map(p => {
    const isCurrent = d.tier === p.key;
    const isUpgrade = !['explorer'].includes(d.tier) ? false : true;
    const border = isCurrent ? `2px solid ${p.color}` : `1px solid rgba(255,255,255,.08)`;
    const bg = isCurrent ? `rgba(${_hexToRgb(p.color)},.06)` : 'transparent';
    const badge = p.popular
      ? `<div style="position:absolute;top:-10px;left:50%;transform:translateX(-50%);
           background:${p.color};color:#0f1628;font-size:9px;font-weight:800;
           padding:2px 12px;border-radius:10px;letter-spacing:.06em;white-space:nowrap">POPULAR</div>`
      : '';

    const btnLabel = isCurrent
      ? (isCancelling ? 'Reactivate ↺' : 'Current Plan')
      : `Upgrade to ${p.label}`;
    const btnStyle = isCurrent && !isCancelling
      ? `background:transparent;border:1px solid ${p.color};color:${p.color};cursor:default;opacity:.7`
      : `background:${p.color};border:none;color:#0f1628;cursor:pointer`;
    const btnClick = isCurrent && isCancelling
      ? `onclick="_billingReactivate()"`
      : isCurrent
        ? ''
        : `onclick="_billingCheckout('${p.key}', _billingGetCycle())"`;

    return `
      <div style="border:${border};background:${bg};border-radius:14px;padding:22px 18px;
        position:relative;transition:border-color .2s">
        ${badge}
        <div style="color:${p.color};font-weight:800;font-size:14px;font-family:var(--font-mono);
          margin-bottom:10px">${p.label}</div>
        <div style="display:flex;align-items:baseline;gap:4px;margin-bottom:4px">
          <span style="font-size:28px;font-weight:800;color:var(--text)">$${p.monthly_usd}</span>
          <span style="font-size:11px;color:var(--text3)">/month</span>
        </div>
        <div style="font-size:10px;color:var(--text3);font-family:var(--font-mono);margin-bottom:14px">
          ₹${p.monthly_inr.toLocaleString('en-IN')}/mo · ₹${p.annual_inr.toLocaleString('en-IN')}/yr
        </div>
        <div style="margin-bottom:16px">
          ${p.features.map(f => `
            <div style="font-size:11px;color:var(--text2);padding:3px 0;display:flex;gap:6px;align-items:start">
              <span style="color:${p.color};flex-shrink:0">✓</span>${f}
            </div>`).join('')}
        </div>
        <button ${btnClick}
          style="width:100%;padding:9px;border-radius:8px;${btnStyle};
          font-family:var(--font-mono);font-size:11px;font-weight:700;transition:opacity .2s">
          ${btnLabel}
        </button>
      </div>`;
  }).join('');

  // ── Billing cycle toggle ─────────────────────────────────────────────────
  const cycleToggle = `
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:20px;
      font-family:var(--font-mono);font-size:11px">
      <span id="billing-cycle-label" style="color:var(--text2)">Billing: Monthly</span>
      <label style="display:flex;align-items:center;gap:8px;cursor:pointer">
        <div onclick="_billingToggleCycle()" id="billing-cycle-toggle"
          style="width:36px;height:20px;border-radius:10px;background:var(--card-border);
          position:relative;cursor:pointer;transition:background .2s">
          <div id="billing-cycle-knob"
            style="width:16px;height:16px;border-radius:50%;background:#fff;
            position:absolute;top:2px;left:2px;transition:left .2s"></div>
        </div>
        <span style="color:var(--text3)">Annual <b style="color:var(--green)">−20%</b></span>
      </label>
    </div>`;

  // ── Payment history ──────────────────────────────────────────────────────
  let historyHtml = '';
  if (d.payments && d.payments.length > 0) {
    const rows = d.payments.map(p => `
      <tr style="border-bottom:1px solid var(--table-border)">
        <td style="padding:9px 12px;color:var(--text2);font-size:11px">${_fmtDate(p.created_at)}</td>
        <td style="padding:9px 12px">
          <span style="font-size:10px;font-weight:700;color:#a855f7;font-family:var(--font-mono)">
            ${p.tier?.toUpperCase()}
          </span>
          <span style="font-size:10px;color:var(--text3);margin-left:6px">${p.billing_cycle}</span>
        </td>
        <td style="padding:9px 12px;font-family:var(--font-mono);font-size:11px;color:var(--text)">
          ₹${(p.amount_inr||0).toLocaleString('en-IN')}
        </td>
        <td style="padding:9px 12px">
          <span style="font-size:9px;font-weight:700;padding:2px 8px;border-radius:4px;
            background:var(--green-dim);color:var(--green)">PAID</span>
        </td>
        <td style="padding:9px 12px;font-size:10px;color:var(--text3);font-family:var(--font-mono)">
          ${p.razorpay_payment_id ? p.razorpay_payment_id.slice(0,18)+'…' : '—'}
        </td>
      </tr>`).join('');

    historyHtml = `
      <div style="margin-top:32px">
        <div style="font-size:12px;font-weight:700;color:var(--text2);
          font-family:var(--font-mono);letter-spacing:.08em;text-transform:uppercase;
          margin-bottom:12px">Payment History</div>
        <div style="overflow-x:auto;border-radius:10px;border:1px solid var(--card-border)">
          <table style="width:100%;border-collapse:collapse;min-width:500px">
            <thead>
              <tr style="background:var(--table-header-bg)">
                ${['Date','Plan','Amount','Status','Transaction ID'].map(h =>
                  `<th style="padding:8px 12px;text-align:left;font-size:9px;
                    font-weight:700;color:var(--text3);letter-spacing:.08em;
                    text-transform:uppercase;font-family:var(--font-mono)">${h}</th>`
                ).join('')}
              </tr>
            </thead>
            <tbody>${rows}</tbody>
          </table>
        </div>
      </div>`;
  }

  // ── Cancel button (only for active paid subscribers) ─────────────────────
  const cancelHtml = isPaid && !isCancelling ? `
    <div style="margin-top:32px;padding:16px 20px;border-radius:12px;
      border:1px solid rgba(239,68,68,.15);background:rgba(239,68,68,.04)">
      <div style="font-size:12px;font-weight:700;color:var(--text2);
        font-family:var(--font-mono);margin-bottom:6px">Cancel Subscription</div>
      <div style="font-size:11px;color:var(--text3);margin-bottom:12px;line-height:1.6">
        You'll keep full access until <b style="color:var(--text)">${subEnd || 'end of billing period'}</b>.
        After that your account moves to the free Explorer plan.
      </div>
      <button onclick="_billingCancel()"
        style="padding:7px 18px;border-radius:7px;border:1px solid rgba(239,68,68,.4);
        background:transparent;color:var(--red);font-family:var(--font-mono);
        font-size:11px;cursor:pointer;font-weight:600">
        Cancel Subscription
      </button>
    </div>` : '';

  // ── Assemble ─────────────────────────────────────────────────────────────
  root.innerHTML = `
    <!-- Page header -->
    <div style="display:flex;align-items:center;justify-content:space-between;
      margin-bottom:24px;flex-wrap:wrap;gap:12px">
      <div>
        <div style="font-size:18px;font-weight:800;color:var(--text);
          font-family:var(--font-mono);letter-spacing:-.01em">My Plan</div>
        <div style="font-size:11px;color:var(--text3);margin-top:4px">
          Manage your subscription and billing
        </div>
      </div>
      <div style="display:flex;align-items:center;gap:8px">
        <span style="font-family:var(--font-mono);font-size:11px;
          color:${tierColor};font-weight:700;padding:4px 12px;
          border:1px solid ${tierColor}33;border-radius:20px;
          background:${tierColor}11">${tierLabel}</span>
      </div>
    </div>

    ${bannerHtml}

    <!-- Plans section -->
    <div id="billing-plans-section">
      <div style="font-size:12px;font-weight:700;color:var(--text2);
        font-family:var(--font-mono);letter-spacing:.08em;text-transform:uppercase;
        margin-bottom:16px">Choose a Plan</div>
      ${cycleToggle}
      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:14px">
        ${planCardsHtml}
      </div>
      ${d.rzp_enabled ? '' : `
        <div style="margin-top:12px;padding:10px 14px;border-radius:8px;
          background:var(--amber-dim);border:1px solid rgba(245,158,11,.2);
          font-size:11px;color:var(--amber);font-family:var(--font-mono)">
          ⚠ Payment gateway not configured. Contact
          <a href="mailto:support@quantumtrade.pro"
            style="color:var(--amber)">support@quantumtrade.pro</a> to upgrade.
        </div>`}
    </div>

    ${historyHtml}
    ${cancelHtml}

    <div style="margin-top:24px;font-size:10px;color:var(--text3);font-family:var(--font-mono)">
      Questions? Email <a href="mailto:support@quantumtrade.pro"
        style="color:var(--cyan)">support@quantumtrade.pro</a>
    </div>`;

  // Mobile: stack plan cards
  if (window.innerWidth < 640) {
    const grid = root.querySelector('[style*="grid-template-columns:repeat(3"]');
    if (grid) grid.style.gridTemplateColumns = '1fr';
  }
}

// ── Billing cycle toggle ──────────────────────────────────────────────────────
let _billingCycle = 'monthly';

function _billingGetCycle() { return _billingCycle; }

function _billingToggleCycle() {
  _billingCycle = _billingCycle === 'monthly' ? 'annual' : 'monthly';
  const toggle  = document.getElementById('billing-cycle-toggle');
  const knob    = document.getElementById('billing-cycle-knob');
  const label   = document.getElementById('billing-cycle-label');
  const isAnnual = _billingCycle === 'annual';
  if (toggle) toggle.style.background = isAnnual ? 'var(--green)' : 'var(--card-border)';
  if (knob)   knob.style.left         = isAnnual ? '18px' : '2px';
  if (label)  label.textContent       = `Billing: ${isAnnual ? 'Annual (save 20%)' : 'Monthly'}`;
}

function _billingScrollToPlans() {
  document.getElementById('billing-plans-section')
    ?.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

// ── Checkout flow ─────────────────────────────────────────────────────────────
async function _billingCheckout(tier, cycle) {
  if (!_billingData?.rzp_enabled) {
    alert('Payment gateway not yet configured.\n\nContact support@quantumtrade.pro to upgrade manually.');
    return;
  }

  const btn = event?.target;
  if (btn) { btn.disabled = true; btn.textContent = 'Creating order…'; }

  try {
    const res   = await fetch(`${API}/api/billing/create-order`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tier, cycle }),
    });
    const order = await res.json();
    if (order.error) throw new Error(order.error);

    // Load Razorpay SDK if not already loaded
    await _loadRzpSdk();

    const rzp = new Razorpay({
      key:         order.key_id,
      amount:      order.amount,
      currency:    order.currency,
      order_id:    order.order_id,
      name:        'Quantum Breadth 360',
      description: `${tier.charAt(0).toUpperCase()+tier.slice(1)} Plan (${cycle})`,
      image:       '/static/logo.png',
      prefill:     order.prefill || {},
      theme:       { color: '#6366f1' },
      handler: async (response) => {
        await _billingVerify(response, tier, cycle);
      },
      modal: {
        ondismiss: () => {
          if (btn) { btn.disabled = false; btn.textContent = `Upgrade to ${tier.charAt(0).toUpperCase()+tier.slice(1)}`; }
        }
      }
    });
    rzp.open();
  } catch (e) {
    alert(`Checkout failed: ${e.message}`);
    if (btn) { btn.disabled = false; btn.textContent = `Upgrade to ${tier.charAt(0).toUpperCase()+tier.slice(1)}`; }
  }
}

async function _billingVerify(rzpResponse, tier, cycle) {
  try {
    const res  = await fetch(`${API}/api/billing/verify-payment`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        razorpay_order_id:   rzpResponse.razorpay_order_id,
        razorpay_payment_id: rzpResponse.razorpay_payment_id,
        razorpay_signature:  rzpResponse.razorpay_signature,
      }),
    });
    const data = await res.json();
    if (data.error) throw new Error(data.error);

    // Refresh JWT + user session so new tier takes effect immediately
    if (data.refreshed_token) localStorage.setItem('qb360_token', data.refreshed_token);
    // Force full page reload so TIER_TABS re-gates correctly
    _billingData = null;
    window.location.reload();
  } catch (e) {
    alert(`Payment verification failed: ${e.message}\nPlease contact support@quantumtrade.pro`);
    initBillingTab();
  }
}

function _loadRzpSdk() {
  if (_rzpLoaded || window.Razorpay) { _rzpLoaded = true; return Promise.resolve(); }
  return new Promise((res, rej) => {
    const s = document.createElement('script');
    s.src = 'https://checkout.razorpay.com/v1/checkout.js';
    s.onload  = () => { _rzpLoaded = true; res(); };
    s.onerror = () => rej(new Error('Razorpay SDK failed to load'));
    document.head.appendChild(s);
  });
}

// ── Cancel / Reactivate ───────────────────────────────────────────────────────
async function _billingCancel() {
  if (!confirm('Cancel your subscription?\n\nYou keep full access until your billing period ends.')) return;
  try {
    const res  = await fetch(`${API}/api/billing/cancel`, { method: 'POST' });
    const data = await res.json();
    if (data.error) throw new Error(data.error);
    alert(data.message);
    await _fetchBilling();
  } catch (e) { alert(`Error: ${e.message}`); }
}

async function _billingReactivate() {
  try {
    const res  = await fetch(`${API}/api/billing/reactivate`, { method: 'POST' });
    const data = await res.json();
    if (data.error) throw new Error(data.error);
    alert(data.message);
    await _fetchBilling();
  } catch (e) { alert(`Error: ${e.message}`); }
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function _fmtDate(iso) {
  try {
    return new Date(iso).toLocaleDateString('en-IN', {
      day: 'numeric', month: 'short', year: 'numeric'
    });
  } catch { return iso; }
}

function _hexToRgb(hex) {
  const r = parseInt(hex.slice(1,3),16);
  const g = parseInt(hex.slice(3,5),16);
  const b = parseInt(hex.slice(5,7),16);
  return `${r},${g},${b}`;
}
