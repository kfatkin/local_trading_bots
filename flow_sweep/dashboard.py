import json
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from .config import (
    CONSENSUS_THRESHOLD,
    DASHBOARD_ENABLED,
    DASHBOARD_HOST,
    DASHBOARD_PORT,
    ENTRY_WINDOW_END,
    ENTRY_WINDOW_START,
    ET,
    LOGGER,
    MIN_FLOW_SCORE,
    OPTION_PREVIEW_REFRESH_SECONDS,
    PAPER,
    SYMBOLS,
    TARGET_DELTA,
    TRADE_ALLOCATION_PCT,
)
from .state import (
    CONTEXT_LOCK,
    STATE_LOCK,
    active_positions,
    daily_context,
    daily_trade_state,
    last_completed_bars,
    pending_entry_orders,
    pending_exit_orders,
)
from .utils import round_or_none, to_int_qty


def default_decision(symbol):
    return {
        "symbol": symbol,
        "status": "pending",
        "reason": "Waiting for daily preparation",
        "direction": "neutral",
        "consensus": None,
        "top_score": 0,
        "bullish_premium": 0.0,
        "bearish_premium": 0.0,
        "total_premium": 0.0,
        "raw_row_count": 0,
        "directional_row_count": 0,
        "option_type": None,
        "trigger_levels": [],
        "target_levels": [],
        "flow_rows": [],
        "key_levels": [],
        "contract_preview": {"status": "not_planned", "reason": "Waiting for daily preparation"},
    }


def position_payload(symbol, position):
    return {
        "symbol": symbol,
        "managed": bool(position.get("managed", True)),
        "option_symbol": position.get("option_symbol"),
        "option_type": position.get("option_type"),
        "entry_status": position.get("entry_status"),
        "total_qty": to_int_qty(position.get("total_qty", 0)),
        "requested_qty": to_int_qty(position.get("requested_qty", 0)),
        "entry_underlying": round_or_none(position.get("entry_underlying")),
        "stop_underlying": round_or_none(position.get("stop_underlying")),
        "target_underlying": round_or_none(position.get("target_underlying")),
        "target_name": position.get("target_name"),
        "swept_level": position.get("swept_level"),
        "swept_level_price": round_or_none(position.get("swept_level_price")),
        "entry_option_delta": round_or_none(position.get("entry_option_delta"), 4),
        "entry_option_gamma": round_or_none(position.get("entry_option_gamma"), 4),
        "entry_option_theta": round_or_none(position.get("entry_option_theta"), 4),
        "entry_option_ask": round_or_none(position.get("entry_option_ask"), 4),
        "entry_contract_cost": round_or_none(position.get("entry_contract_cost"), 2),
    }


def pending_exit_payload(order_id, order_state):
    return {
        "order_id": order_id,
        "symbol": order_state.get("symbol"),
        "qty": to_int_qty(order_state.get("qty", 0)),
        "filled_qty": to_int_qty(order_state.get("filled_qty", 0)),
        "reason": order_state.get("reason"),
    }


def completed_bar_payload(symbol, bar):
    return {
        "symbol": symbol,
        "bucket": bar["bucket"].isoformat() if bar.get("bucket") else None,
        "close_time": bar["close_time"].isoformat() if bar.get("close_time") else None,
        "open": round_or_none(bar.get("open")),
        "high": round_or_none(bar.get("high")),
        "low": round_or_none(bar.get("low")),
        "close": round_or_none(bar.get("close")),
        "volume": round_or_none(bar.get("volume"), 0),
    }


def dashboard_status_payload():
    try:
        from .strategy import refresh_contract_previews_if_needed

        refresh_contract_previews_if_needed()
    except Exception as exc:
        LOGGER.warning("Unable to refresh contract previews for dashboard: %s", exc)

    now_et = datetime.now(ET)
    display_host = "127.0.0.1" if DASHBOARD_HOST in {"0.0.0.0", "::"} else DASHBOARD_HOST

    with STATE_LOCK:
        active = [position_payload(symbol, position.copy()) for symbol, position in active_positions.items()]
        pending_entries = [{"order_id": order_id, "symbol": symbol} for order_id, symbol in pending_entry_orders.items()]
        pending_exits = [pending_exit_payload(order_id, state.copy()) for order_id, state in pending_exit_orders.items()]
        daily_trades = dict(daily_trade_state)

    with CONTEXT_LOCK:
        decisions_by_symbol = dict(daily_context.get("decisions") or {})
        decisions = [decisions_by_symbol.get(symbol, default_decision(symbol)) for symbol in SYMBOLS]
        recent_bars = [completed_bar_payload(symbol, bar.copy()) for symbol, bar in last_completed_bars.items()]
        context = {
            "session": daily_context.get("session"),
            "prior_session": daily_context.get("prior_session"),
            "prepared": bool(daily_context.get("prepared")),
            "prepared_at": daily_context.get("prepared_at"),
            "ready_count": sum(1 for decision in decisions if decision.get("status") == "ready"),
            "high_score_flow_count": sum(len(decision.get("flow_rows", [])) for decision in decisions),
            "last_attempt_seconds_ago": round(time.monotonic() - daily_context.get("last_attempt_monotonic", 0.0), 1),
            "account": daily_context.get("account") or {},
            "contract_previews_refreshed_at": daily_context.get("contract_previews_refreshed_at"),
        }

    return {
        "bot": {
            "name": "Flow Sweep Bot",
            "paper": PAPER,
            "symbols": SYMBOLS,
            "min_flow_score": MIN_FLOW_SCORE,
            "consensus_threshold": CONSENSUS_THRESHOLD,
            "trade_allocation_pct": TRADE_ALLOCATION_PCT,
            "target_delta": TARGET_DELTA,
            "option_preview_refresh_seconds": OPTION_PREVIEW_REFRESH_SECONDS,
            "entry_window": f"{ENTRY_WINDOW_START.strftime('%H:%M')}-{ENTRY_WINDOW_END.strftime('%H:%M')} ET",
            "dashboard_url": f"http://{display_host}:{DASHBOARD_PORT}",
        },
        "clock": {"now_et": now_et.isoformat()},
        "daily_context": context,
        "daily_trade_state": daily_trades,
        "decisions": decisions,
        "active_positions": active,
        "pending_entry_orders": pending_entries,
        "pending_exit_orders": pending_exits,
        "recent_5m_bars": sorted(recent_bars, key=lambda item: item.get("symbol") or ""),
    }


DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Flow Sweep Bot</title>
    <style>
        :root {
            color-scheme: light dark;
            --bg: #f7f8fa;
            --panel: #ffffff;
            --text: #1f2933;
            --muted: #637083;
            --border: #d9dee7;
            --bull: #0f766e;
            --bear: #b42318;
            --ready: #0f766e;
            --skip: #6b7280;
            --warn: #b54708;
        }
        @media (prefers-color-scheme: dark) {
            :root {
                --bg: #111827;
                --panel: #182230;
                --text: #e5e7eb;
                --muted: #a7b0c0;
                --border: #2f3b4d;
            }
        }
        * { box-sizing: border-box; }
        body {
            margin: 0;
            background: var(--bg);
            color: var(--text);
            font: 14px/1.45 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        }
        header {
            padding: 20px 24px 12px;
            border-bottom: 1px solid var(--border);
            background: var(--panel);
            position: sticky;
            top: 0;
            z-index: 2;
        }
        h1 { margin: 0 0 8px; font-size: 22px; letter-spacing: 0; }
        main { padding: 20px 24px 32px; display: grid; gap: 16px; }
        .meta { display: flex; flex-wrap: wrap; gap: 10px; color: var(--muted); }
        .pill {
            border: 1px solid var(--border);
            border-radius: 999px;
            padding: 4px 10px;
            background: color-mix(in srgb, var(--panel) 85%, var(--bg));
            white-space: nowrap;
        }
        .grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; }
        .panel {
            background: var(--panel);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 14px;
        }
        .panel h2 { margin: 0 0 10px; font-size: 16px; letter-spacing: 0; }
        .metric { color: var(--muted); font-size: 12px; text-transform: uppercase; }
        .metric-value { font-size: 20px; font-weight: 700; margin-top: 2px; }
        table { width: 100%; border-collapse: collapse; background: var(--panel); border: 1px solid var(--border); border-radius: 8px; overflow: hidden; }
        th, td { padding: 10px 8px; border-bottom: 1px solid var(--border); text-align: left; vertical-align: top; }
        th { color: var(--muted); font-size: 12px; text-transform: uppercase; background: color-mix(in srgb, var(--panel) 88%, var(--bg)); }
        tr:last-child td { border-bottom: 0; }
        .symbol { font-weight: 700; }
        .bullish { color: var(--bull); font-weight: 700; }
        .bearish { color: var(--bear); font-weight: 700; }
        .neutral { color: var(--muted); font-weight: 700; }
        .status-ready { color: var(--ready); font-weight: 700; }
        .status-flow_preview { color: var(--warn); font-weight: 700; }
        .status-skipped, .status-pending { color: var(--skip); font-weight: 700; }
        .status-error, .status-unavailable, .status-too_expensive { color: var(--warn); font-weight: 700; }
        .levels { display: flex; flex-wrap: wrap; gap: 6px; }
        .level { border: 1px solid var(--border); border-radius: 6px; padding: 3px 6px; white-space: nowrap; }
        .muted { color: var(--muted); }
        .key-level-grid { display: grid; grid-template-columns: repeat(2, minmax(145px, 1fr)); gap: 6px; min-width: 310px; }
        .key-level-card { border: 1px solid var(--border); border-radius: 6px; padding: 6px; background: color-mix(in srgb, var(--panel) 90%, var(--bg)); }
        .key-level-card.observed { border-color: var(--ready); background: color-mix(in srgb, var(--ready) 11%, var(--panel)); }
        .key-level-card.skipped { opacity: 0.72; }
        .key-level-card.pending { border-style: dashed; }
        .key-level-label { display: flex; justify-content: space-between; gap: 8px; font-weight: 700; }
        .key-level-price { font-variant-numeric: tabular-nums; }
        .key-level-action { margin-top: 3px; font-size: 12px; color: var(--muted); }
        .key-level-card.observed .key-level-action { color: var(--text); font-weight: 700; }
        .contract-preview { min-width: 260px; border: 1px solid var(--border); border-radius: 6px; padding: 8px; background: color-mix(in srgb, var(--panel) 91%, var(--bg)); }
        .contract-preview.ready { border-color: var(--ready); background: color-mix(in srgb, var(--ready) 9%, var(--panel)); }
        .contract-preview.error, .contract-preview.unavailable, .contract-preview.too_expensive { border-color: var(--warn); }
        .contract-title { font-weight: 800; font-variant-numeric: tabular-nums; }
        .contract-subtitle { margin-top: 2px; color: var(--muted); }
        .contract-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 6px 10px; margin-top: 8px; }
        .contract-label { color: var(--muted); font-size: 11px; text-transform: uppercase; }
        .contract-value { font-weight: 700; font-variant-numeric: tabular-nums; }
        .contract-note { margin-top: 7px; color: var(--muted); font-size: 12px; }
        .contract-warning { margin-top: 6px; color: var(--warn); font-size: 12px; font-weight: 700; }
        .flow-detail-row td { background: color-mix(in srgb, var(--panel) 92%, var(--bg)); padding-top: 0; }
        .flow-details summary { cursor: pointer; color: var(--muted); font-weight: 700; padding: 8px 0; }
        .flow-details[open] summary { color: var(--text); }
        .flow-table { margin: 6px 0 10px; border-radius: 6px; }
        .flow-table th, .flow-table td { font-size: 12px; padding: 7px 8px; }
        .reasons { max-width: 320px; white-space: normal; }
        .flag-list { display: flex; flex-wrap: wrap; gap: 4px; }
        .flag { border: 1px solid var(--border); border-radius: 6px; padding: 2px 5px; white-space: nowrap; }
        .section-title { margin: 8px 0 6px; font-size: 18px; }
        .wide { overflow-x: auto; }
        @media (max-width: 980px) { .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
        @media (max-width: 640px) { header, main { padding-left: 12px; padding-right: 12px; } .grid { grid-template-columns: 1fr; } th, td { padding: 8px 6px; } }
    </style>
</head>
<body>
    <header>
        <h1>Flow Sweep Bot</h1>
        <div class="meta" id="meta"></div>
    </header>
    <main>
        <section class="grid" id="metrics"></section>
        <section>
            <h2 class="section-title">Decision Board</h2>
            <div class="wide"><table id="decisions"></table></div>
        </section>
        <section>
            <h2 class="section-title">Active Positions</h2>
            <div class="wide"><table id="positions"></table></div>
        </section>
        <section>
            <h2 class="section-title">Recent 5m Bars</h2>
            <div class="wide"><table id="bars"></table></div>
        </section>
    </main>
    <script>
        const money = new Intl.NumberFormat(undefined, { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
        const price = new Intl.NumberFormat(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        const expandedFlowSymbols = new Set();
        function esc(value) {
            return String(value ?? '').replace(/[&<>'"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' }[c]));
        }
        function pct(value) { return value == null ? '-' : `${(Number(value) * 100).toFixed(1)}%`; }
        function usd(value) { return value == null ? '-' : money.format(Number(value)); }
        function px(value) { return value == null ? '-' : price.format(Number(value)); }
        function premium(value) { return value == null ? '-' : `$${Number(value).toFixed(2)}`; }
        function fixed(value, digits = 4) { return value == null ? '-' : Number(value).toFixed(digits); }
        function whole(value) { return value == null ? '-' : Number(value).toLocaleString(); }
        function expiry(value) {
            if (!value) return '-';
            const parts = String(value).split('-').map(Number);
            if (parts.length !== 3 || parts.some(Number.isNaN)) return esc(value);
            return esc(new Date(parts[0], parts[1] - 1, parts[2]).toLocaleDateString(undefined, { month: 'short', day: 'numeric' }));
        }
        function shortDateTime(value) {
            if (!value) return '-';
            const parsed = new Date(value);
            if (Number.isNaN(parsed.valueOf())) return esc(value);
            return esc(parsed.toLocaleString(undefined, { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' }));
        }
        function levels(items) {
            if (!items || !items.length) return '<span class="muted">none</span>';
            return `<div class="levels">${items.map(item => `<span class="level">${esc(item.name)} ${px(item.price)}</span>`).join('')}</div>`;
        }
        function keyLevelAction(level) {
            if (level.status === 'pending') return 'Pending premarket';
            if (level.status === 'missing') return 'Missing';
            if (level.role === 'observed' && level.side === 'support') return 'Observed: calls on close back above';
            if (level.role === 'observed' && level.side === 'resistance') return 'Observed: puts on close back below';
            return 'Skipped for this setup';
        }
        function keyLevels(decision) {
            const rows = decision.key_levels || [];
            if (!rows.length) return '<span class="muted">levels pending</span>';
            return `<div class="key-level-grid">${rows.map(level => {
                const priceText = level.price == null ? esc(level.status || '-') : px(level.price);
                const classes = ['key-level-card', level.role || 'skipped', level.status || 'ready', level.side || ''].join(' ');
                return `<div class="${classes}" title="${esc(level.note || '')}">
                    <div class="key-level-label"><span>${esc(level.label || level.name)}</span><span class="key-level-price">${priceText}</span></div>
                    <div class="key-level-action">${keyLevelAction(level)}</div>
                </div>`;
            }).join('')}</div>`;
        }
        function contractPreview(decision) {
            const preview = decision.contract_preview || {};
            if (!preview.symbol) {
                return `<span class="muted">${esc(preview.reason || 'none')}</span>`;
            }
            const status = preview.status || 'unknown';
            const title = `${decision.symbol} $${px(preview.strike)} ${preview.option_type || ''}`;
            const warnings = preview.warnings && preview.warnings.length ? `<div class="contract-warning">${preview.warnings.map(esc).join(' / ')}</div>` : '';
            const oneContract = preview.minimum_one_contract ? '<div class="contract-warning">Minimum 1 contract sizing override</div>' : '';
            return `<div class="contract-preview ${esc(status)}">
                <div class="contract-title">${esc(preview.symbol)}</div>
                <div class="contract-subtitle">${esc(title)} exp ${expiry(preview.expiration)}</div>
                <div class="contract-grid">
                    <div><div class="contract-label">Delta</div><div class="contract-value">${fixed(preview.delta, 4)}</div></div>
                    <div><div class="contract-label">Gamma</div><div class="contract-value">${fixed(preview.gamma, 4)}</div></div>
                    <div><div class="contract-label">Theta</div><div class="contract-value">${fixed(preview.theta, 4)}</div></div>
                    <div><div class="contract-label">Ask</div><div class="contract-value">${premium(preview.ask)}</div></div>
                    <div><div class="contract-label">Cost</div><div class="contract-value">${usd(preview.contract_cost)}</div></div>
                    <div><div class="contract-label">Qty</div><div class="contract-value">${esc(preview.quantity || 0)}</div></div>
                    <div><div class="contract-label">Spread</div><div class="contract-value">${pct(preview.spread_pct)}</div></div>
                    <div><div class="contract-label">Volume</div><div class="contract-value">${whole(preview.volume)}</div></div>
                    <div><div class="contract-label">OI</div><div class="contract-value">${whole(preview.open_interest)}</div></div>
                </div>
                <div class="contract-note">${esc(status)} / ${usd(preview.allocation_amount)} allocation / ${usd(preview.account_balance)} balance</div>
                <div class="contract-note">${esc(preview.reason || '')} / candidates ${esc(preview.candidate_count || 0)}</div>
                ${oneContract}${warnings}
            </div>`;
        }
        function contractText(row) {
            const parts = [row.option_type, row.strike, row.expiry].filter(Boolean).map(esc);
            const extra = [row.side && `Side ${esc(row.side)}`, row.size && `Size ${esc(row.size)}`, row.price && `Price ${esc(row.price)}`].filter(Boolean);
            if (!parts.length && !extra.length) return '<span class="muted">aggregate score row</span>';
            return `${parts.join(' ')}${extra.length ? `<br><span class="muted">${extra.join(' / ')}</span>` : ''}`;
        }
        function flags(row) {
            const items = [];
            if (row.tier) items.push(row.tier);
            if (row.is_sweep) items.push(`Sweeps ${row.sweep_count || 1}`);
            if (row.repeat_hit_count) items.push(`Repeats ${row.repeat_hit_count}`);
            if (row.ask_side_pct != null) items.push(`Ask ${pct(row.ask_side_pct)}`);
            if (row.cross_expiry_cluster) items.push('Cross expiry');
            if (row.dark_pool_confirmed) items.push('Dark pool');
            if (row.alerts_ingested) items.push(`Alerts ${row.alerts_ingested}`);
            return items.length ? `<div class="flag-list">${items.map(item => `<span class="flag">${esc(item)}</span>`).join('')}</div>` : '<span class="muted">-</span>';
        }
        function toggleFlowDetail(element) {
            const symbol = element.dataset.symbol;
            if (!symbol) return;
            if (element.open) expandedFlowSymbols.add(symbol);
            else expandedFlowSymbols.delete(symbol);
        }
        function flowDetails(decision) {
            const rows = decision.flow_rows || [];
            if (!rows.length) {
                return `<tr class="flow-detail-row"><td></td><td colspan="6"><span class="muted">No >70 flow rows from the prior session.</span></td></tr>`;
            }
            const openAttr = expandedFlowSymbols.has(decision.symbol) ? ' open' : '';
            const body = rows.map(row => {
                const directionClass = row.direction === 'bullish' ? 'bullish' : row.direction === 'bearish' ? 'bearish' : 'neutral';
                const reasons = row.reasons && row.reasons.length ? row.reasons.map(esc).join(', ') : '-';
                return `<tr>
                    <td>${shortDateTime(row.scored_at)}</td>
                    <td><strong>${esc(row.score)}</strong><br><span class="muted">${esc(row.tier || '')}</span></td>
                    <td><span class="${directionClass}">${esc(row.direction)}</span><br><span class="muted">Conf ${pct(row.confidence)}</span></td>
                    <td>${usd(row.premium)}<br><span class="muted">Largest ${usd(row.largest_premium)}</span></td>
                    <td>${px(row.spot_price)}</td>
                    <td>${contractText(row)}</td>
                    <td>${flags(row)}</td>
                    <td class="reasons">${esc(reasons)}</td>
                </tr>`;
            }).join('');
            return `<tr class="flow-detail-row"><td></td><td colspan="6">
                <details class="flow-details" data-symbol="${esc(decision.symbol)}" ontoggle="toggleFlowDetail(this)"${openAttr}>
                    <summary>${esc(decision.symbol)} high-score flow from prior session (${rows.length})</summary>
                    <table class="flow-table">
                        <thead><tr><th>Time</th><th>Score</th><th>Bias</th><th>Premium</th><th>Spot</th><th>Contract</th><th>Flags</th><th>Reasons</th></tr></thead>
                        <tbody>${body}</tbody>
                    </table>
                </details>
            </td></tr>`;
        }
        function table(elementId, headers, rows, emptyText) {
            const el = document.getElementById(elementId);
            if (!rows.length) {
                el.innerHTML = `<tbody><tr><td class="muted">${esc(emptyText)}</td></tr></tbody>`;
                return;
            }
            el.innerHTML = `<thead><tr>${headers.map(h => `<th>${esc(h)}</th>`).join('')}</tr></thead><tbody>${rows.join('')}</tbody>`;
        }
        function metric(label, value) {
            return `<div class="panel"><div class="metric">${esc(label)}</div><div class="metric-value">${esc(value)}</div></div>`;
        }
        async function refresh() {
            const response = await fetch('/api/status', { cache: 'no-store' });
            const data = await response.json();
            const ready = data.daily_context.ready_count || 0;
            document.getElementById('meta').innerHTML = [
                `Now ${esc(new Date(data.clock.now_et).toLocaleString())}`,
                `Session ${esc(data.daily_context.session || '-')}`,
                `Prior ${esc(data.daily_context.prior_session || '-')}`,
                `Mode ${data.bot.paper ? 'paper' : 'live'}`,
                `Entry ${esc(data.bot.entry_window)}`,
                `Refresh 5s`,
                `Contracts ${shortDateTime(data.daily_context.contract_previews_refreshed_at)}`
            ].map(item => `<span class="pill">${item}</span>`).join('');
            document.getElementById('metrics').innerHTML = [
                metric('Ready Setups', ready),
                metric('High-Score Flow Rows', data.daily_context.high_score_flow_count || 0),
                metric('Active Positions', data.active_positions.length),
                metric('Pending Orders', data.pending_entry_orders.length + data.pending_exit_orders.length),
                metric('Account Balance', usd(data.daily_context.account && data.daily_context.account.account_balance)),
                metric('Allocation', `${(data.bot.trade_allocation_pct * 100).toFixed(1)}% balance`)
            ].join('');
            table('decisions', ['Symbol', 'Decision', 'Consensus', 'Key Levels', 'Planned Contract', 'Targets', 'Reason'], data.decisions.flatMap(d => {
                const directionClass = d.direction === 'bullish' ? 'bullish' : d.direction === 'bearish' ? 'bearish' : 'neutral';
                const mainRow = `<tr>
                    <td class="symbol">${esc(d.symbol)}</td>
                    <td><span class="${directionClass}">${esc(d.direction)}</span><br><span class="status-${esc(d.status)}">${esc(d.status)}</span><br><span class="muted">${esc(d.option_type || '-')} score ${esc(d.top_score || 0)}</span></td>
                    <td>${pct(d.consensus)}<br><span class="muted">Bull ${usd(d.bullish_premium)} / Bear ${usd(d.bearish_premium)}</span></td>
                    <td>${keyLevels(d)}</td>
                    <td>${contractPreview(d)}</td>
                    <td>${levels(d.target_levels)}</td>
                    <td>${esc(d.reason || '')}<br><span class="muted">Rows ${esc(d.directional_row_count || 0)} of ${esc(d.raw_row_count || 0)}</span></td>
                </tr>`;
                return [mainRow, flowDetails(d)];
            }), 'No decisions prepared yet.');
            table('positions', ['Symbol', 'Option', 'Qty', 'Stop', 'Target', 'Status'], data.active_positions.map(p => `<tr>
                <td class="symbol">${esc(p.symbol)}</td>
                <td>${esc(p.option_symbol || '-')}<br><span class="muted">${esc(p.option_type || '-')}</span></td>
                <td>${esc(p.total_qty)} / ${esc(p.requested_qty)}</td>
                <td>${px(p.stop_underlying)}</td>
                <td>${esc(p.target_name || '-')} ${px(p.target_underlying)}</td>
                <td>${esc(p.entry_status || '-')}<br><span class="muted">Swept ${esc(p.swept_level || '-')}</span></td>
            </tr>`), 'No active positions.');
            table('bars', ['Symbol', 'Close Time', 'O/H/L/C', 'Volume'], data.recent_5m_bars.map(b => `<tr>
                <td class="symbol">${esc(b.symbol)}</td>
                <td>${b.close_time ? esc(new Date(b.close_time).toLocaleTimeString()) : '-'}</td>
                <td>${px(b.open)} / ${px(b.high)} / ${px(b.low)} / ${px(b.close)}</td>
                <td>${esc(b.volume ?? '-')}</td>
            </tr>`), 'No completed 5-minute bars yet.');
        }
        refresh().catch(console.error);
        setInterval(() => refresh().catch(console.error), 5000);
    </script>
</body>
</html>
"""


class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlparse(self.path).path
        if path in {"/", "/index.html"}:
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(DASHBOARD_HTML.encode("utf-8"))
            return

        if path == "/api/status":
            payload = dashboard_status_payload()
            body = json.dumps(payload, default=str).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(b'{"ok": true}')
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, _format, *args):
        LOGGER.debug("Dashboard request: " + _format, *args)


def start_dashboard_thread():
    if not DASHBOARD_ENABLED:
        LOGGER.info("Dashboard disabled by BOT_DASHBOARD_ENABLED=0")
        return None

    try:
        server = ThreadingHTTPServer((DASHBOARD_HOST, DASHBOARD_PORT), DashboardHandler)
    except OSError as exc:
        LOGGER.error("Dashboard failed to start on %s:%s: %s", DASHBOARD_HOST, DASHBOARD_PORT, exc)
        return None

    thread = threading.Thread(target=server.serve_forever, name="flow-sweep-dashboard", daemon=True)
    thread.start()
    display_host = "127.0.0.1" if DASHBOARD_HOST in {"0.0.0.0", "::"} else DASHBOARD_HOST
    LOGGER.info("Dashboard running at http://%s:%s", display_host, DASHBOARD_PORT)
    return server
