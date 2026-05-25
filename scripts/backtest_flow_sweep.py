#!/usr/bin/env python3
import argparse
import csv
import math
import sys
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path

import pandas as pd

PROJECT_DIR = Path(__file__).resolve().parents[1]
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from flow_sweep.config import (  # noqa: E402
    BREAKEVEN_TRIGGER_R_MULTIPLE,
    ENTRY_LEVEL_CLEARANCE_MIN_RANGE_PCT,
    ENTRY_MAX_TARGET_R_MULTIPLE,
    ENTRY_RECLAIM_CLOSE_MIN_RANGE_PCT,
    ENTRY_WINDOW_END,
    ENTRY_WINDOW_START,
    ET,
    PREMARKET_START,
    REGULAR_OPEN,
    RUNTIME_DIR,
    SYMBOLS,
    TARGET_R_MULTIPLE,
    UTC,
    configure_logging,
)
from flow_sweep.flow_data import query_flow_scores, summarize_flow_rows  # noqa: E402
from flow_sweep.market_data import daily_bars, get_schedule, intraday_bars, previous_calendar_week_sessions  # noqa: E402
from flow_sweep.strategy import (  # noqa: E402
    apply_level_roles,
    age_active_continuation_zone,
    build_continuation_zone,
    confirmation_entry_ready,
    continuation_entry_metadata,
    continuation_zone_status,
    entry_risk_quality_reason,
    new_continuation_context,
    setup_from_key_levels,
    sweep_entry_metadata,
    swept_level_for_bar,
    target_level_for_entry,
    trim_tail,
    update_continuation_swings,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Backtest the Flow Sweep entry/exit logic with UW flow and underlying price data.")
    parser.add_argument("--sessions", type=int, default=20, help="Number of completed NYSE sessions to test.")
    parser.add_argument("--end-date", help="Last session date to include, YYYY-MM-DD. Defaults to latest completed session.")
    parser.add_argument("--symbols", help="Comma-separated symbol list. Defaults to the bot watchlist.")
    parser.add_argument("--output-dir", default=str(RUNTIME_DIR / "backtests"), help="Directory for markdown and CSV logs.")
    return parser.parse_args()


def session_dates(schedule):
    return [idx.date() for idx in schedule.index]


def completed_session_indices(schedule, explicit_end_date=None):
    dates = session_dates(schedule)
    if explicit_end_date:
        return [idx for idx, session_day in enumerate(dates) if session_day <= explicit_end_date]

    now_utc = datetime.now(UTC)
    completed = []
    for idx, session_day in enumerate(dates):
        market_close = schedule.iloc[idx]["market_close"].to_pydatetime().astimezone(UTC)
        if market_close <= now_utc:
            completed.append(idx)
    return completed


def selected_sessions(count, explicit_end_date=None):
    end_day = explicit_end_date or datetime.now(ET).date()
    start_day = end_day - timedelta(days=max(count * 4, 90))
    schedule = get_schedule(start_day, end_day)
    completed = completed_session_indices(schedule, explicit_end_date=explicit_end_date)
    completed = [idx for idx in completed if idx > 0]
    if not completed:
        raise RuntimeError("No completed sessions with a prior session were available for backtest.")
    return schedule, completed[-count:]


def day_slice(bars, session_day, start_time, end_time):
    if bars.empty:
        return pd.DataFrame()
    start = datetime.combine(session_day, start_time, ET)
    end = datetime.combine(session_day, end_time, ET)
    return bars[(bars.index >= start) & (bars.index < end)].copy()


def high_low(bars):
    if bars.empty:
        return None
    return float(bars["high"].max()), float(bars["low"].min())


def daily_high_low(daily, intraday, session_day):
    if not daily.empty:
        rows = daily[daily.index.date == session_day]
        if not rows.empty:
            return high_low(rows)
    return high_low(day_slice(intraday, session_day, REGULAR_OPEN, dt_time(16, 0)))


def week_high_low(daily, intraday, session_days):
    if not session_days:
        return None
    session_set = set(session_days)
    if not daily.empty:
        rows = daily[[idx.date() in session_set for idx in daily.index]]
        if not rows.empty:
            return high_low(rows)
    rows = pd.concat([day_slice(intraday, day, REGULAR_OPEN, dt_time(16, 0)) for day in session_days])
    return high_low(rows)


def level_payload(name, label, side, price=None, status="ready", role="skipped", note=None):
    return {
        "name": name,
        "label": label,
        "side": side,
        "price": round(float(price), 4) if price not in (None, "") else None,
        "status": status,
        "role": role,
        "note": note,
    }


def missing_level(name, label, side):
    return level_payload(name, label, side, status="missing", note="Level unavailable")


def build_backtest_key_levels(symbol, session_day, prior_day, week_days, daily, intraday):
    premarket_range = high_low(day_slice(intraday, session_day, PREMARKET_START, REGULAR_OPEN))
    prior_range = daily_high_low(daily, intraday, prior_day)
    week_range = week_high_low(daily, intraday, week_days)

    if premarket_range:
        premarket_high, premarket_low = premarket_range
        premarket_levels = [
            level_payload("premarket_low", "Premarket Low", "support", premarket_low),
            level_payload("premarket_high", "Premarket High", "resistance", premarket_high),
        ]
    else:
        premarket_levels = [
            missing_level("premarket_low", "Premarket Low", "support"),
            missing_level("premarket_high", "Premarket High", "resistance"),
        ]

    if prior_range:
        prior_high, prior_low = prior_range
        prior_levels = [
            level_payload("prior_day_low", "Prior Day Low", "support", prior_low),
            level_payload("prior_day_high", "Prior Day High", "resistance", prior_high),
        ]
    else:
        prior_levels = [
            missing_level("prior_day_low", "Prior Day Low", "support"),
            missing_level("prior_day_high", "Prior Day High", "resistance"),
        ]

    if week_range:
        week_high, week_low = week_range
        week_levels = [
            level_payload("prior_week_low", "Prior Week Low", "support", week_low),
            level_payload("prior_week_high", "Prior Week High", "resistance", week_high),
        ]
    else:
        week_levels = [
            missing_level("prior_week_low", "Prior Week Low", "support"),
            missing_level("prior_week_high", "Prior Week High", "resistance"),
        ]

    return premarket_levels + prior_levels + week_levels


def five_minute_bars(intraday, session_day):
    regular = day_slice(intraday, session_day, REGULAR_OPEN, dt_time(16, 0))
    if regular.empty:
        return pd.DataFrame()
    five = regular.resample("5min", label="left", closed="left").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    )
    five = five.dropna(subset=["open", "high", "low", "close"])
    five["bucket"] = five.index
    five["close_time"] = five.index + pd.Timedelta(minutes=5)
    return five


def bar_dict(row):
    return {
        "bucket": row["bucket"].to_pydatetime(),
        "close_time": row["close_time"].to_pydatetime(),
        "open": float(row["open"]),
        "high": float(row["high"]),
        "low": float(row["low"]),
        "close": float(row["close"]),
        "volume": float(row.get("volume", 0) or 0),
    }


def in_entry_window(close_time):
    return ENTRY_WINDOW_START <= close_time.time() <= ENTRY_WINDOW_END


def exit_r(option_type, entry, exit_price, risk):
    if option_type == "CALL":
        return (exit_price - entry) / risk
    return (entry - exit_price) / risk


def simulate_exit(option_type, entry_bar_index, bars, entry, stop, target, breakeven_trigger):
    risk = entry - stop if option_type == "CALL" else stop - entry
    if risk <= 0:
        return None

    breakeven_active = False
    rows = list(bars.iloc[entry_bar_index + 1 :].iterrows())
    if not rows:
        return {"exit_reason": "NO_FUTURE_BARS", "exit_price": entry, "exit_time": None, "r_multiple": 0.0, "breakeven_active": False}

    last_bar = None
    for _timestamp, row in rows:
        current = bar_dict(row)
        last_bar = current
        if option_type == "CALL":
            target_hit = current["high"] >= target
            stop_hit = current["low"] <= stop
            be_trigger_hit = current["high"] >= breakeven_trigger
            be_stop_hit = current["low"] <= entry
        else:
            target_hit = current["low"] <= target
            stop_hit = current["high"] >= stop
            be_trigger_hit = current["low"] <= breakeven_trigger
            be_stop_hit = current["high"] >= entry

        if target_hit:
            return {
                "exit_reason": "TARGET",
                "exit_price": target,
                "exit_time": current["close_time"],
                "r_multiple": exit_r(option_type, entry, target, risk),
                "breakeven_active": breakeven_active,
            }

        if not breakeven_active and stop_hit:
            return {
                "exit_reason": "STOP",
                "exit_price": stop,
                "exit_time": current["close_time"],
                "r_multiple": -1.0,
                "breakeven_active": False,
            }

        if breakeven_active and be_stop_hit:
            return {
                "exit_reason": "BREAKEVEN",
                "exit_price": entry,
                "exit_time": current["close_time"],
                "r_multiple": 0.0,
                "breakeven_active": True,
            }

        if not breakeven_active and be_trigger_hit:
            breakeven_active = True

        if current["close_time"].time() >= dt_time(15, 55):
            return {
                "exit_reason": "EOD",
                "exit_price": current["close"],
                "exit_time": current["close_time"],
                "r_multiple": exit_r(option_type, entry, current["close"], risk),
                "breakeven_active": breakeven_active,
            }

    return {
        "exit_reason": "LAST_BAR",
        "exit_price": last_bar["close"],
        "exit_time": last_bar["close_time"],
        "r_multiple": exit_r(option_type, entry, last_bar["close"], risk),
        "breakeven_active": breakeven_active,
    }


def backtest_symbol(symbol, schedule, session_indices):
    dates = session_dates(schedule)
    first_day = dates[min(session_indices)] - timedelta(days=14)
    last_day = dates[max(session_indices)]
    start_et = datetime.combine(first_day, PREMARKET_START, ET)
    end_et = datetime.combine(last_day + timedelta(days=1), dt_time(16, 5), ET)
    intraday = intraday_bars(symbol, start_et, end_et, prepost=True)
    daily = daily_bars(symbol, first_day, last_day)

    rows = []
    for session_idx in session_indices:
        session_day = dates[session_idx]
        prior_day = dates[session_idx - 1]
        prior_open = schedule.iloc[session_idx - 1]["market_open"].to_pydatetime().astimezone(UTC)
        prior_close = schedule.iloc[session_idx - 1]["market_close"].to_pydatetime().astimezone(UTC)
        flow_rows = query_flow_scores(symbol, prior_open, prior_close)
        bias, decision = summarize_flow_rows(symbol, flow_rows)

        base = {
            "session": session_day.isoformat(),
            "prior_session": prior_day.isoformat(),
            "symbol": symbol,
            "flow_rows": len(flow_rows),
            "direction": decision.get("direction"),
            "consensus": decision.get("consensus"),
            "top_score": decision.get("top_score"),
            "bullish_premium": decision.get("bullish_premium"),
            "bearish_premium": decision.get("bearish_premium"),
        }

        if not bias:
            rows.append({**base, "status": "NO_BIAS", "reason": decision.get("reason")})
            continue

        week_days = previous_calendar_week_sessions(schedule, session_day)
        key_levels = build_backtest_key_levels(symbol, session_day, prior_day, week_days, daily, intraday)
        role_levels = apply_level_roles(key_levels, bias)
        setup = setup_from_key_levels(symbol, bias, role_levels)
        if not setup:
            rows.append({**base, "status": "NO_SETUP", "reason": "Missing one or more chart levels"})
            continue

        bars = five_minute_bars(intraday, session_day)
        if bars.empty:
            rows.append({**base, "status": "NO_PRICE", "reason": "No 5m regular-session bars"})
            continue

        entry_found = False
        pending_signal = None
        continuation_context = new_continuation_context()
        for bar_index, (_timestamp, row) in enumerate(bars.iterrows()):
            signal_bar = bar_dict(row)
            close_clock = signal_bar["close_time"].time()
            if close_clock > ENTRY_WINDOW_END:
                pending_signal = None
                break
            if close_clock < ENTRY_WINDOW_START:
                continue

            candidates = []
            if pending_signal:
                swept = pending_signal["swept_level"]
                sweep_bar = pending_signal["signal_bar"]
                ready, confirmation_reason = confirmation_entry_ready(setup, swept, sweep_bar, signal_bar)
                if ready:
                    candidates.append(
                        {
                            "armed_at": sweep_bar["close_time"],
                            "metadata": sweep_entry_metadata(setup, swept, signal_bar, sweep_bar),
                        }
                    )
                else:
                    rows.append(
                        {
                            **base,
                            "status": "SKIPPED",
                            "setup_type": "flow_sweep",
                            "reason": f"Confirmation failed: {confirmation_reason}",
                            "sweep_signal_time": sweep_bar["close_time"].isoformat(),
                            "entry_time": signal_bar["close_time"].isoformat(),
                            "swept_level": swept.name,
                            "swept_level_price": swept.price,
                        }
                    )
                pending_signal = None

            active_zone = continuation_context.get("active_zone")
            if active_zone:
                zone_status, zone_reason = continuation_zone_status(setup, active_zone, signal_bar)
                if zone_status == "ready":
                    candidates.append({"armed_at": active_zone["armed_at"], "metadata": continuation_entry_metadata(active_zone, signal_bar)})
                elif zone_status == "invalidated":
                    rows.append(
                        {
                            **base,
                            "status": "SKIPPED",
                            "setup_type": "continuation_fvg",
                            "reason": zone_reason,
                            "sweep_signal_time": active_zone["armed_at"].isoformat(),
                            "entry_time": signal_bar["close_time"].isoformat(),
                            "swept_level": active_zone["signal_name"],
                            "swept_level_price": round((active_zone["zone_low"] + active_zone["zone_high"]) / 2, 4),
                            "continuation_zone_low": active_zone["zone_low"],
                            "continuation_zone_high": active_zone["zone_high"],
                            "continuation_structure_price": active_zone["structure_price"],
                        }
                    )
                    continuation_context["active_zone"] = None

            if candidates:
                candidates.sort(
                    key=lambda item: (
                        item["armed_at"].isoformat() if hasattr(item.get("armed_at"), "isoformat") else "9999-12-31T23:59:59+00:00",
                        item["metadata"].get("setup_type") != "flow_sweep",
                    )
                )
                option_type = "CALL" if bias.direction == "bullish" else "PUT"
                for candidate in candidates:
                    metadata = candidate["metadata"]
                    stop = float(metadata["stop_underlying"])
                    target_level, risk_plan = target_level_for_entry(setup, option_type, signal_bar["close"], stop)
                    if not target_level or not risk_plan:
                        rows.append(
                            {
                                **base,
                                "status": "SKIPPED",
                                "setup_type": metadata["setup_type"],
                                "reason": "Invalid entry/stop risk",
                                "entry_time": signal_bar["close_time"].isoformat(),
                                "swept_level": metadata["signal_name"],
                                "swept_level_price": metadata.get("signal_price"),
                            }
                        )
                        continue

                    quality_reason = entry_risk_quality_reason(risk_plan)
                    if quality_reason:
                        rows.append(
                            {
                                **base,
                                "status": "SKIPPED",
                                "setup_type": metadata["setup_type"],
                                "reason": quality_reason,
                                "entry_time": signal_bar["close_time"].isoformat(),
                                "entry_price": round(signal_bar["close"], 4),
                                "stop_price": round(stop, 4),
                                "swept_level": metadata["signal_name"],
                                "swept_level_price": metadata.get("signal_price"),
                                "target_name": target_level.name,
                                "target_price": round(target_level.price, 4),
                                "target_r": risk_plan["target_r_multiple"],
                            }
                        )
                        continue

                    exit_result = simulate_exit(
                        option_type,
                        bar_index,
                        bars,
                        signal_bar["close"],
                        stop,
                        target_level.price,
                        risk_plan["breakeven_trigger_underlying"],
                    )
                    if not exit_result:
                        rows.append({**base, "status": "SKIPPED", "setup_type": metadata["setup_type"], "reason": "Invalid simulated risk"})
                        continue

                    rows.append(
                        {
                            **base,
                            "status": "TRADE",
                            "setup_type": metadata["setup_type"],
                            "option_type": option_type,
                            "swept_level": metadata["signal_name"],
                            "swept_level_price": metadata.get("signal_price"),
                            "sweep_signal_time": metadata.get("signal_time").isoformat() if hasattr(metadata.get("signal_time"), "isoformat") else metadata.get("signal_time"),
                            "entry_time": signal_bar["close_time"].isoformat(),
                            "entry_price": round(signal_bar["close"], 4),
                            "stop_price": round(stop, 4),
                            "target_name": target_level.name,
                            "target_price": round(target_level.price, 4),
                            "risk": risk_plan["risk_underlying"],
                            "target_r": risk_plan["target_r_multiple"],
                            "breakeven_trigger": risk_plan["breakeven_trigger_underlying"],
                            "exit_time": exit_result["exit_time"].isoformat() if exit_result["exit_time"] else None,
                            "exit_price": round(exit_result["exit_price"], 4),
                            "exit_reason": exit_result["exit_reason"],
                            "r_multiple": round(exit_result["r_multiple"], 4),
                            "breakeven_active": exit_result["breakeven_active"],
                            **(metadata.get("event_fields") or {}),
                        }
                    )
                    entry_found = True
                    break

                if entry_found:
                    break

            expired_zone = age_active_continuation_zone(continuation_context)
            if expired_zone:
                rows.append(
                    {
                        **base,
                        "status": "SKIPPED",
                        "setup_type": "continuation_fvg",
                        "reason": "Continuation FVG expired before a valid touch-and-close entry",
                        "sweep_signal_time": expired_zone["armed_at"].isoformat(),
                        "swept_level": expired_zone["signal_name"],
                        "swept_level_price": round((expired_zone["zone_low"] + expired_zone["zone_high"]) / 2, 4),
                        "continuation_zone_low": expired_zone["zone_low"],
                        "continuation_zone_high": expired_zone["zone_high"],
                        "continuation_structure_price": expired_zone["structure_price"],
                    }
                )

            continuation_context["recent_bars"].append(signal_bar.copy())
            trim_tail(continuation_context["recent_bars"])
            update_continuation_swings(continuation_context)

            new_zone = build_continuation_zone(setup, continuation_context, signal_bar)
            if new_zone and (not continuation_context.get("active_zone") or new_zone["armed_at"] >= continuation_context["active_zone"]["armed_at"]):
                continuation_context["active_zone"] = new_zone

            swept = swept_level_for_bar(setup, signal_bar)
            if not swept:
                continue

            pending_signal = {"swept_level": swept, "signal_bar": signal_bar, "bar_index": bar_index}

        if not entry_found:
            rows.append({**base, "status": "NO_ENTRY", "reason": "No confirmed sweep or continuation FVG entry in entry window"})

    return rows


def summarize(rows):
    trades = [row for row in rows if row.get("status") == "TRADE"]
    wins = [row for row in trades if row.get("r_multiple", 0) > 0]
    losses = [row for row in trades if row.get("r_multiple", 0) < 0]
    breakeven = [row for row in trades if row.get("r_multiple", 0) == 0]
    gross_win = sum(row["r_multiple"] for row in wins)
    gross_loss = abs(sum(row["r_multiple"] for row in losses))
    profit_factor = gross_win / gross_loss if gross_loss else math.inf if gross_win else 0.0
    return {
        "rows": len(rows),
        "trades": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "breakeven": len(breakeven),
        "win_rate": len(wins) / len(trades) if trades else 0.0,
        "profit_factor": profit_factor,
        "total_r": sum(row["r_multiple"] for row in trades),
        "avg_r": sum(row["r_multiple"] for row in trades) / len(trades) if trades else 0.0,
    }


def summarize_by_symbol(rows):
    summaries = []
    for symbol in sorted({row["symbol"] for row in rows}):
        symbol_rows = [row for row in rows if row["symbol"] == symbol]
        summary = summarize(symbol_rows)
        summary["symbol"] = symbol
        summary["setups"] = sum(1 for row in symbol_rows if row.get("status") in {"TRADE", "NO_ENTRY"})
        summaries.append(summary)
    return summaries


def fmt_pct(value):
    return f"{value * 100:.1f}%"


def fmt_number(value):
    if value == math.inf:
        return "inf"
    return f"{value:.2f}"


def write_outputs(rows, output_dir, started_at, sessions, symbols):
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = started_at.strftime("%Y%m%d_%H%M%S")
    csv_path = output_dir / f"flow_sweep_backtest_{stamp}.csv"
    md_path = output_dir / f"flow_sweep_backtest_{stamp}.md"

    fieldnames = sorted({key for row in rows for key in row.keys()})
    with csv_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    overall = summarize(rows)
    by_symbol = summarize_by_symbol(rows)
    status_counts = pd.Series([row.get("status") for row in rows]).value_counts().to_dict()
    exit_counts = pd.Series([row.get("exit_reason") for row in rows if row.get("status") == "TRADE"]).value_counts().to_dict()
    setup_counts = pd.Series([row.get("setup_type", "unknown") for row in rows if row.get("status") == "TRADE"]).value_counts().to_dict()

    lines = [
        "# Flow Sweep Backtest",
        "",
        f"Generated: {started_at.astimezone(ET).isoformat()}",
        f"Sessions: {sessions[0].isoformat()} through {sessions[-1].isoformat()} ({len(sessions)} sessions)",
        f"Symbols: {', '.join(symbols)}",
        "",
        "## Assumptions",
        "",
        "- Uses UW scored-flow rows from each prior regular session and underlying OHLC price data for the tested session.",
        f"- Entries are only considered from {ENTRY_WINDOW_START.strftime('%H:%M')} through {ENTRY_WINDOW_END.strftime('%H:%M')} ET.",
        "- Entry can come from either the confirmed level-sweep model or the continuation FVG model, with the earliest armed valid setup winning for that symbol.",
        "- Sweep entries require a meaningful sweep/reclaim candle followed by one confirming 5-minute candle.",
        "- Continuation entries require a 5-minute break of structure with displacement, an armed fair value gap, and then a touch plus directional confirming close back out of the gap.",
        f"- Sweep and confirmation strength uses a {fmt_pct(ENTRY_RECLAIM_CLOSE_MIN_RANGE_PCT)} candle-range close test, and sweep candles must clear the level by at least {fmt_pct(ENTRY_LEVEL_CLEARANCE_MIN_RANGE_PCT)} of candle range.",
        f"- Planned targets above {ENTRY_MAX_TARGET_R_MULTIPLE:.2f}R are skipped as low-quality/tight-risk entries.",
        "- Results are measured in underlying R multiples, not historical option premium PnL.",
        "- Breakeven stop is approximated as an underlying entry-price stop after the 1.5R trigger because historical option bid/ask marks are not replayed here.",
        "- Intrabar conflicts follow the live bot priority: target first, then initial stop, then breakeven activation/stop, then EOD.",
        "",
        "## Summary",
        "",
        f"- Trades: {overall['trades']}",
        f"- Wins / Losses / Breakeven: {overall['wins']} / {overall['losses']} / {overall['breakeven']}",
        f"- Win rate: {fmt_pct(overall['win_rate'])}",
        f"- Profit factor: {fmt_number(overall['profit_factor'])}",
        f"- Total R: {overall['total_r']:.2f}",
        f"- Average R: {overall['avg_r']:.2f}",
        f"- Status counts: {status_counts}",
        f"- Exit counts: {exit_counts}",
        f"- Setup counts: {setup_counts}",
        "",
        "## By Symbol",
        "",
        "| Symbol | Setups | Trades | Win Rate | Profit Factor | Total R | Avg R |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for item in by_symbol:
        lines.append(
            f"| {item['symbol']} | {item['setups']} | {item['trades']} | {fmt_pct(item['win_rate'])} | {fmt_number(item['profit_factor'])} | {item['total_r']:.2f} | {item['avg_r']:.2f} |"
        )

    lines.extend(
        [
            "",
            "## Trade Log",
            "",
            "| Session | Symbol | Setup | Bias | Entry | Exit | Result | Target | Flow |",
            "|---|---|---|---|---|---|---:|---|---:|",
        ]
    )
    for row in rows:
        if row.get("status") != "TRADE":
            continue
        entry = f"{row.get('entry_time', '')[-14:-6]} @ {row.get('entry_price')}"
        exit_text = f"{str(row.get('exit_time') or '')[-14:-6]} {row.get('exit_reason')} @ {row.get('exit_price')}"
        target = f"{row.get('target_name')} {row.get('target_price')} ({row.get('target_r')}R)"
        lines.append(
            f"| {row['session']} | {row['symbol']} | {row.get('setup_type', 'flow_sweep')} | {row.get('direction')} {row.get('option_type')} | {entry} | {exit_text} | {row.get('r_multiple'):.2f} | {target} | {row.get('flow_rows')} |"
        )

    lines.extend(
        [
            "",
            "## Non-Trade Log",
            "",
            "| Session | Symbol | Status | Setup | Bias | Reason | Flow Rows |",
            "|---|---|---|---|---|---|---:|",
        ]
    )
    for row in rows:
        if row.get("status") == "TRADE":
            continue
        lines.append(
            f"| {row['session']} | {row['symbol']} | {row.get('status')} | {row.get('setup_type', '-')} | {row.get('direction')} | {row.get('reason', '')} | {row.get('flow_rows')} |"
        )

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return md_path, csv_path, overall


def main():
    args = parse_args()
    configure_logging()
    explicit_end = datetime.strptime(args.end_date, "%Y-%m-%d").date() if args.end_date else None
    symbols = [symbol.strip().upper() for symbol in args.symbols.split(",")] if args.symbols else SYMBOLS
    started_at = datetime.now(ET)
    schedule, indices = selected_sessions(args.sessions, explicit_end_date=explicit_end)
    dates = session_dates(schedule)
    tested_sessions = [dates[idx] for idx in indices]

    rows = []
    for symbol in symbols:
        print(f"Backtesting {symbol} across {len(indices)} sessions...", flush=True)
        rows.extend(backtest_symbol(symbol, schedule, indices))

    md_path, csv_path, overall = write_outputs(rows, Path(args.output_dir), started_at, tested_sessions, symbols)
    print(f"Backtest markdown: {md_path}")
    print(f"Backtest CSV: {csv_path}")
    print(
        "Summary: "
        f"trades={overall['trades']} wins={overall['wins']} losses={overall['losses']} "
        f"breakeven={overall['breakeven']} win_rate={fmt_pct(overall['win_rate'])} "
        f"profit_factor={fmt_number(overall['profit_factor'])} total_r={overall['total_r']:.2f} avg_r={overall['avg_r']:.2f}"
    )


if __name__ == "__main__":
    main()