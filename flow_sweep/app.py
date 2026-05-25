import argparse
import threading

from .clients import stock_stream
from .config import LOGGER, SYMBOLS, configure_logging, validate_configuration
from .dashboard import dashboard_status_payload, start_dashboard_thread
from .option_selection import validate_entry_contract
from .state import load_state_from_disk
from .strategy import handle_bar, log_startup_context, prepare_daily_context, reconcile_state, start_trading_stream


def run_smoke_test():
    configure_logging()
    validate_configuration()
    load_state_from_disk()
    server = start_dashboard_thread()
    payload = dashboard_status_payload()
    LOGGER.info(
        "Smoke test OK: dashboard=%s symbols=%s ready=%s",
        "started" if server else "disabled_or_unavailable",
        len(payload.get("decisions", [])),
        payload.get("daily_context", {}).get("ready_count", 0),
    )
    if server:
        server.shutdown()
        server.server_close()


def run_entry_preflight():
    configure_logging()
    validate_configuration()
    load_state_from_disk()
    log_startup_context()
    prepare_daily_context(force=True)
    payload = dashboard_status_payload()
    checked = 0
    failures = 0
    for decision in payload.get("decisions", []):
        preview = decision.get("contract_preview") or {}
        if not preview.get("symbol"):
            continue
        checked += 1
        result = validate_entry_contract(preview, require_market_open=False)
        status = "OK" if result.get("ok") else "FAIL"
        if not result.get("ok"):
            failures += 1
        LOGGER.info(
            "Entry preflight %s %s %s qty=%s delta=%s bid=%s ask=%s notional=%s warnings=%s blocking=%s",
            status,
            decision.get("symbol"),
            preview.get("symbol"),
            preview.get("quantity"),
            preview.get("delta"),
            preview.get("bid"),
            preview.get("ask"),
            result.get("estimated_notional"),
            "; ".join(result.get("warnings") or []) or "none",
            "; ".join(result.get("blocking") or []) or "none",
        )
    LOGGER.info("Entry preflight complete: checked=%s failures=%s", checked, failures)
    if failures:
        raise SystemExit(1)


def run_live():
    configure_logging()
    validate_configuration()
    load_state_from_disk()
    log_startup_context()
    reconcile_state()
    start_dashboard_thread()
    prepare_daily_context(force=True)
    trading_thread = threading.Thread(target=start_trading_stream, name="alpaca-trading-stream", daemon=True)
    trading_thread.start()
    stock_stream.subscribe_bars(handle_bar, *SYMBOLS)
    stock_stream.run()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run the Flow Sweep options bot.")
    parser.add_argument("--smoke-test", action="store_true", help="Start config/dashboard checks only; do not open streams or submit orders.")
    parser.add_argument("--entry-preflight", action="store_true", help="Validate current Alpaca option contract selection without submitting orders.")
    args = parser.parse_args(argv)
    if args.entry_preflight:
        run_entry_preflight()
        return
    if args.smoke_test:
        run_smoke_test()
        return
    run_live()
