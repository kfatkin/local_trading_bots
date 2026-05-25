import argparse
import threading

from .clients import stock_stream
from .config import LOGGER, SYMBOLS, configure_logging, validate_configuration
from .dashboard import dashboard_status_payload, start_dashboard_thread
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
    args = parser.parse_args(argv)
    if args.smoke_test:
        run_smoke_test()
        return
    run_live()
