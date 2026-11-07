"""Serve the sample orders as JSON over a small local HTTP API."""

import csv
import json
import logging
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from src.utils.config import get_project_root

logger = logging.getLogger(__name__)
DEFAULT_ORDERS_FILE = get_project_root() / "data" / "sample" / "orders.csv"


def create_handler(sample_file: str | Path = DEFAULT_ORDERS_FILE):
    """Create a request handler that serves orders from the selected CSV file."""
    orders_path = Path(sample_file)

    class OrdersRequestHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            route = urlparse(self.path).path
            if route == "/health":
                self._send_json(200, {"status": "ok"})
                return
            if route != "/api/orders":
                self._send_json(404, {"error": "Not found"})
                return

            try:
                with orders_path.open("r", encoding="utf-8", newline="") as orders_file:
                    orders = list(csv.DictReader(orders_file))
                for order in orders:
                    order["quantity"] = int(order["quantity"])
            except (OSError, KeyError, ValueError):
                logger.exception("Could not load mock API data from %s", orders_path)
                self._send_json(500, {"error": "Could not load sample orders"})
                return

            self._send_json(200, {"orders": orders})

        def _send_json(self, status_code: int, payload: dict[str, object]) -> None:
            response_body = json.dumps(payload).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(response_body)))
            self.end_headers()
            self.wfile.write(response_body)

        def log_message(self, format_string: str, *args: object) -> None:
            logger.info("Mock API: " + format_string, *args)

    return OrdersRequestHandler


def run_server(host: str = "127.0.0.1", port: int = 5000) -> None:
    """Run the mock orders API until the process is stopped."""
    server = ThreadingHTTPServer((host, port), create_handler())
    logger.info("Mock orders API listening at http://%s:%s", host, port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Stopping mock orders API")
    finally:
        server.server_close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_server(
        host=os.getenv("MOCK_API_HOST", "127.0.0.1"),
        port=int(os.getenv("MOCK_API_PORT", "5000")),
    )