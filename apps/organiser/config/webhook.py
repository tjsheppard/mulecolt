"""
webhook.py — HTTP webhook server for triggering scan cycles.

Receives POST /trigger from Zurg's on_library_update hook to wake
the organiser scan loop immediately. Also exposes GET /health.
"""

import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

log = logging.getLogger("organiser")


class _WebhookHandler(BaseHTTPRequestHandler):
    """Minimal HTTP handler — POST /trigger wakes the scan loop."""

    # The threading.Event is injected via the class attribute by start_server()
    scan_event: threading.Event

    def do_POST(self):
        if self.path.rstrip("/") == "/trigger":
            self.scan_event.set()
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"status":"triggered"}\n')
            log.info("Webhook trigger received — waking scan loop")
        else:
            self.send_response(404)
            self.end_headers()

    def do_GET(self):
        if self.path.rstrip("/") == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}\n')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):  # noqa: A002
        """Suppress default stderr logging — we use our own logger."""
        pass


def start_server(event: threading.Event, port: int) -> None:
    """Start the webhook HTTP server in a daemon thread.

    Args:
        event: The threading.Event to set when a trigger is received.
        port:  The port to listen on.
    """
    _WebhookHandler.scan_event = event
    server = HTTPServer(("", port), _WebhookHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    log.info(f"Webhook server listening on port {port}")
