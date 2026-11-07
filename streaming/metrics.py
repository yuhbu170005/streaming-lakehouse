import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock, Thread

from pyspark.sql.streaming import StreamingQueryListener


class Metrics(StreamingQueryListener):
    def __init__(self):
        self.values = {}
        self.dropped = {}
        self.lock = Lock()

    def onQueryStarted(self, event):
        pass

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass

    def onQueryProgress(self, event):
        progress = event.progress
        name = progress.name
        with self.lock:
            operators = progress.stateOperators
            dropped = sum(operator.numRowsDroppedByWatermark for operator in operators)
            self.dropped[name] = self.dropped.get(name, 0) + dropped
            offsets = []
            for source in progress.sources:
                try:
                    end = json.loads(source.endOffset or "{}")
                    latest = json.loads(source.latestOffset or "{}")
                    offsets.extend(
                        max(0, offset - end.get(topic, {}).get(partition, offset))
                        for topic, partitions in latest.items()
                        if isinstance(partitions, dict)
                        for partition, offset in partitions.items()
                    )
                except (TypeError, ValueError, AttributeError):
                    continue
            self.values[name] = {
                "input_rows_per_sec": progress.inputRowsPerSecond,
                "processed_rows_per_sec": progress.processedRowsPerSecond,
                "batch_duration_ms": progress.durationMs.get("triggerExecution", 0),
                "state_rows": sum(operator.numRowsTotal for operator in operators),
                "late_events_total": self.dropped[name],
                "batch_id": progress.batchId,
                "kafka_offset_lag": sum(offsets),
            }

    def serve(self, spark):
        metrics = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/status":
                    data = json.dumps(
                        {query.name: query.status for query in spark.streams.active}
                    )
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(data.encode())
                    return
                if self.path not in ("/health", "/metrics"):
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4")
                self.end_headers()
                with metrics.lock:
                    lines = [
                        f'spark_{key}{{query="{name}"}} {value}'
                        for name, values in metrics.values.items()
                        for key, value in values.items()
                    ]
                self.wfile.write(("\n".join(lines) + "\n").encode())

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("0.0.0.0", 9110), Handler)
        Thread(target=server.serve_forever, daemon=True).start()
