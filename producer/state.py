import json
import sqlite3

from producer.contracts import now_ms


class State:
    """The cursor only advances after broker ack. Gaps and cursor commit together."""

    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS cursors (symbol TEXT PRIMARY KEY, last_id INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS gaps (
                symbol TEXT, from_id INTEGER, to_id INTEGER, next_id INTEGER,
                detected_at INTEGER, backfilled_at INTEGER,
                PRIMARY KEY (symbol, from_id)
            );
            CREATE TABLE IF NOT EXISTS reference_requests (
                symbol TEXT, minute_ms INTEGER, PRIMARY KEY(symbol, minute_ms)
            );
            CREATE TABLE IF NOT EXISTS outbox (id INTEGER PRIMARY KEY, event TEXT NOT NULL);
            """
        )

    def _outbox(self, event):
        self.db.execute(
            "INSERT INTO outbox(event) VALUES (?)", (json.dumps(dict(event)),)
        )

    def acknowledge(self, records):
        gaps = duplicates = 0
        with self.db:
            for record in records:
                symbol, trade_id = record["s"], record["a"]
                cursor = self.db.execute(
                    "SELECT last_id FROM cursors WHERE symbol = ?", (symbol,)
                ).fetchone()
                if cursor is not None:
                    expected = cursor["last_id"] + 1
                    if trade_id < expected:
                        duplicates += 1
                        continue
                    if trade_id > expected:
                        detected = now_ms()
                        self.db.execute(
                            "INSERT INTO gaps VALUES (?, ?, ?, ?, ?, NULL)",
                            (symbol, expected, trade_id - 1, expected, detected),
                        )
                        self._outbox(
                            {
                                "symbol": symbol,
                                "from_id": expected,
                                "to_id": trade_id - 1,
                                "detected_at": detected,
                                "backfilled_at": None,
                            }
                        )
                        gaps += 1
                self.db.execute(
                    "INSERT INTO cursors VALUES (?, ?) ON CONFLICT(symbol) "
                    "DO UPDATE SET last_id = excluded.last_id",
                    (symbol, trade_id),
                )
        return gaps, duplicates

    def pending(self):
        return self.db.execute(
            "SELECT * FROM gaps WHERE backfilled_at IS NULL ORDER BY detected_at, symbol"
        ).fetchall()

    def advance(self, gap, next_id, minutes):
        completed = now_ms() if next_id > gap["to_id"] else None
        with self.db:
            self.db.execute(
                "UPDATE gaps SET next_id = ?, backfilled_at = ? WHERE symbol = ? AND from_id = ?",
                (next_id, completed, gap["symbol"], gap["from_id"]),
            )
            self.db.executemany(
                "INSERT OR IGNORE INTO reference_requests VALUES (?, ?)",
                [(gap["symbol"], minute) for minute in minutes],
            )
            if completed is not None:
                event = dict(gap)
                event.pop("next_id")
                event["backfilled_at"] = completed
                self._outbox(event)

    def notifications(self):
        return self.db.execute(
            "SELECT id, event FROM outbox ORDER BY id LIMIT 100"
        ).fetchall()

    def clear_notifications(self, last_id):
        with self.db:
            self.db.execute("DELETE FROM outbox WHERE id <= ?", (last_id,))

    def closed_reference_requests(self):
        return self.db.execute(
            "SELECT * FROM reference_requests WHERE minute_ms <= ? ORDER BY minute_ms LIMIT 50",
            (now_ms() - 65_000,),
        ).fetchall()

    def finish_reference(self, symbol, minute):
        with self.db:
            self.db.execute(
                "DELETE FROM reference_requests WHERE symbol = ? AND minute_ms = ?",
                (symbol, minute),
            )
