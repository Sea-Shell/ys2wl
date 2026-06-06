from contextlib import contextmanager
from typing import Generator, Optional
import sqlite3
import logging

log = logging.getLogger("sortarr.db")


@contextmanager
def get_connection(db_path: str) -> Generator[sqlite3.Connection, None, None]:
    con: Optional[sqlite3.Connection] = None
    try:
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row
        yield con
    except sqlite3.Error as err:
        log.error("Database connection error: %s", err)
        raise
    finally:
        if con:
            con.close()
