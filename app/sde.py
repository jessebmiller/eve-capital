import sqlite3
from contextlib import contextmanager


@contextmanager
def cursor(db=sqlite3, connection_string="/eve-sde/dump-2016-07-04_19:24.sqlite"):
    print(connection_string)
    with db.connect(connection_string) as conn:
        cur = conn.cursor()
        yield cur
        cur.close()
