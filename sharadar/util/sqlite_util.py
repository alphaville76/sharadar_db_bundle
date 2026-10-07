"""SQLite helpers that keep the bundle databases free of -wal/-shm files.

A database in WAL journal mode stores committed transactions in ``<db>-wal``
and uses ``<db>-shm`` as shared memory index. They remain after a crash, a
killed process or a connection that is never closed. The files are never
deleted by hand (that could lose committed data): SQLite itself checkpoints
the WAL into the database and deletes both files when the database is
switched back to the default rollback journal mode (``journal_mode=DELETE``).
"""
import os
import sqlite3

from sharadar.util.logger import log

WAL_SUFFIXES = ('-wal', '-shm')


def _is_file_db(dbpath):
    return bool(dbpath) and dbpath != ':memory:' and not dbpath.startswith('file:')


def wal_files(dbpath):
    """Return the existing -wal/-shm files of ``dbpath``."""
    return [f for f in (dbpath + s for s in WAL_SUFFIXES) if os.path.exists(f)]


def remove_wal_files(dbpath):
    """Let SQLite checkpoint the WAL into ``dbpath`` and delete its -wal/-shm files.

    Nothing is done while another connection uses the database: the files are
    then still needed and will be removed by a later call.

    Returns:
        bool: True if no -wal/-shm file is left.
    """
    dbpath = os.fspath(dbpath)
    if not _is_file_db(dbpath) or not wal_files(dbpath):
        return True
    if not os.path.exists(dbpath):
        # Don't create an empty database just to apply an orphan WAL.
        log.warning("Found -wal/-shm files without SQLite database %s." % dbpath)
        return False

    try:
        con = sqlite3.connect(dbpath, timeout=0, isolation_level=None)
        try:
            # Opening the database recovers the WAL. Leaving WAL mode needs exclusive
            # access: SQLite then checkpoints the WAL and deletes the -wal and -shm files.
            mode = con.execute('PRAGMA journal_mode = DELETE').fetchone()[0]
            if mode.lower() == 'wal':
                log.debug("SQLite database %s is in use, -wal/-shm files not removed." % dbpath)
                return False
        finally:
            con.close()
    except sqlite3.Error as e:
        log.debug("Couldn't remove -wal/-shm files of %s: %s" % (dbpath, e))
        return False
    return not wal_files(dbpath)


class _Connection(sqlite3.Connection):
    """sqlite3 connection that removes the -wal/-shm files on close."""
    _dbpath = None

    def close(self):
        if self._dbpath is not None:
            try:
                # Leave WAL mode while this connection is still open (no-op in the other modes).
                self.execute('PRAGMA busy_timeout = 0')
                self.execute('PRAGMA journal_mode = DELETE')
            except sqlite3.Error:
                pass
        super().close()
        if self._dbpath is not None:
            remove_wal_files(self._dbpath)


def connect(dbpath, **kwargs):
    """Like ``sqlite3.connect``, removing -wal/-shm files on open and close."""
    dbpath = os.fspath(dbpath)
    if not _is_file_db(dbpath):
        return sqlite3.connect(dbpath, **kwargs)
    remove_wal_files(dbpath)
    con = sqlite3.connect(dbpath, factory=_Connection, **kwargs)
    con._dbpath = dbpath
    return con
