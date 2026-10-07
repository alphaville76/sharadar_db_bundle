import os
import sqlite3
import subprocess
import sys
from unittest.mock import patch

from sharadar.util import sqlite_util
from sharadar.util.sqlite_util import remove_wal_files, wal_files

CRASHED_WRITER = """
import os, sqlite3, sys
con = sqlite3.connect(sys.argv[1])
con.execute('PRAGMA journal_mode = WAL')
con.execute('PRAGMA wal_autocheckpoint = 0')
con.execute('CREATE TABLE t (x INTEGER)')
con.execute('INSERT INTO t VALUES (42)')
con.commit()
os._exit(0)  # killed without closing: committed data only in the -wal file
"""


def _crashed_wal_db(tmp_path):
    path = str(tmp_path / 'db.sqlite')
    subprocess.run([sys.executable, '-c', CRASHED_WRITER, path], check=True)
    assert len(wal_files(path)) == 2
    return path


def _journal_mode(path):
    with sqlite3.connect(path) as con:
        return con.execute('PRAGMA journal_mode').fetchone()[0]


def test_remove_wal_files_keeps_committed_data(tmp_path):
    path = _crashed_wal_db(tmp_path)

    with patch('os.remove', side_effect=AssertionError('files must be deleted by SQLite')), \
            patch('os.unlink', side_effect=AssertionError('files must be deleted by SQLite')):
        assert remove_wal_files(path)

    assert wal_files(path) == []
    assert _journal_mode(path) == 'delete'
    con = sqlite3.connect(path)
    assert con.execute('SELECT x FROM t').fetchall() == [(42,)]
    con.close()
    assert wal_files(path) == []


def test_remove_wal_files_skips_database_in_use(tmp_path):
    path = _crashed_wal_db(tmp_path)
    other = sqlite3.connect(path)
    other.execute('SELECT * FROM t').fetchall()

    assert not remove_wal_files(path)
    assert wal_files(path) != []
    other.close()

    assert remove_wal_files(path)
    assert wal_files(path) == []


def test_remove_wal_files_without_files_or_database(tmp_path):
    path = str(tmp_path / 'missing.sqlite')
    assert remove_wal_files(path)
    for suffix in sqlite_util.WAL_SUFFIXES:
        open(path + suffix, 'w').close()
    # Orphan files are left alone and no empty database is created.
    assert not remove_wal_files(path)
    assert len(wal_files(path)) == 2
    assert not os.path.exists(path)


def test_connect_removes_wal_files_on_open_and_close(tmp_path):
    path = _crashed_wal_db(tmp_path)

    con = sqlite_util.connect(path)
    assert wal_files(path) == []
    assert con.execute('SELECT x FROM t').fetchall() == [(42,)]
    con.execute('PRAGMA journal_mode = WAL')
    con.execute('INSERT INTO t VALUES (43)')
    con.commit()
    con.close()

    assert wal_files(path) == []
    assert _journal_mode(path) == 'delete'
    with sqlite3.connect(path) as check:
        assert check.execute('SELECT x FROM t ORDER BY x').fetchall() == [(42,), (43,)]


def test_connect_in_memory():
    con = sqlite_util.connect(':memory:')
    assert con.execute('SELECT 1').fetchone() == (1,)
    con.close()
