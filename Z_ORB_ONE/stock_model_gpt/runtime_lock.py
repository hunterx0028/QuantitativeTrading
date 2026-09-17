"""OS-released process lock; daily steps run sequentially in the same process."""
import json
import os
from contextlib import contextmanager
from functools import wraps
from uuid import uuid4

from .paths import PACKAGE_DIR

LOCK_PATH = PACKAGE_DIR / ".workflow.lock"
_active_token = None


@contextmanager
def workflow_lock():
    global _active_token
    if _active_token is not None:
        yield
        return
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    stream = LOCK_PATH.open("a+b")
    try:
        stream.seek(0, 2)
        if not stream.tell():
            stream.write(b" ")
            stream.flush()
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise RuntimeError("另一個 stock_model_gpt 流程正在執行，請稍後重試") from exc
        _active_token = uuid4().hex
        stream.seek(1)
        stream.truncate()
        stream.write(json.dumps({"pid": os.getpid(), "token": _active_token}).encode())
        stream.flush()
        yield
    finally:
        _active_token = None
        stream.close()  # OS releases lock even after a crash; file is not a stale lock.


def locked(function):
    @wraps(function)
    def wrapped(*args, **kwargs):
        with workflow_lock():
            return function(*args, **kwargs)
    return wrapped
