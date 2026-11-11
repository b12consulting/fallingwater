"""
Configure runtime type checks and isolated Redis integration tests.
"""

import shutil
import subprocess
from collections.abc import Iterator
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic, sleep

import pytest
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from typeguard import install_import_hook

install_import_hook("fallingwater")


@pytest.fixture
def isolated_redis(tmp_path: Path) -> Iterator[Redis]:
    executable = shutil.which("redis-server")
    if executable is None:
        pytest.skip("redis-server is required for Redis integration tests")
    log = tmp_path / "redis.log"
    with TemporaryDirectory(prefix="fw-") as directory, subprocess.Popen(
        [
            executable,
            "--port",
            "0",
            "--unixsocket",
            str(Path(directory) / "redis.sock"),
            "--save",
            "",
            "--appendonly",
            "no",
            "--logfile",
            str(log),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
    ) as process:
        redis = Redis(unix_socket_path=str(Path(directory) / "redis.sock"))
        try:
            deadline = monotonic() + 5
            while True:
                if process.poll() is not None:
                    raise RuntimeError(log.read_text())
                try:
                    redis.ping()
                    break
                except RedisConnectionError:
                    if monotonic() >= deadline:
                        raise
                    sleep(0.01)
            yield redis
        finally:
            redis.close()
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
