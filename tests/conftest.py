"""测试公共夹具：可控时钟 + 输入/判定日志辅助。"""

from __future__ import annotations

import logging

import pytest

from iot_hub.ratelimit import BoundedBacklog, RateLimiter
from iot_hub.service import HubService
from iot_hub.storage import HubStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("tests")


class FakeClock:
    def __init__(self, start: float = 1_000_000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def service(clock: FakeClock) -> HubService:
    store = HubStore(clock=clock)
    limiter = RateLimiter(
        global_rate=1_000_000, global_burst=1_000_000,
        device_rate=1_000_000, device_burst=1_000_000,
        clock=clock,
    )
    svc = HubService(
        store, rate_limiter=limiter, backlog=BoundedBacklog(1_000_000),
        clock=clock, auto_sweep=False,
    )
    return svc


@pytest.fixture
def registered(service: HubService) -> dict:
    res = service.register_device("dev-1", "sensor-v1", initial_fw_version="1.0.0")
    log.info("SETUP register input=%r => credential_id=%s", {"device_id": "dev-1", "model": "sensor-v1"}, res["credential_id"])
    return res
