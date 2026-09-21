import logging

import pytest

from iot_hub.models import Scope
from iot_hub.service import DeviceHubService

ALL_SCOPES = {Scope.TELEMETRY_WRITE, Scope.SHADOW_READ, Scope.SHADOW_WRITE, Scope.OTA_ACK}

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("test")


class Clock:
    """可控时钟：测试用确定性时间。"""

    def __init__(self, start: float = 1_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def hub(clock: Clock) -> DeviceHubService:
    return DeviceHubService(now_fn=clock)


@pytest.fixture
def registered(hub: DeviceHubService, clock: Clock):
    """注册一台设备并签发全 scope 凭据，返回 (device_id, token)。"""

    def _make(device_id: str = "dev-1", model: str = "sensor-A",
              fw: str = "1.0.0", scopes: set[Scope] = ALL_SCOPES):
        hub.registry.register(device_id, model, fw, clock())
        cred = hub.registry.issue_credential(device_id, scopes, clock())
        log.info("注册设备 device_id=%s model=%s fw=%s scopes=%s",
                 device_id, model, fw, [s.value for s in scopes])
        return device_id, cred.token

    return _make
