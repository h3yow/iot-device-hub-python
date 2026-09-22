"""本地 IoT 设备接入平台入口：uvicorn main:app"""

from iot_hub.api import create_app

app = create_app()
