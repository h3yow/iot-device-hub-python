"""OTA：分批下发、确认、超时与失败回退、离线排队。

状态语义（确定且可解释）：
- QUEUED：设备离线或当前批次已满，排队等待，未下发；
- PENDING：已下发，等待设备在 ack_deadline 前确认；未确认绝不视为已生效；
- SUCCEEDED：设备确认成功，此时才允许推进设备固件版本；
- FAILED：设备上报失败，设备保持原可用版本（回退结论）；
- ROLLED_BACK：确认超时，强制回退到升级前版本。
兼容性：型号不匹配或目标版本等于当前版本 -> INCOMPATIBLE_FIRMWARE，明确拒绝。
"""
from __future__ import annotations

import threading
import uuid
from collections.abc import Callable

from .errors import ErrorCode, HubError
from .models import Device, OtaCampaign, OtaJob, OtaJobStatus


class OtaManager:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._campaigns: dict[str, OtaCampaign] = {}

    def create_campaign(self, campaign_id: str, model: str, target_version: str,
                        batch_size: int, ack_timeout: float, now: float) -> OtaCampaign:
        if not campaign_id or not model or not target_version:
            raise HubError(ErrorCode.MALFORMED_PAYLOAD, "campaign_id/model/target_version 不能为空")
        if batch_size <= 0 or ack_timeout <= 0:
            raise HubError(ErrorCode.MALFORMED_PAYLOAD, "batch_size/ack_timeout 必须为正")
        with self._lock:
            if campaign_id in self._campaigns:
                raise HubError(ErrorCode.CONFLICT, "批次任务已存在")
            campaign = OtaCampaign(
                campaign_id=campaign_id, model=model, target_version=target_version,
                batch_size=batch_size, ack_timeout=ack_timeout, created_at=now,
            )
            self._campaigns[campaign_id] = campaign
            return campaign

    def dispatch(self, campaign_id: str, device: Device, now: float) -> OtaJob:
        """对单台设备下发；不兼容明确拒绝；离线或批次满则排队。"""
        with self._lock:
            campaign = self._campaigns.get(campaign_id)
            if campaign is None:
                raise HubError(ErrorCode.CAMPAIGN_NOT_FOUND, "批次任务不存在")
            if device.model != campaign.model:
                raise HubError(ErrorCode.INCOMPATIBLE_FIRMWARE, "设备型号与目标固件不兼容")
            if device.firmware_version == campaign.target_version:
                raise HubError(ErrorCode.INCOMPATIBLE_FIRMWARE, "设备已处于目标版本")
            existing = campaign.jobs.get(device.device_id)
            if existing is not None and existing.status in (
                OtaJobStatus.QUEUED, OtaJobStatus.PENDING,
            ):
                return existing  # 幂等：重复下发不产生第二个任务
            pending_count = sum(
                1 for j in campaign.jobs.values() if j.status == OtaJobStatus.PENDING
            )
            if not device.online or pending_count >= campaign.batch_size:
                status = OtaJobStatus.QUEUED
                deadline = 0.0
            else:
                status = OtaJobStatus.PENDING
                deadline = now + campaign.ack_timeout
            job = OtaJob(
                job_id=uuid.uuid4().hex,
                campaign_id=campaign_id,
                device_id=device.device_id,
                target_version=campaign.target_version,
                status=status,
                dispatched_at=now if status == OtaJobStatus.PENDING else 0.0,
                ack_deadline=deadline,
            )
            campaign.jobs[device.device_id] = job
            return job

    def promote_queued(self, campaign_id: str,
                       is_online: Callable[[str], bool], now: float) -> list[OtaJob]:
        """批次有空位且设备在线时，将 QUEUED 提升为 PENDING。"""
        promoted: list[OtaJob] = []
        with self._lock:
            campaign = self._campaigns.get(campaign_id)
            if campaign is None:
                raise HubError(ErrorCode.CAMPAIGN_NOT_FOUND, "批次任务不存在")
            pending_count = sum(
                1 for j in campaign.jobs.values() if j.status == OtaJobStatus.PENDING
            )
            for job in campaign.jobs.values():
                if pending_count >= campaign.batch_size:
                    break
                if job.status == OtaJobStatus.QUEUED and is_online(job.device_id):
                    job.status = OtaJobStatus.PENDING
                    job.dispatched_at = now
                    job.ack_deadline = now + campaign.ack_timeout
                    pending_count += 1
                    promoted.append(job)
        return promoted

    def ack(self, device_id: str, job_id: str, success: bool, now: float) -> OtaJob:
        """确认：仅 PENDING 可确认；成功才生效，失败即回退结论。"""
        with self._lock:
            job = self._find_job(device_id, job_id)
            if job.status != OtaJobStatus.PENDING:
                raise HubError(ErrorCode.OTA_NOT_PENDING,
                               f"任务状态 {job.status.value} 不可确认")
            job.status = OtaJobStatus.SUCCEEDED if success else OtaJobStatus.FAILED
            return job

    def sweep_timeouts(self, now: float) -> list[OtaJob]:
        """确认超时的 PENDING 任务强制回退为 ROLLED_BACK。"""
        rolled_back: list[OtaJob] = []
        with self._lock:
            for campaign in self._campaigns.values():
                for job in campaign.jobs.values():
                    if job.status == OtaJobStatus.PENDING and now >= job.ack_deadline:
                        job.status = OtaJobStatus.ROLLED_BACK
                        rolled_back.append(job)
        return rolled_back

    def get_campaign(self, campaign_id: str) -> OtaCampaign:
        with self._lock:
            campaign = self._campaigns.get(campaign_id)
            if campaign is None:
                raise HubError(ErrorCode.CAMPAIGN_NOT_FOUND, "批次任务不存在")
            return campaign

    def job_for(self, device_id: str) -> OtaJob | None:
        with self._lock:
            latest: OtaJob | None = None
            for campaign in self._campaigns.values():
                job = campaign.jobs.get(device_id)
                if job is not None and (
                    latest is None or job.dispatched_at >= latest.dispatched_at
                ):
                    latest = job
            return latest

    def _find_job(self, device_id: str, job_id: str) -> OtaJob:
        for campaign in self._campaigns.values():
            job = campaign.jobs.get(device_id)
            if job is not None and job.job_id == job_id:
                return job
        raise HubError(ErrorCode.NOT_FOUND, "OTA 任务不存在")
