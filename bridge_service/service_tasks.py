"""
QMTDataer bridge 服务任务执行器。

Responsibilities:
    - 提供最小任务模型与统一任务返回结构；
    - 以后台线程方式执行历史下载 / 补数 / 入库任务；
    - 维护近期任务列表与单任务状态查询；
    - 复用现有 ingest_runner，而不重写业务逻辑。

Data Contract:
    - 第一阶段仅支持 task_type = `ingest_run`；
    - payload 至少包含 `mode`，其余字段按 ingest_runner 的 profile 覆盖参数透传；
    - 所有时间字段均使用本地无时区 ISO8601 字符串；
    - 任务结果字段 result/error 为结构化对象或字符串，不返回 Python 原生异常对象。
"""
from __future__ import annotations

import io
import threading
import uuid
from contextlib import redirect_stdout
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from core.ingest_runner import list_profile_names, run_profile


CN_TZ = timezone(timedelta(hours=8))


def _now_iso() -> str:
    """返回当前本地时间的无时区 ISO8601 字符串。"""
    return datetime.now(CN_TZ).replace(tzinfo=None).isoformat(timespec="seconds")


@dataclass
class ServiceTask:
    """bridge 服务任务对象。"""

    task_id: str
    module_id: str
    task_type: str
    payload: dict[str, Any]
    status: str = "pending"
    message: str = "任务已创建，等待执行。"
    created_at: str = field(default_factory=_now_iso)
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    progress: int = 0
    result: Optional[dict[str, Any]] = None
    error: Optional[str] = None
    logs: list[str] = field(default_factory=list)

    def to_public_dict(self) -> dict[str, Any]:
        """转换为统一任务输出结构。"""
        data = asdict(self)
        data.pop("payload", None)
        return data


class ServiceTaskManager:
    """
    QMTDataer 最小任务执行器。

    第一阶段设计取舍：
        - 仅使用后台线程，不引入复杂任务队列；
        - 默认串行执行，避免并发下载/补数互相干扰；
        - 不做数据库持久化，只维护进程内近期任务列表。
    """

    def __init__(self, module_id: str = "qmtdataer", max_recent: int = 50) -> None:
        self.module_id = module_id
        self.max_recent = max_recent
        self._tasks: dict[str, ServiceTask] = {}
        self._recent_ids: list[str] = []
        self._lock = threading.RLock()
        self._worker_lock = threading.Lock()

    # ---- 公开接口 ----

    def submit_task(self, task_type: str, payload: dict[str, Any]) -> ServiceTask:
        """
        创建并提交任务。

        参数：
            task_type: 当前仅支持 `ingest_run`
            payload  : 任务参数字典，至少包含 `mode`
        """
        task = ServiceTask(
            task_id=str(uuid.uuid4()),
            module_id=self.module_id,
            task_type=task_type,
            payload=dict(payload),
        )
        with self._lock:
            self._tasks[task.task_id] = task
            self._recent_ids.insert(0, task.task_id)
            self._recent_ids = self._recent_ids[: self.max_recent]

        thread = threading.Thread(
            target=self._run_task_worker,
            args=(task.task_id,),
            daemon=True,
            name=f"{self.module_id}-task-{task.task_id[:8]}",
        )
        thread.start()
        return task

    def get_task(self, task_id: str) -> Optional[ServiceTask]:
        """按 task_id 获取任务对象。"""
        with self._lock:
            return self._tasks.get(task_id)

    def list_recent_tasks(self, limit: int = 20) -> list[dict[str, Any]]:
        """返回近期任务列表。"""
        with self._lock:
            ids = self._recent_ids[: max(0, limit)]
            return [self._tasks[i].to_public_dict() for i in ids if i in self._tasks]

    def list_task_types(self) -> tuple[str, ...]:
        """返回当前支持的任务类型。"""
        return ("ingest_run",)

    def list_ingest_modes(self) -> tuple[str, ...]:
        """返回当前支持的入库模式。"""
        return list_profile_names()

    # ---- 内部执行 ----

    def _run_task_worker(self, task_id: str) -> None:
        """
        后台执行任务。

        说明：
            - 使用 worker_lock 保证同一时刻只跑一个入库任务；
            - 这样最稳，避免多个下载/补数任务同时抢资源。
        """
        task = self.get_task(task_id)
        if task is None:
            return

        with self._worker_lock:
            self._mark_running(task)
            try:
                result = self._execute_task(task)
            except Exception as exc:
                self._mark_failed(task, str(exc))
                return
            self._mark_success(task, result)

    def _execute_task(self, task: ServiceTask) -> dict[str, Any]:
        """根据任务类型执行实际业务逻辑。"""
        if task.task_type != "ingest_run":
            raise ValueError(f"当前不支持的 task_type: {task.task_type}")

        mode = str(task.payload.get("mode") or "").strip()
        if not mode:
            raise ValueError("ingest_run 缺少必要参数：mode")
        if mode not in self.list_ingest_modes():
            raise ValueError(f"未知 ingest mode: {mode}")

        overrides = {k: v for k, v in task.payload.items() if k != "mode" and v is not None}
        output = io.StringIO()
        with redirect_stdout(output):
            result = run_profile(mode, **overrides)

        logs = [line for line in output.getvalue().splitlines() if line.strip()]
        with self._lock:
            task.logs = logs[-100:]
        return {
            "mode": mode,
            "summary": result,
            "log_lines": len(logs),
            "logs": logs[-30:],
        }

    # ---- 状态变更 ----

    def _mark_running(self, task: ServiceTask) -> None:
        with self._lock:
            task.status = "running"
            task.message = "任务正在执行。"
            task.started_at = _now_iso()
            task.progress = 10

    def _mark_success(self, task: ServiceTask, result: dict[str, Any]) -> None:
        with self._lock:
            task.status = "success"
            task.message = "任务执行成功。"
            task.finished_at = _now_iso()
            task.progress = 100
            task.result = result
            task.error = None

    def _mark_failed(self, task: ServiceTask, error: str) -> None:
        with self._lock:
            task.status = "failed"
            task.message = "任务执行失败。"
            task.finished_at = _now_iso()
            task.progress = 100
            task.error = error
            task.result = None
