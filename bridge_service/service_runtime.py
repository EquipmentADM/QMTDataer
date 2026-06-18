"""
QMTDataer bridge 服务运行时管理。

Responsibilities:
    - 管理服务实例的 instance_id、启动时间与进程信息；
    - 通过启动锁保证同一时刻只有一个有效实例；
    - 写入与清理 runtime.json，供主控识别实例身份；
    - 提供最小的运行时读取与状态辅助方法。

Data Contract:
    - runtime 文件与 lock 文件均以 UTF-8 JSON 文本写入；
    - runtime.json 至少包含 module_id、instance_id、pid、port、started_at；
    - lock 文件至少包含 pid、created_at 与 instance_id；
    - 所有时间均为本地无时区 ISO8601 字符串。

Internal Dependencies:
    - None

External Systems:
    - None
"""
from __future__ import annotations

import json
import os
import socket
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional


CN_TZ = timezone(timedelta(hours=8))


def _now_iso() -> str:
    """返回当前本地时间的无时区 ISO8601 字符串。"""
    return datetime.now(CN_TZ).replace(tzinfo=None).isoformat(timespec="seconds")


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """
    以原子替换方式写入 JSON 文件。

    说明：
        - 先写入临时文件，再 replace 到目标路径；
        - 避免主控或其他进程读取到半写入状态的 runtime.json。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp_path.replace(path)


def _safe_unlink(path: Path) -> None:
    """静默删除文件；若文件不存在则忽略。"""
    try:
        path.unlink()
    except FileNotFoundError:
        return


def is_pid_running(pid: int) -> bool:
    """
    判断给定 pid 是否仍然存活。

    说明：
        - 使用 os.kill(pid, 0) 做最小探测；
        - Windows 下若返回 PermissionError，也视为进程仍然存在。
    """
    if pid <= 0:
        return False
    if os.name == "nt":
        return _is_pid_running_windows(pid)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except SystemError:
        return False
    except OSError:
        return False
    return True


def _is_pid_running_windows(pid: int) -> bool:
    """
    使用 Windows API 判断 pid 是否仍然存活。

    说明：
        - Windows 上 os.kill(pid, 0) 对部分异常 pid 会抛出底层包装错误；
        - 这里直接调用 OpenProcess/GetExitCodeProcess，避免运行时准备阶段被打断。
    """
    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32
        process_query_limited_information = 0x1000
        still_active = 259
        error_access_denied = 5
        error_invalid_parameter = 87

        handle = kernel32.OpenProcess(process_query_limited_information, False, int(pid))
        if not handle:
            error_code = kernel32.GetLastError()
            if error_code == error_access_denied:
                return True
            if error_code == error_invalid_parameter:
                return False
            return False

        try:
            exit_code = ctypes.c_ulong()
            if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                return False
            return exit_code.value == still_active
        finally:
            kernel32.CloseHandle(handle)
    except Exception:
        return False


def is_port_listening(host: str, port: int, timeout: float = 0.25) -> bool:
    """
    判断本地服务端口是否可连接。

    说明：
        - 用于识别 stale runtime/lock；
        - 若 pid 存在但端口未监听，说明旧 bridge 实例对主控不可用，应允许清理后重启。
    """
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except OSError:
        return False


@dataclass
class ServiceRuntimeInfo:
    """服务实例运行时快照。"""

    module_id: str
    service: str
    instance_id: str
    pid: int
    host: str
    port: int
    started_at: str
    runtime_file: str
    lock_file: str
    owner: str = "service_main"
    session_mode: str = "shared"

    def to_dict(self) -> dict[str, Any]:
        """转换为可直接返回或写入 JSON 的字典。"""
        return asdict(self)


class ServiceRuntime:
    """
    QMTDataer bridge 服务运行时管理器。

    该类负责以下几件事：
        - 启动前抢占 lock 文件；
        - 为本次实例生成新的 instance_id；
        - 写入 runtime.json；
        - 退出时清理 lock 与 runtime.json。
    """

    def __init__(
        self,
        module_id: str = "qmtdataer",
        service: str = "qmtdataer",
        host: str = "127.0.0.1",
        port: int = 18931,
        base_dir: Optional[Path] = None,
        runtime_file: Optional[Path] = None,
        lock_file: Optional[Path] = None,
    ) -> None:
        self.module_id = module_id
        self.service = service
        self.host = host
        self.port = port
        self.base_dir = Path(base_dir or Path(__file__).resolve().parent)
        self.runtime_dir = self.base_dir / "runtime"
        self.runtime_file = Path(runtime_file or (self.runtime_dir / f"{module_id}.runtime.json"))
        self.lock_file = Path(lock_file or (self.runtime_dir / f"{module_id}.lock"))
        self.instance_id: Optional[str] = None
        self.started_at: Optional[str] = None
        self._lock_acquired = False

    # ---- 运行时准备与清理 ----

    def prepare(self) -> ServiceRuntimeInfo:
        """
        抢锁并生成新的运行时信息。

        返回：
            ServiceRuntimeInfo：当前实例的完整运行时信息。
        """
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self._cleanup_stale_runtime_state()
        self._acquire_lock()
        self.instance_id = str(uuid.uuid4())
        self.started_at = _now_iso()
        info = ServiceRuntimeInfo(
            module_id=self.module_id,
            service=self.service,
            instance_id=self.instance_id,
            pid=os.getpid(),
            host=self.host,
            port=self.port,
            started_at=self.started_at,
            runtime_file=str(self.runtime_file),
            lock_file=str(self.lock_file),
        )
        self.write_runtime(info)
        return info

    def cleanup(self) -> None:
        """
        清理本实例的运行时文件与启动锁。

        说明：
            - 第一阶段直接删除 runtime.json；
            - 若后续需要保留退出历史，可再扩展为写入 shutdown 状态。
        """
        _safe_unlink(self.runtime_file)
        if self._lock_acquired:
            _safe_unlink(self.lock_file)
            self._lock_acquired = False

    # ---- 运行时文件 ----

    def write_runtime(self, info: ServiceRuntimeInfo) -> None:
        """写入当前服务实例的 runtime.json。"""
        _atomic_write_json(self.runtime_file, info.to_dict())

    def read_runtime(self) -> Optional[dict[str, Any]]:
        """读取 runtime.json；若不存在或损坏则返回 None。"""
        if not self.runtime_file.exists():
            return None
        try:
            return json.loads(self.runtime_file.read_text(encoding="utf-8"))
        except Exception:
            return None

    def get_runtime_snapshot(self) -> dict[str, Any]:
        """返回当前 runtime.json 内容或最小占位信息。"""
        runtime_data = self.read_runtime()
        if runtime_data is not None:
            return runtime_data
        return {
            "module_id": self.module_id,
            "service": self.service,
            "instance_id": self.instance_id,
            "pid": os.getpid(),
            "host": self.host,
            "port": self.port,
            "started_at": self.started_at,
            "runtime_file": str(self.runtime_file),
            "lock_file": str(self.lock_file),
        }

    # ---- 启动锁 ----

    # ---- 陈旧运行时清理 ----

    def _cleanup_stale_runtime_state(self) -> None:
        """
        清理不可用的历史 runtime/lock。

        判定规则：
            - pid 不存在，视为 stale；
            - pid 存在但端口未监听，视为 stale；
            - JSON 损坏或关键字段缺失，视为 stale。
        """
        stale_detected = False
        active_detected = False
        for path in (self.runtime_file, self.lock_file):
            payload = self._read_json_file(path)
            if payload is None:
                if path.exists():
                    stale_detected = True
                continue
            if self._is_payload_active(payload):
                active_detected = True
            else:
                stale_detected = True

        if active_detected:
            return
        if stale_detected:
            _safe_unlink(self.runtime_file)
            _safe_unlink(self.lock_file)

    def _read_json_file(self, path: Path) -> Optional[dict[str, Any]]:
        """读取 JSON 文件；文件不存在、损坏或内容非字典时返回 None。"""
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        return payload

    def _is_payload_active(self, payload: dict[str, Any]) -> bool:
        """
        判断 runtime/lock 指向的旧实例是否仍然可用。

        说明：
            - 只要 pid 不存活或端口不可连接，就不能让旧记录阻塞新实例启动；
            - host/port 缺失时回退到当前服务配置，兼容旧版 lock 文件。
        """
        try:
            pid = int(payload.get("pid") or 0)
            port = int(payload.get("port") or self.port)
        except (TypeError, ValueError):
            return False
        host = str(payload.get("host") or self.host)
        if not is_pid_running(pid):
            return False
        if not is_port_listening(host, port):
            return False
        return True

    def _acquire_lock(self) -> None:
        """
        抢占启动锁。

        规则：
            - 若 lock 不存在，则直接创建；
            - 若 lock 已存在，则检查其中 pid 是否仍存活；
            - 若判断为陈旧锁，则先清理后重试一次；
            - 若仍无法获取，则抛出 RuntimeError。
        """
        if self._lock_acquired:
            return

        payload = {
            "module_id": self.module_id,
            "pid": os.getpid(),
            "created_at": _now_iso(),
            "host": self.host,
            "port": self.port,
            "instance_id": self.instance_id,
        }

        try:
            self._create_lock_file(payload)
        except FileExistsError:
            if self._cleanup_stale_lock():
                self._create_lock_file(payload)
            else:
                raise RuntimeError(f"启动锁已存在：{self.lock_file}")
        self._lock_acquired = True

    def _create_lock_file(self, payload: dict[str, Any]) -> None:
        """以排他模式创建 lock 文件。"""
        fd = os.open(str(self.lock_file), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception:
            _safe_unlink(self.lock_file)
            raise

    def _cleanup_stale_lock(self) -> bool:
        """
        尝试清理陈旧锁。

        返回：
            bool：True 表示已清理并可重试；False 表示锁对应实例仍可能有效。
        """
        if not self.lock_file.exists():
            return True
        try:
            payload = json.loads(self.lock_file.read_text(encoding="utf-8"))
        except Exception:
            _safe_unlink(self.lock_file)
            return True

        if not self._is_payload_active(payload):
            _safe_unlink(self.runtime_file)
            _safe_unlink(self.lock_file)
            return True
        return False
