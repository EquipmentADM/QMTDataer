# -*- coding = utf-8 -*-
# @Time : 2025/9/10 22:51
# @Author : EquipmentADV
# @File : registry.py
# @Software : PyCharm
# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Dict, Any, List, Optional
import time
import uuid
import json

try:
    import redis
except Exception as e:  # pragma: no cover
    redis = None  # type: ignore
    _IMPORT_ERR = e
else:
    _IMPORT_ERR = None


@dataclass
class SubscriptionSpec:
    """类说明：订阅规格
    功能：描述一次订阅所需的全部信息；
    上下游：上游为控制面下发；下游为 RealtimeSubscriptionService。
    """
    strategy_id: str
    codes: List[str]
    periods: List[str]
    mode: str
    preload_days: int
    topic: str
    created_at: int


class Registry:
    """类说明：订阅注册表（Redis 持久化）
    功能：保存/查询/删除订阅规格，提供重启恢复数据；
    上游：控制面；
    下游：运行入口（重放订阅）。
    """
    def __init__(self, host: str, port: int, password: Optional[str], db: int, prefix: str = "xt:bridge") -> None:
        if _IMPORT_ERR is not None:
            raise RuntimeError(f"未能导入 redis：{_IMPORT_ERR}")
        self._cli = redis.Redis(host=host, port=port, password=password, db=db, decode_responses=True)
        self.prefix = prefix.rstrip(":")

    # Key 设计
    def _k_subs(self) -> str: return f"{self.prefix}:subs"
    def _k_sub(self, sub_id: str) -> str: return f"{self.prefix}:sub:{sub_id}"
    def _k_strategy_subs(self, strategy_id: str) -> str: return f"{self.prefix}:strategy:{strategy_id}:subs"

    @staticmethod
    def gen_sub_id() -> str:
        return f"sub-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"

    @staticmethod
    def _encode_mapping(d: Dict[str, Any]) -> Dict[str, str]:
        """方法说明：将 dict 转为 Redis 可接受的 {str: str}
        功能：list/dict -> JSON 字符串；其他 -> str；
        上游：save；下游：Redis.hset。
        """
        out: Dict[str, str] = {}
        for k, v in d.items():
            if isinstance(v, (list, dict)):
                out[k] = json.dumps(v, ensure_ascii=False)
            else:
                out[k] = "" if v is None else str(v)
        return out

    @staticmethod
    def _decode_mapping(d: Dict[str, str]) -> Dict[str, Any]:
        """方法说明：反序列化部分字段（尽力而为）"""
        out: Dict[str, Any] = dict(d)
        for k in ("codes", "periods"):
            if k in out:
                try:
                    out[k] = json.loads(out[k])
                except Exception:
                    pass
        if "created_at" in out:
            try:
                out["created_at"] = int(out["created_at"])
            except Exception:
                pass
        return out

    def save(self, sub_id: str, spec: SubscriptionSpec) -> None:
        payload = self._encode_mapping(asdict(spec))
        self._cli.hset(self._k_sub(sub_id), mapping=payload)
        self._cli.sadd(self._k_subs(), sub_id)
        self._cli.sadd(self._k_strategy_subs(spec.strategy_id), sub_id)

    def delete(self, sub_id: str) -> None:
        data = self._cli.hgetall(self._k_sub(sub_id))
        if data and "strategy_id" in data:
            self._cli.srem(self._k_strategy_subs(data["strategy_id"]), sub_id)
        self._cli.delete(self._k_sub(sub_id))
        self._cli.srem(self._k_subs(), sub_id)

    def list_all(self) -> List[str]:
        return sorted(self._cli.smembers(self._k_subs()))

    def load(self, sub_id: str) -> Optional[Dict[str, Any]]:
        data = self._cli.hgetall(self._k_sub(sub_id))
        return self._decode_mapping(data) if data else None

    def list_by_strategy(self, strategy_id: str) -> List[str]:
        return sorted(self._cli.smembers(self._k_strategy_subs(strategy_id)))