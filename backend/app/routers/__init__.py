"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。

序号16 的专用路由优先注册，避免 /api/{module}/{entry_id} 动态路径抢先匹配
/api/sample-trace/* 下的固定路径。
"""
from __future__ import annotations

from app.routers import resource, sample_trace

ROUTERS = [sample_trace, resource]
