# 实时计算服务中心

面向实时数据分析场景，提供流式计算定义、窗口聚合、策略告警和状态管理的实时数据平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 流式源端 | `stream_source` | 数据源配置 | 源端名称、消息队列、Topic名称 |
| 窗口配置 | `window_config` | 窗口策略 | 策略名称、窗口类型、窗口大小 |
| 计算算子 | `calc_operator` | 算子定义 | 算子名称、算子类型、输入字段 |
| 状态存储 | `state_store` | 状态配置 | 配置名称、后端类型、状态键模式 |
| 水位追踪 | `watermark_track` | 水位记录 | 水位名称、数据源、当前水位 |
| 告警策略 | `alert_policy` | 告警规则 | 规则名称、监控指标、触发条件 |
| 滑动指标 | `sliding_index` | 指标计算 | 指标名称、计算窗口、聚合函数 |
| 流关联 | `join_stream` | 关联配置 | 配置名称、左流源、右流源 |
| 结果落地 | `sink_output` | 输出配置 | 配置名称、输出类型、目标地址 |
| 补数任务 | `backfill_job` | 补数作业 | 作业名称、目标指标、补数范围 |
| CEP规则 | `cep_rule` | 事件序列 | 序列名称、事件类型、匹配顺序 |
| 吞吐管理 | `throughput_mgr` | 吞吐配置 | 配置名称、数据源、最大QPS |
| 检查点 | `checkpoint_mgr` | 检查点配置 | 配置名称、间隔时间、超时时长 |
| 故障恢复 | `restore_plan` | 恢复方案 | 方案名称、任务范围、恢复模式 |
| 指标血缘 | `metric_lineage` | 血缘记录 | 记录ID、指标名称、上游源端 |
| 迟到处理 | `late_handler` | 处理策略 | 策略名称、允许延迟、迟到动作 |
| 任务拓扑 | `task_topology` | 拓扑配置 | 拓扑名称、算子链、并行度 |
| 数据采样 | `data_sampler` | 采样策略 | 策略名称、采样方式、采样比例 |
| 调试追踪 | `debug_trace` | 调试会话 | 会话名称、目标算子、采样数据 |
| 聚合落库 | `aggregate_sink` | 聚合任务 | 任务名称、源指标集、聚合粒度 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 序号16：分层样本数据 ↔ 调试会话 ↔ 调试异常报告可追溯链路

数据采样不再只输出样本数量，`data_sampler → debug_trace` 链路新增完整的可追溯闭环，
状态独立存放在 `app/store.py` 的序号16专用注册表中，不改变历史 `data_sampler` /
`debug_trace` 列表接口的返回结构。

### 领域规则

- 导入源事件文件后执行**分层采样**，每份分层样本只绑定一个**采样批次**和一个
  **策略版本**（`backend/app/services/sample_trace.py`）。
- 调试会话必须**引用完整样本**；调试追踪按目标算子记录中间结果（期望/实际/差异）
  与异常标记，并生成唯一的**调试异常报告**。同一批次重复启动调试不产生多个当前报告。
- 序号16连续/重复导入只入库一次：以「文件边界 + 采样策略」同源指纹做幂等；
  两种文件边界在序号16下生成不同策略版本。事务失败时回滚全部注册表改动并退回原文件。
- 调试期间采样上限/比例被修改时，可「按原样本完成当前报告」或「按新比例重新采样
  再开一轮」，页面最终只确定一个与样本版本一致的当前报告；故障重开沿同批样本重建当前报告。
- 导出按采样批次打包分层样本数据与调试异常报告，支持再次上传只读校验，并登记
  导入/导出台账；同时提供只读校验与幂等回流导入两种导出处理方式的取舍比较。

### 接口（前缀 `/api/sample-trace`，见 `backend/app/routers/sample_trace.py`）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/imports` | 导入文件生成分层样本数据（幂等，失败回滚退回原文件） |
| GET | `/batches`、`/batches/{id}` | 采样批次列表/详情（含分层数量、样本特征） |
| POST | `/batches/{id}/debug-sessions` | 引用完整样本启动调试，生成唯一当前报告 |
| GET | `/debug-sessions`、`/debug-sessions/{id}` | 调试会话列表/详情（含完整样本与报告） |
| POST | `/debug-sessions/{id}/complete` | 按原样本完成当前报告 |
| POST | `/batches/{id}/resample` | 按新比例/新上限重采样再开一轮 |
| POST | `/batches/{id}/fault-reopen` | 故障重开，沿同批样本重建当前报告 |
| GET | `/batches/{id}/reports/current`、`/reports/{id}` | 当前/指定调试异常报告 |
| GET | `/batches/{id}/export` | 调试详情页文件下载 |
| POST | `/exports/verify`、`/exports/compare-handling` | 再上传校验 / 两种导出处理取舍 |
| GET | `/ledger` | 导入导出台账 |

前端在「数据采样」页面挂载 `SampleTracePanel.vue`（导入、分层数量、审核人核对），
在「调试追踪」页面挂载 `DebugTracePanel.vue`（会话、异常报告、中间结果差异、下载、
再上传校验与台账），相关接口封装在 `frontend/src/api/sample-trace.ts`。

领域：数据工程
子方向：数据分析与数据应用
