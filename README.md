# 实训学时合规与冻结服务

该服务汇聚学员签到、导师确认和请假修正事件，按培养方案与时区重放学时状态，并保存可追溯的学期冻结快照。项目还提供导师分配、证明材料、豁免复核、规则版本、名额、通知和数据留存等领域模块，供后续业务扩展时复用统一的状态与审计约束。

## 合作院校事件交换

合作院校通过版本化交换批次批量提交签到事件，批次与逐条回执均按发送方
（`X-Sender-ID` 请求头）隔离，发送方只能查看自己的数据。

* `POST /api/plans/{plan_version}/exchange/batches/{batch_id}`：接收整批。
  整批具有规范化 SHA-256 内容指纹；相同内容重放幂等返回，不同内容复用
  `batch_id` 返回 422。处理逐行提交并推进重放游标 `cursor`，崩溃后用相同
  请求体重放即可从游标恢复，已接受事件依赖唯一约束不会重复写入。
* `GET .../batches/{batch_id}/receipt`：逐条回执游标分页（`cursor`、`limit`、
  `status` 过滤）。每条状态为 `accepted` / `duplicate` / `conflict` /
  `pending_review`（冲突修正后原行翻转为 `resolved`），携带原因码与行级
  载荷指纹。
* `POST .../batches/{batch_id}/supplements`：补交重试。条目通过 `references`
  引用原批次行号修正冲突，或以 `resolution="confirm"` 确认待审核项；每次
  成功补交使批次 `revision` 递增，已接受项不重复写入。
* `POST .../batches/{batch_id}/close`：关闭批次（存在未解决冲突/待审核项时
  默认 409，可 `force`）。关闭是条件更新，并发关闭只有一方成功，关闭后
  拒绝接收与补交。
* `GET .../batches/{batch_id}/reconcile`：对账。校验整批指纹、重放游标、
  回执与事件表的一一对应、解决链完整性，输出 `balanced` 与逐条问题码。

## 运行方式

默认数据保存在项目目录的 SQLite 文件中。安装依赖后执行 `uvicorn app.main:app --host 127.0.0.1 --port 8000`，健康检查地址为 `/health`，业务接口位于 `/api`。

## 测试

```bash
python3 -m pytest -q
```

## 编译检查

```bash
python3 -m compileall -q app tests
```

测试覆盖事件幂等导入、跨时区与跨日学时合并、实习确认、负向修正、冻结快照和差异查询；交换模块覆盖大批量重放不重复写入、批内冲突与跨批重复、冲突补交引用重试、并发关闭与补交互斥、崩溃后游标恢复和批次对账；运行过程中不需要单独的数据库或网络服务。
