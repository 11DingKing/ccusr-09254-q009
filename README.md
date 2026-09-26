# 实训学时合规与冻结服务

该服务汇聚学员签到、导师确认和请假修正事件，按培养方案与时区重放学时状态，并保存可追溯的学期冻结快照。项目还提供导师分配、证明材料、豁免复核、规则版本、名额、通知和数据留存等领域模块，供后续业务扩展时复用统一的状态与审计约束。

## 运行方式

默认数据保存在项目目录的 SQLite 文件中。安装依赖后执行 `uvicorn app.main:app --host 127.0.0.1 --port 8000`，健康检查地址为 `/health`，业务接口位于 `/api`。

## 测试

```bash
python3 -m pytest -q
```

## 外部事件交换

合作院校通过 `/api/exchange` 批量推送签到事件，请求需携带 `X-Sender-Id` 头，发送方只能访问自己的批次，跨发送方访问一律按不存在处理。

- `POST /api/exchange/batches`：接收版本化批次（声明 `schema_version`），返回逐条回执（已接受/重复/冲突/待审核）、整批内容指纹与重放游标；相同批次号与相同内容重发时幂等重放已存回执，内容不同则返回 409；已接受事件写入事件流且绝不重复写入。
- `GET /api/exchange/batches/{batch_id}/receipts?after_seq=&limit=`：按重放游标增量拉取回执，支持断点续传。
- `POST /api/exchange/batches/{batch_id}/supplements`：修正冲突或待审核条目，`retry_of` 引用原条目号，重试后原条目转为 superseded 保留审计轨迹；已接受条目不可改写。
- `POST /api/exchange/batches/{batch_id}/close`：按预期版本号关闭批次，乐观锁保证并发关闭只有一个成功；关闭后禁止补交。
- `GET /api/exchange/batches/{batch_id}/reconciliation`：输出计数自洽性、事件流命中数与指纹比对结果的对账单。

## 编译检查

```bash
python3 -m compileall -q app tests
```

测试覆盖事件幂等导入、跨时区与跨日学时合并、实习确认、负向修正、冻结快照和差异查询；运行过程中不需要单独的数据库或网络服务。
