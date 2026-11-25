# 青屿冷链分区用电湖

使用 Python 3.11 标准库与 SQLite。采集员把甲库和乙库的采样批次交给目录维护员。夜班核账员在网关离线时按成果号复算状态用电，并用明细定位停机期间仍有用电的采样。

## 运行

```sh
python3 -B check_project.py
python3 -B -m src.recovery.entrypoints demo
```

回归入口由运行平台在导出时提供。也可直接用 `python3 -B -m unittest discover -s tests -t . -v` 执行完整测试。演示默认用临时工作区。可在 `demo` 后提供现有工作区路径以保留结果。重复初始化与重复运行同参样例会保留已有业务记录。复用样例请求号却修改其参数会被拒绝。

## 工作剧本

1. 采集员接收 `samples/workday.json` 中的 A1 和 B1，逐行校验分区、设备键、状态、来源、事件序号和用电。采样序号在批次内必须严格递增。
2. 维护员同时发布 A1 和 B1 为 S0。两份文件都使用整数 1，分别代表运行、停机。
3. 夜班读取 S0 得到运行 2 条 4.30 Wh、停机 1 条 7.30 Wh，并用乙库采样 20 的来源定位停机用电。
4. 甲库独立发布 A2，乙库登记 B2。S1 组合旧 A1 与新 B2，得到运行 3 条 5.40 Wh、停机 1 条 5.60 Wh。
5. 维护员把 S0 的逻辑文件分段写入同一个容器，验证每段还原后的完整字节再切换定位。
6. 夜班用原凭据领取 S0，并核对复算结果、原始顺序和来源。仓内缺少文件或字典时需要补齐。文件来自错误版本、绑定冲突或含未知编码时，整次核账拒绝。
7. 若发布或核账已提交却没有收到响应，用原号同参重开并领取原成果。待办在修复可用性后按原参执行。同号改投不同数值会返回冲突。

## Python 接口

用 `open_repository(workspace)` 打开 `src.catalog.repository` 的目录对象，再用 `ArtifactStore(repo.root)` 打开 `src.publish.storage` 的文件仓。目录对象可用作上下文管理器。文件仓与目录位于同一个工作区。

- I1 `src.ingest.partition_writer.prepare_partition(batch, version, revision)` 校验并编码一个 `PartitionBatch`，返回字节与版本描述。采样包含 `partition`、`key`、`status`、`value`、`sequence`、`source`。用电输入可以是 Decimal、整数或十进制字符串，JSON 小数通过 `parse_float=Decimal` 读取。缺值、浮点近似值、非有限值和错误原序会被拒绝。
- I2 `src.recovery.replay.resume_publish(repo, store, request_id, credential, params=...)` 是带终局与回执的发布入口。`params` 包含 `snapshot` 和按交付顺序排列的 `members`。新成员提供 `partition`、`version`、`batch`、`generation`、`events`，可用 `dictionary` 声明 `[整数编码, 状态名]` 列表。省略映射时使用同次声明或仓内的指定代次。调用者可用 `load_current_revision` 查看当前代次。旧成员只提供 `partition`、`version`。同一快照可纳入同分区的不同文件版本，每个版本至多出现一次，每条采样按实际文件出现计数。
- I3 `src.readers.snapshot_reader.read_snapshot(repo, store, snapshot_id)` 返回全量解码明细。`resume_reconcile` 用 `params={'snapshot': snapshot_id}` 计算并持久保存报告，报告编号为核账请求号。`src.readers.reconcile.load_report(repo, report_id)` 读取原报告。可选 `available` 是仓内可用的 `(分区, 版本)` 集合，可含闲置版本。可选 `supplied` 是明确交入的有序实际成员，必须匹配固定清单。二者是本次物理可用性信息，重开可随正确材料补齐而变化。
- I4 `resume_publish` 与 `resume_reconcile` 可只提供 `digest=...` 与原凭据，从请求表恢复原参数。`src.recovery.receipts.claim_receipt` 领取发布或核账回执。`src.recovery.replay.claim_historical_snapshot` 还核对指定成果号，返回原回执和目录项。错误凭据抛出 `ClaimError`，同号异参抛出 `RequestConflict`。执行结果含状态、摘要、成果引用或错误原因。`retry` 表示可续办，`rejected` 表示当前固定输入已拒绝，`success` 表示结果及回执已提交。
- `src.publish.compaction.compact_members(repo, store, snapshot_id, segment_bytes=4096)` 提交验证过的物理定位。旧容器保留，以便已经固定定位的读取继续执行。报告顺序为清单成员顺序再按文件原序。合计以 Wh 表示，显示小数位保留输入尺度，求和使用精确整数系数。零值计数，零行成员可发布，全部为空时返回空报告。

CLI 提供 `init`、`publish`、`reconcile`、`compact`、`claim`、`demo`，用 `python3 -B -m src.recovery.entrypoints <子命令> --help` 查看参数。执行失败退出非零。发布的输入文件是上述 `params` 对象。物理文件在工作区内部完整落盘，读者通过已提交的目录定位。业务失败不留下成功回执或半份报告。中断遗留的发布暂件由原请求安全重开时定向清理。
