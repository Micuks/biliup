# 断点续录设计 (Resume-on-Restart)

## 问题
biliup-server 重启 / SIGKILL / 崩溃后，正在写入的 `.ts.part` 不会被
- 重命名（依赖 `LifecycleFile::Drop`，SIGKILL 时不执行）
- 自动上传（无启动期扫描器）
- 续录（无任何状态持久化）

可观测后果：磁盘上出现孤儿 `.part`（4-30 / 4-19 / 4-3 / 12-29 等多次重启留下 8 GB），且当时段录像数据丢失。

## 目标（按优先级）
1. **Salvage**：让磁盘上的 `.part` 总能进入上传流水线（不丢已写入的字节）。
2. **Quick resume**：重启后尽快重新拉取仍在直播的频道（缩短录制空窗）。
3. **HLS 真续传**：跳过 HLS sequence 复用——HLS 服务器保留窗口短、PTS 必然跳变；目标 1+2 已等价。

## 三层方案

### Pillar A — 优雅关停 (graceful shutdown)
让 SIGTERM 走完 Drop 路径，覆盖 99% 主动重启场景。

**改动点**
- `crates/biliup-cli/src/server/app.rs::shutdown_signal`
  当前只 `cleanup()` + abort `deletion_task`。新增：
  ```rust
  // 在 cleanup() 之前
  let monitor = service_register.monitor.clone();
  for worker in monitor.get_all().await {
      let _ = worker.stop().await;  // 调用 StreamGears.stop() / Ffmpeg.stop()
  }
  // 等下载流水线 drain：每个 Worker 把当前 .part flush + rename，并把
  // SegmentEvent::Segment 推到 upload 通道
  let _ = tokio::time::timeout(Duration::from_secs(60), monitor.wait_idle()).await;
  ```
- `Worker.stop()` 已存在（cancel CancellationToken）。Drop chain 触发：
  `tokio::select!` → drop start_download future → drop `LifecycleFile` → `rename()`。
- 上传 actor 需要 graceful 模式：收到 shutdown 信号后处理完 channel 中已入队的 segment，再退出。
  当前 `upload_videos_actor` 是 `while let Some(event) = rx.next().await`——只要写端不 close 就一直阻塞。需要加：
  - `tokio::select!` 监听一个 shutdown 通道
  - 或在 monitor 全部 stop 后 close `tx`，actor 自然 exit

**systemd 配合**
```ini
[Service]
KillSignal=SIGTERM
TimeoutStopSec=120        # 当前默认 90s，预留 60s 让 segment 重命名 + 上传完成
KillMode=mixed            # 主进程 SIGTERM，子进程 SIGKILL（避免子 ffmpeg 卡住）
```

**风险**
- 上传未完成的段会停留在 disk → 由 Pillar B 兜底
- StreamGears 的 `start_download` 是 awaitable，cancel 时 BufWriter Drop 会 flush，但
  最后一段 HTTP chunk 可能未消耗 → 文件结尾几百 KB 残缺，MPEG-TS 解析鲁棒
- Python 进程层（biliup CLI 入口）若忽略 SIGTERM 则 Rust handler 不会触发——
  需要确认 entry point。如果是，systemd 下挂的是 Python，需在 Python 层透传 SIGTERM 到子。

### Pillar B — 启动期 Salvage (startup scan)
覆盖崩溃 / OOM / 断电场景。每次启动时：

1. 扫描 `working_dir`（biliup 配置中的 `保存路径` / `download_dir`）下所有 `.ts.part` / `.flv.part`。
2. 对每个 `.part`：
   - 跳过 mtime < 60 秒的（可能是别的进程在写）
   - `lsof` / `fuser` 检查（更稳：fcntl OFD lock 探测）——0 字节直接删
   - 通过 ⇒ `rename(p, p.with_extension(""))` 去掉 `.part`
3. 对成功重命名的 `.ts` / `.flv`：
   - 解析文件名前缀匹配 `LiveStreamer.url` / 某 streamer 的 `filename_template`
   - 走和正常 `SegmentEvent::Segment` 相同的入队路径，触发 segment_processor (含 remux) + upload pipeline + postprocessor

**改动点**
- 新模块 `crates/biliup-cli/src/server/core/salvage.rs`
- `app.rs` 在 `axum::serve` 之前调用 `salvage::scan_and_recover(working_dir, monitor).await`

**0 字节 .part 的处理**
当前已知 `EDG ZmjjKK*_自习.flv.part` 多个 0 字节文件（HTTP 连接失败时 LifecycleFile.create
后立刻报错没写任何字节）。salvage 直接 unlink 它们。

**幂等性**
若 segment 已经上传（DB 有记录）则 skip——`upload_streamers_id` + `file_name` 唯一。

### Pillar C — 快速恢复监控 (warm restart)
重启后 monitor 默认要等下一轮 polling cycle 才知道直播在线。改进：

1. 在 graceful shutdown 时持久化「当前正在录制」的 streamer id 到
   `data/recording_state.json`（只是 hint，不是 source-of-truth）。
2. 启动后 monitor 把这些 streamer 优先排进 polling 队列（首轮立即检查），其他常规
   按周期扫描。

**改动点小**：context.rs 加 `record_active(id)`/`load_active() -> Vec<i64>`，
`MonitorActor::new()` 启动后先 emit 一次 `Check` 给 hint 列表里的 streamer。

## 实施顺序
1. **Pillar B 单独可上线**——纯加法，无侵入，先实现 + 上线（验证 salvage 路径）。
2. **Pillar A** 需要改 actor 关停语义，谨慎；先在 systemd 上把 `TimeoutStopSec=120` /
   `KillMode=mixed` 加上，再改 Rust。
3. **Pillar C** 锦上添花，最后做。

## 验证
- 杀进程 (SIGKILL) → 重启 → `journalctl --user -fu biliup` 看到 `salvage:
  recovered N file(s)`，孤儿被 rename + 入队上传
- `systemctl --user stop biliup` (SIGTERM) → 重启 → 不应有任何 `.part` 残留
- 单元测试覆盖 salvage 的：fd-held skip、0-byte cleanup、未知 prefix（不属于任何
  streamer 的 .part）
