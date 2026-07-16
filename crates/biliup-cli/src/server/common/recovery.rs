//! 崩溃/重启后恢复被中断的录制会话。
//!
//! 背景:进程崩溃(或 SIGKILL / SIGBUS 换 .so)时,`LifecycleFile::rename` 与
//! 收尾 hook 都不会执行 → 正在写的分段留下孤儿 `.part`,且已收尾但会话未
//! 提交的分段也搁死在盘上,从不进入上传。详见 `RECOVERY_DESIGN.md`。
//!
//! 本模块(增量 1)负责:
//!   1. 收尾孤儿 `.part`(去后缀 → 正式文件;过小残片删除)。
//!   2. 扫描 `filelist` 表中"文件仍在盘上"的记录 = 待上传候选,按主播分组。
//!
//! 把候选重放进现有 `UActor`(复用 `process_with_upload` 的 remux/上传/投稿/
//! 回收全链)留待增量 2。

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use ormlite::Model;
use tracing::{error, info, warn};

use crate::server::infrastructure::connection_pool::ConnectionPool;
use crate::server::infrastructure::models::FileItem;

/// 小于此大小的 `.part` 视为无价值残片(多为刚开录即崩),直接删除。
const MIN_KEEP_BYTES: u64 = 20 * 1024 * 1024; // 20 MB
/// `.part` 距最近写入需超过此时长才收尾,避免误碰仍在写入的文件。
/// 启动阶段本不应有活跃录制,这里是双保险。
const MIN_IDLE_SECS: u64 = 60;

/// 收尾指定目录下的孤儿 `.part`:去掉 `.part` 后缀变为正式文件。
///
/// - 近 [`MIN_IDLE_SECS`] 秒内仍在写入的文件跳过(双保险)。
/// - 小于 [`MIN_KEEP_BYTES`] 的残片直接删除。
/// - 目标正式文件已存在则跳过(不覆盖)。
///
/// 返回成功收尾的正式文件路径。
pub fn finalize_orphan_parts(dir: &Path) -> Vec<PathBuf> {
    let mut finalized = Vec::new();
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) => {
            warn!(dir = %dir.display(), "recovery: 无法读取目录: {e}");
            return finalized;
        }
    };
    let now = SystemTime::now();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("part") {
            continue;
        }
        let meta = match entry.metadata() {
            Ok(m) if m.is_file() => m,
            _ => continue,
        };
        // 跳过仍活跃写入的 .part(启动时本不应有)
        if let Ok(modified) = meta.modified()
            && let Ok(idle) = now.duration_since(modified)
            && idle < Duration::from_secs(MIN_IDLE_SECS)
        {
            info!(file = %path.display(), "recovery: .part 仍活跃,跳过");
            continue;
        }
        // 过小残片:删除
        if meta.len() < MIN_KEEP_BYTES {
            match fs::remove_file(&path) {
                Ok(_) => info!(file = %path.display(), size = meta.len(), "recovery: 删除过小 .part 残片"),
                Err(e) => warn!(file = %path.display(), "recovery: 删除 .part 失败: {e}"),
            }
            continue;
        }
        // foo.ts.part -> foo.ts
        let final_path = path.with_extension("");
        if final_path.exists() {
            warn!(target = %final_path.display(), "recovery: 目标已存在,跳过收尾");
            continue;
        }
        match fs::rename(&path, &final_path) {
            Ok(_) => {
                info!(from = %path.display(), to = %final_path.display(), "recovery: 收尾孤儿 .part");
                finalized.push(final_path);
            }
            Err(e) => error!(file = %path.display(), "recovery: 收尾 .part 失败: {e}"),
        }
    }
    finalized
}

/// 查询 `filelist` 中「文件仍在盘上」的记录 = 待上传候选。
///
/// 成功提交的会话其文件已被 postprocessor 回收(`rm`),故其 filelist 行会
/// 指向不存在的路径 → 被过滤掉。留下的即未成功上传、仍需处理的文件。
pub async fn find_pending_files(pool: &ConnectionPool) -> Vec<FileItem> {
    let all = match FileItem::select().fetch_all(pool).await {
        Ok(v) => v,
        Err(e) => {
            error!("recovery: 查询 filelist 失败: {e}");
            return Vec::new();
        }
    };
    all.into_iter()
        .filter(|f| Path::new(&f.file).exists())
        .collect()
}

/// 启动时恢复入口(增量 1:收尾 + 扫描 + 分组统计)。
///
/// best-effort:任何失败只记录日志,绝不阻塞 server 启动。
/// 注入现有 `UActor`(重放 `SegmentInfo`)见增量 2。
pub async fn recover_pending_uploads(pool: &ConnectionPool, output_dir: &Path) {
    info!("recovery: 开始扫描中断的录制…");

    let finalized = finalize_orphan_parts(output_dir);
    info!(count = finalized.len(), "recovery: 孤儿 .part 收尾完成");

    let pending = find_pending_files(pool).await;
    let mut by_streamer: BTreeMap<i64, (usize, u64)> = BTreeMap::new();
    for f in &pending {
        let sz = fs::metadata(&f.file).map(|m| m.len()).unwrap_or(0);
        let entry = by_streamer.entry(f.streamer_info_id).or_default();
        entry.0 += 1;
        entry.1 += sz;
    }
    info!(
        files = pending.len(),
        streamers = by_streamer.len(),
        "recovery: filelist 待传候选(文件仍在盘)"
    );
    for (sid, (n, bytes)) in &by_streamer {
        info!(streamer_info_id = sid, files = n, mb = bytes / 1024 / 1024, "recovery: 待传分组");
    }

    // TODO(增量 2):为每个 (streamer_info_id, 会话) 建 Context + async_channel,
    //   把候选文件重放成 SegmentInfo 灌入,drop sender 后
    //   up_tx.send(UploaderMessage::SegmentEvent(rx, ctx)) →
    //   复用 process_with_upload 的 remux/上传/submit/回收全链。
    warn!("recovery: 注入 UActor 尚未实现(增量 2);本轮仅收尾 .part + 统计待传候选");
}
