//! 崩溃/重启后恢复被中断的录制会话。
//!
//! 进程崩溃(或 SIGKILL / SIGBUS 换 .so)时,`LifecycleFile::rename` 与收尾
//! hook 都不执行 → 正在写的分段留下孤儿 `.part`,且已收尾但会话未提交的分段
//! 也搁死本地、从不进入上传。详见 `RECOVERY_DESIGN.md`。
//!
//! 本模块:
//!   1. 收尾孤儿 `.part`(去后缀 → 正式文件;过小残片删除)。
//!   2. disk-first 扫描盘上录像 → 经 `filelist` 表(file→streamer_info_id)
//!      按会话分组 = 待上传候选。
//!   3. 为每个会话构建 `Context`,把候选文件重放成 `SegmentInfo` 灌入 channel,
//!      发 `UploaderMessage::SegmentEvent` 给现有 `UActor` → 复用
//!      `process_with_upload` 的 remux/上传/submit/回收全链(零新上传逻辑)。
//!
//! 过滤(踩事故教训:2026-07-16 曾把十几秒广告/短录像传成稿件):
//!   - `.part` 过小残片删除(MIN_KEEP_BYTES)。
//!   - 会话有效内容 < MIN_SESSION_BYTES(≈30min @ ~6Mbps)不投,避免传广告/短录像。

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use async_channel::{Sender, bounded};
use ormlite::Model;
use tracing::{error, info, warn};

use crate::server::config::Config;
use crate::server::common::upload::UploaderMessage;
use crate::server::core::downloader::SegmentInfo;
use crate::server::core::plugin::StreamInfoExt;
use crate::server::infrastructure::connection_pool::ConnectionPool;
use crate::server::infrastructure::context::{PluginContext, Worker};
use crate::server::infrastructure::models::{FileItem, StreamerInfo};
use crate::server::infrastructure::models::live_streamer::LiveStreamer;
use crate::server::infrastructure::models::upload_streamer::UploadStreamer;
use biliup::client::StatelessClient;

const REC_EXTS: [&str; 3] = ["ts", "mp4", "flv"];
/// 小于此大小的 `.part` 视为无价值残片,删除。
const MIN_KEEP_BYTES: u64 = 20 * 1024 * 1024; // 20 MB
/// `.part` 距最近写入需超过此时长才收尾(双保险,启动时本不应有活跃录制)。
const MIN_IDLE_SECS: u64 = 60;
/// 会话有效内容小于此值不投稿(≈30min @ ~6Mbps;既滤广告碎片,也滤不够一局的短录像)。
const MIN_SESSION_BYTES: u64 = 1300 * 1024 * 1024; // 1.3 GB

fn has_rec_ext(p: &Path) -> bool {
    p.extension()
        .and_then(|s| s.to_str())
        .map(|e| REC_EXTS.contains(&e))
        .unwrap_or(false)
}

/// 收尾指定目录下的孤儿 `.part`(去 `.part` 后缀)。返回成功收尾的正式文件路径。
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
        if let Ok(modified) = meta.modified()
            && let Ok(idle) = now.duration_since(modified)
            && idle < Duration::from_secs(MIN_IDLE_SECS)
        {
            info!(file = %path.display(), "recovery: .part 仍活跃,跳过");
            continue;
        }
        if meta.len() < MIN_KEEP_BYTES {
            match fs::remove_file(&path) {
                Ok(_) => info!(file = %path.display(), size = meta.len(), "recovery: 删除过小 .part 残片"),
                Err(e) => warn!(file = %path.display(), "recovery: 删除 .part 失败: {e}"),
            }
            continue;
        }
        let final_path = path.with_extension(""); // foo.ts.part -> foo.ts
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

/// disk-first 扫描待上传候选,按 `streamer_info_id` 分组。
///
/// filelist 表有 27 万+ 行(只插不删),故不遍历全表:一次性载入 file→sid 映射,
/// 再遍历盘上的录像文件(~千级)去匹配。成功会话的文件已被回收 → 不在盘 → 天然过滤。
async fn scan_pending(pool: &ConnectionPool, dir: &Path) -> HashMap<i64, Vec<PathBuf>> {
    let mut groups: HashMap<i64, Vec<PathBuf>> = HashMap::new();
    let rows = match FileItem::select().fetch_all(pool).await {
        Ok(v) => v,
        Err(e) => {
            error!("recovery: 查询 filelist 失败: {e}");
            return groups;
        }
    };
    // file(相对名) → streamer_info_id
    let mut map: HashMap<String, i64> = HashMap::with_capacity(rows.len());
    for r in rows {
        map.entry(r.file).or_insert(r.streamer_info_id);
    }
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) => {
            error!(dir = %dir.display(), "recovery: 无法读取目录: {e}");
            return groups;
        }
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !has_rec_ext(&path) {
            continue;
        }
        let name = match path.file_name().and_then(|s| s.to_str()) {
            Some(n) => n,
            None => continue,
        };
        if let Some(&sid) = map.get(name) {
            groups.entry(sid).or_default().push(path);
        }
        // 不在 filelist 的文件(如 remux .mp4 产物、非 ISO)本轮跳过,留待后续增强。
    }
    groups
}

/// 为一个会话(streamer_info_id)构建上传 `Context`。
/// 映射:StreamerInfo(by id) → LiveStreamer(by url) → UploadStreamer(by fk)。
/// 找不到主播/上传配置则返回 None(保守:不投,留给人工/兜底)。
async fn build_context(
    pool: &ConnectionPool,
    config: Arc<RwLock<Config>>,
    client: StatelessClient,
    sid: i64,
) -> Option<crate::server::infrastructure::context::Context> {
    let streamer_info = StreamerInfo::select()
        .where_("id = ?")
        .bind(sid)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()?;
    let live = LiveStreamer::select()
        .where_("url = ?")
        .bind(&streamer_info.url)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()?;
    let upload = match live.upload_streamers_id {
        Some(uid) => UploadStreamer::select()
            .where_("id = ?")
            .bind(uid)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten(),
        None => None,
    };
    upload.as_ref()?; // 无上传配置 → process_with_upload 只会 postprocess 不投;跳过
    let worker = Arc::new(Worker::new(live, upload, config, client));
    let ext = StreamInfoExt {
        streamer_info: streamer_info.clone(),
        raw_stream_url: String::new(),
        platform: String::new(),
        stream_headers: HashMap::new(),
        suffix: String::new(),
    };
    Some(PluginContext::new(worker, pool.clone()).to_context(sid, ext))
}

/// 把一个会话的候选文件重放进 UActor(等价一次正常录制会话的上传)。
async fn inject(up_tx: &Sender<UploaderMessage>, ctx: crate::server::infrastructure::context::Context, files: Vec<PathBuf>) {
    let (tx, rx) = bounded::<SegmentInfo>(files.len().max(1));
    for (i, f) in files.iter().enumerate() {
        let seg = SegmentInfo {
            prev_file_path: f.clone(),
            next_file_path: None,
            segment_index: i,
        };
        if tx.send(seg).await.is_err() {
            break;
        }
    }
    drop(tx); // 关闭 channel → pipeline_upload_videos 结束循环 → build_studio + submit
    if let Err(e) = up_tx.send(UploaderMessage::SegmentEvent(rx, ctx)).await {
        error!("recovery: 注入 UActor 失败: {e}");
    }
}

/// 启动时恢复入口。best-effort:任何失败只记录,不阻塞 server 启动。
pub async fn recover_pending_uploads(
    pool: ConnectionPool,
    config: Arc<RwLock<Config>>,
    client: StatelessClient,
    up_tx: Sender<UploaderMessage>,
) {
    let dir = Path::new(".");
    info!("recovery: 开始扫描中断的录制…");

    let finalized = finalize_orphan_parts(dir);
    info!(count = finalized.len(), "recovery: 孤儿 .part 收尾完成");

    let groups = scan_pending(&pool, dir).await;
    info!(sessions = groups.len(), "recovery: filelist 待传候选会话(文件仍在盘)");

    let (mut injected, mut skipped_small, mut skipped_noctx) = (0u32, 0u32, 0u32);
    for (sid, files) in groups {
        let total: u64 = files
            .iter()
            .filter_map(|f| fs::metadata(f).ok())
            .map(|m| m.len())
            .sum();
        if total < MIN_SESSION_BYTES {
            skipped_small += 1; // 广告碎片/短录像,不投
            continue;
        }
        match build_context(&pool, config.clone(), client.clone(), sid).await {
            Some(ctx) => {
                inject(&up_tx, ctx, files).await;
                injected += 1;
            }
            None => skipped_noctx += 1,
        }
    }
    info!(
        injected, skipped_small, skipped_noctx,
        "recovery: 完成(注入=已排队上传;small=过小跳过;noctx=缺主播/上传配置跳过)"
    );
}
