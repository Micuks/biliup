use crate::UploadLine;
use crate::server::common::util::Recorder;
use crate::server::config::Config;
use crate::server::core::downloader::SegmentInfo;
use crate::server::errors::{AppError, AppResult};
use crate::server::infrastructure::context::{Context, Stage, Worker, WorkerStatus};
use crate::server::infrastructure::models::InsertFileItem;
use crate::server::infrastructure::models::hook_step::process_video;
use crate::server::infrastructure::models::upload_streamer::UploadStreamer;
use async_channel::Receiver;
use biliup::bilibili::{BiliBili, ResponseData, Studio, Video};
use biliup::client::StatelessClient;
use biliup::credential::login_by_cookies;
use biliup::error::Kind;
use biliup::uploader::line::{Line, Probe};
use biliup::uploader::util::SubmitOption;
use biliup::uploader::{VideoFile, line};
use error_stack::ResultExt;
use futures::StreamExt;
use ormlite::Insert;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Instant;
use tokio::pin;
use tracing::{error, info};

// 辅助结构体
const MAX_VIDEOS_PER_SUBMIT: usize = 99;

struct UploadContext {
    bilibili: BiliBili,
    line: Line,
    threads: usize,
    client: StatelessClient,
}

#[derive(Default)]
struct UploadedVideos {
    videos: Vec<Video>,
    paths: Vec<PathBuf>,
}

pub async fn process_with_upload(
    rx: Receiver<SegmentInfo>,
    ctx: &Context,
    upload_config: &UploadStreamer,
) -> AppResult<()> {
    info!(upload_config=?upload_config, "Starting process with upload");
    // 1. 初始化上传环境
    let upload_context =
        initialize_upload_context(&ctx.config(), &ctx.stateless_client(), upload_config).await?;

    // 2. 流水线处理视频上传
    let uploaded_videos = pipeline_upload_videos(rx, &upload_context, ctx).await?;

    // 3. 提交到B站
    if !uploaded_videos.videos.is_empty() {
        let mut recorder = ctx
            .recorder(ctx.stream_info_ext().streamer_info.clone())
            .clone();
        recorder.filename_prefix = upload_config.title.clone();
        let submit_api = ctx.config().submit_api.clone();

        let total_batches = uploaded_videos.videos.len().div_ceil(MAX_VIDEOS_PER_SUBMIT);
        for (batch_index, videos_chunk) in uploaded_videos
            .videos
            .chunks(MAX_VIDEOS_PER_SUBMIT)
            .enumerate()
        {
            let mut studio = build_studio(
                upload_config,
                &upload_context.bilibili,
                videos_chunk.to_vec(),
                &recorder,
            )
            .await?;

            studio.title = add_batch_suffix(&studio.title, batch_index + 1, total_batches);
            info!(
                batch_index = batch_index + 1,
                total_batches,
                videos = studio.videos.len(),
                "Submitting video batch"
            );
            submit_to_bilibili(&upload_context.bilibili, &studio, submit_api.as_deref()).await?;
        }
    }

    // 4. 执行后处理
    if !uploaded_videos.paths.is_empty() {
        execute_postprocessor(uploaded_videos.paths, ctx).await?;
    }

    Ok(())
}

fn add_batch_suffix(title: &str, batch_index: usize, total_batches: usize) -> String {
    if total_batches <= 1 {
        if title.chars().count() > 80 {
            return Video::truncate_title(title, 80);
        }
        return title.to_string();
    }

    let suffix = format!(" [{batch_index}/{total_batches}]");
    let suffix_len = suffix.chars().count();

    if suffix_len >= 80 {
        return suffix.chars().take(80).collect();
    }

    let max_title_chars = 80 - suffix_len;
    let base = if title.chars().count() > max_title_chars {
        Video::truncate_title(title, max_title_chars)
    } else {
        title.to_string()
    };
    format!("{base}{suffix}")
}

async fn initialize_upload_context(
    config: &Config,
    client: &StatelessClient,
    upload_config: &UploadStreamer,
) -> AppResult<UploadContext> {
    // 登录处理
    let cookie_file = upload_config
        .user_cookie
        .clone()
        .unwrap_or("cookies.json".to_string());
    let bilibili = login_by_cookies(&cookie_file, None)
        .await
        .change_context(AppError::Unknown)?;

    // 获取上传线路
    let line = get_upload_line(&client.client, &config.lines).await?;

    Ok(UploadContext {
        bilibili,
        line,
        threads: config.threads as usize,
        client: client.clone(),
    })
}

async fn get_upload_line(client: &reqwest::Client, line: &str) -> AppResult<Line> {
    let line = match line {
        "bda2" => line::bda2(),
        "bda" => line::bda(),
        "tx" => line::tx(),
        "txa" => line::txa(),
        "bldsa" => line::bldsa(),
        "alia" => line::alia(),
        _ => Probe::probe(client).await.unwrap_or_default(),
    };
    Ok(line)
}

async fn insert_file_record(file_path: &Path, ctx: &Context) -> AppResult<()> {
    let result = InsertFileItem {
        file: file_path.display().to_string(),
        streamer_info_id: ctx.id(),
    }
    .insert(ctx.pool())
    .await
    .change_context(AppError::Unknown)?;

    info!(result=?result, "Insert file");
    Ok(())
}

async fn drain_unique_segment_paths(
    rx: Receiver<SegmentInfo>,
    ctx: &Context,
) -> AppResult<Vec<PathBuf>> {
    let mut paths = Vec::new();
    let mut seen_paths = HashSet::new();
    pin!(rx);
    while let Some(event) = rx.next().await {
        if !seen_paths.insert(event.prev_file_path.clone()) {
            info!(
                file = %event.prev_file_path.display(),
                "Skipping duplicate segment event before postprocess"
            );
            continue;
        }

        insert_file_record(&event.prev_file_path, ctx).await?;
        paths.push(event.prev_file_path);
    }

    Ok(paths)
}

async fn pipeline_upload_videos(
    rx: Receiver<SegmentInfo>,
    context: &UploadContext,
    ctx: &Context,
) -> AppResult<UploadedVideos> {
    // let mut desc_v2 = Vec::new();
    // for credit in context.upload_config.desc_v2_credit {
    //     desc_v2.push(Credit {
    //         type_id: credit.type_id,
    //         raw_text: credit.raw_text,
    //         biz_id: credit.biz_id,
    //     });
    // }

    let mut uploaded = UploadedVideos::default();
    let mut seen_paths = HashSet::new();
    pin!(rx);
    // 流式处理后续事件
    while let Some(event) = rx.next().await {
        if !seen_paths.insert(event.prev_file_path.clone()) {
            info!(
                file = %event.prev_file_path.display(),
                "Skipping duplicate segment event before upload"
            );
            continue;
        }

        insert_file_record(&event.prev_file_path, ctx).await?;

        match upload_single_file(&event.prev_file_path, context).await {
            Ok(video) => {
                uploaded.videos.push(video);
                uploaded.paths.push(event.prev_file_path);
            }
            Err(e) => {
                error!(
                    file = %event.prev_file_path.display(),
                    error = ?e,
                    "Upload single file failed, skipping segment"
                );
            }
        }
    }

    Ok(uploaded)
}

async fn upload_single_file(file_path: &Path, context: &UploadContext) -> AppResult<Video> {
    let video_path = file_path;
    let UploadContext {
        bilibili,
        line,
        threads: limit,
        client,
    } = context;

    info!(
        "开始上传文件：{:?}",
        video_path
            .canonicalize()
            .change_context(AppError::Unknown)?
            .to_str()
    );
    info!("线路选择：{line:?}");
    let video_file = VideoFile::new(video_path).change_context(AppError::Unknown)?;
    let total_size = video_file.total_size;
    let file_name = video_file.file_name.clone();
    let uploader = line
        .pre_upload(bilibili, video_file)
        .await
        .change_context(AppError::Unknown)?;

    let instant = Instant::now();

    let video = uploader
        .upload(client.clone(), *limit, |vs| {
            vs.map(|vs| {
                let chunk = vs?;
                let len = chunk.len();
                Ok((chunk, len))
            })
        })
        .await
        .change_context(AppError::Unknown)?;
    let t = instant.elapsed().as_millis();
    info!(
        "Upload completed: {file_name} => cost {:.2}s, {:.2} MB/s.",
        t as f64 / 1000.,
        total_size as f64 / 1000. / t as f64
    );
    Ok(video)
}

pub async fn submit_to_bilibili(
    bilibili: &BiliBili,
    studio: &Studio,
    submit_api: Option<&str>,
) -> AppResult<ResponseData> {
    // let submit = match worker.config.read().unwrap().submit_api {
    //     Some(submit) => SubmitOption::from_str(&submit).unwrap_or(SubmitOption::App),
    //     _ => SubmitOption::App,
    // };

    // let submit_result = match submit {
    //     SubmitOption::BCutAndroid => {
    //         bilibili.submit_by_bcut_android(&studio, None).await
    //     }
    //     _ => bilibili.submit_by_app(&studio, None).await,
    // };

    let submit_option = match submit_api {
        Some(submit) => SubmitOption::from_str(submit).unwrap_or(SubmitOption::App),
        _ => SubmitOption::App,
    };

    let result = match submit_option {
        SubmitOption::BCutAndroid => bilibili
            .submit_by_bcut_android(studio, None)
            .await
            .change_context(AppError::Unknown)?,
        _ => bilibili
            .submit_by_app(studio, None)
            .await
            .change_context(AppError::Unknown)?,
    };
    info!("Submit successful");
    Ok(result)
}

pub(crate) async fn build_studio(
    upload_config: &UploadStreamer,
    bilibili: &BiliBili,
    mut videos: Vec<Video>,
    recorder: &Recorder,
) -> AppResult<Studio> {
    let formatted_title = recorder.format_filename();
    let studio_title = if formatted_title.chars().count() > 80 {
        Video::truncate_title(&formatted_title, 80)
    } else {
        formatted_title
    };

    for video in &mut videos {
        if let Some(title) = video.title.as_ref()
            && title.chars().count() > 80
        {
            video.title = Some(Video::truncate_title(title, 80));
        }
    }

    // 使用 Builder 模式简化构建
    let mut studio: Studio = Studio::builder()
        .desc(recorder.format(&upload_config.description.clone().unwrap_or_default()))
        .maybe_dtime(upload_config.dtime)
        .maybe_copyright(upload_config.copyright)
        .cover(upload_config.cover_path.clone().unwrap_or_default())
        .dynamic(upload_config.dynamic.clone().unwrap_or_default())
        .source(
            upload_config
                .copyright_source
                .clone()
                .unwrap_or_else(|| recorder.streamer_info.url.clone()),
        )
        .tag(upload_config.tags.join(","))
        .maybe_tid(upload_config.tid)
        .title(studio_title)
        .videos(videos)
        .dolby(upload_config.dolby.unwrap_or_default())
        // .lossless_music(upload_config.)
        .no_reprint(upload_config.no_reprint.unwrap_or_default())
        .charging_pay(upload_config.charging_pay.unwrap_or_default())
        .up_close_reply(upload_config.up_close_reply.unwrap_or_default())
        .up_selection_reply(upload_config.up_selection_reply.unwrap_or_default())
        .up_close_danmu(upload_config.up_close_danmu.unwrap_or_default())
        .maybe_is_only_self(upload_config.is_only_self)
        .maybe_desc_v2(None)
        .extra_fields(
            serde_json::from_str(&upload_config.extra_fields.clone().unwrap_or_default())
                .unwrap_or_default(), // 处理额外字段
        )
        .build();
    // 处理封面上传
    if !studio.cover.is_empty()
        && let Ok(c) = &std::fs::read(&studio.cover).inspect_err(|e| error!(e=?e))
        && let Ok(url) = bilibili.cover_up(c).await.inspect_err(|e| error!(e=?e))
    {
        studio.cover = url;
    };

    Ok(studio)
}

pub async fn execute_postprocessor(video_paths: Vec<PathBuf>, ctx: &Context) -> AppResult<()> {
    if let Some(processor) = &ctx.live_streamer().postprocessor {
        let paths: Vec<&Path> = video_paths.iter().map(|p| p.as_path()).collect();
        process_video(&paths, processor).await?;
    }
    Ok(())
}

pub async fn upload(
    cookie_file: impl AsRef<Path>,
    proxy: Option<&str>,
    line: Option<UploadLine>,
    video_paths: &[PathBuf],
    limit: usize,
) -> AppResult<(BiliBili, Vec<Video>)> {
    let bilibili = login_by_cookies(&cookie_file, proxy).await;
    let bilibili = match bilibili {
        Err(Kind::IO(_)) => bilibili.change_context_lazy(|| {
            AppError::Custom(format!(
                "open cookies file: {}",
                &cookie_file.as_ref().to_string_lossy()
            ))
        })?,
        _ => bilibili.change_context_lazy(|| AppError::Unknown)?,
    };

    let client = StatelessClient::default();
    let mut videos = Vec::new();
    let line = match line {
        Some(UploadLine::Bldsa) => line::bldsa(),
        Some(UploadLine::Cnbldsa) => line::cnbldsa(),
        Some(UploadLine::Andsa) => line::andsa(),
        Some(UploadLine::Atdsa) => line::atdsa(),
        Some(UploadLine::Bda2) => line::bda2(),
        Some(UploadLine::Cnbd) => line::cnbd(),
        Some(UploadLine::Anbd) => line::anbd(),
        Some(UploadLine::Atbd) => line::atbd(),
        Some(UploadLine::Tx) => line::tx(),
        Some(UploadLine::Cntx) => line::cntx(),
        Some(UploadLine::Antx) => line::antx(),
        Some(UploadLine::Attx) => line::attx(),
        // Some(UploadLine::Bda) => line::bda(),
        Some(UploadLine::Txa) => line::txa(),
        Some(UploadLine::Alia) => line::alia(),
        _ => Probe::probe(&client.client).await.unwrap_or_default(),
    };
    for video_path in video_paths {
        println!(
            "{:?}",
            video_path
                .canonicalize()
                .change_context_lazy(|| AppError::Unknown)?
                .to_str()
        );
        info!("{line:?}");
        let video_file = VideoFile::new(video_path).change_context_lazy(|| AppError::Unknown)?;
        let total_size = video_file.total_size;
        let file_name = video_file.file_name.clone();
        let uploader = line
            .pre_upload(&bilibili, video_file)
            .await
            .change_context_lazy(|| AppError::Unknown)?;

        let instant = Instant::now();

        let video = uploader
            .upload(client.clone(), limit, |vs| {
                vs.map(|vs| {
                    let chunk = vs?;
                    let len = chunk.len();
                    Ok((chunk, len))
                })
            })
            .await
            .change_context_lazy(|| AppError::Unknown)?;
        let t = instant.elapsed().as_millis();
        info!(
            "Upload completed: {file_name} => cost {:.2}s, {:.2} MB/s.",
            t as f64 / 1000.,
            total_size as f64 / 1000. / t as f64
        );
        videos.push(video);
    }

    Ok((bilibili, videos))
}

/// 上传Actor
/// 负责处理上传相关的消息和任务
pub struct UActor {
    /// 上传消息接收器
    receiver: Receiver<UploaderMessage>,
}

impl UActor {
    /// 创建新的上传Actor实例
    pub fn new(receiver: Receiver<UploaderMessage>) -> Self {
        Self { receiver }
    }

    /// 运行Actor主循环，处理接收到的消息
    pub(crate) async fn run(&mut self) {
        while let Ok(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }

    /// 处理上传消息
    ///
    /// # 参数
    /// * `msg` - 要处理的上传消息
    async fn handle_message(&mut self, msg: UploaderMessage) {
        match msg {
            UploaderMessage::SegmentEvent(rx, ctx) => {
                ctx.change_status(Stage::Upload, WorkerStatus::Pending)
                    .await;
                let result = match ctx.upload_config() {
                    Some(config) => process_with_upload(rx, &ctx, config).await,
                    None => match drain_unique_segment_paths(rx, &ctx).await {
                        Ok(paths) => {
                            // 无上传配置时，直接执行后处理
                            execute_postprocessor(paths, &ctx).await
                        }
                        Err(e) => Err(e),
                    },
                };

                if let Err(e) = &result {
                    error!("Process segment event failed: {}", e);
                    // 可以添加错误通知机制
                }
                info!(url=ctx.live_streamer().url, result=?result, "后处理执行完毕：Finished processing segment event");
                ctx.change_status(Stage::Upload, WorkerStatus::Idle).await;
            }
        }
    }
}

/// 上传消息枚举
/// 定义上传Actor可以处理的消息类型
#[derive(Debug)]
pub enum UploaderMessage {
    /// 分段事件消息，包含事件、接收器和工作器
    SegmentEvent(Receiver<SegmentInfo>, Context),
}
