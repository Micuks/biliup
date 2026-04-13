use crate::server::errors::{AppError, AppResult};
use crate::server::infrastructure::context::Context;
use crate::server::infrastructure::models::hook_step::BiliCollectionConfig;
use biliup::bilibili::{BiliBili, ResponseData};
use error_stack::{ResultExt, bail};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::time::{Duration, sleep};
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct SubmittedArchive {
    pub aid: Option<u64>,
    pub title: String,
}

#[derive(Debug, Deserialize)]
struct CollectionsState {
    streamer_collections: HashMap<String, CollectionEntry>,
}

#[derive(Debug, Deserialize)]
struct CollectionEntry {
    section_id: i64,
}

#[derive(Debug)]
struct VideoInfo {
    title: String,
    cid: u64,
}

const PLAYER_ALIASES: &[(&str, &str)] = &[
    ("sen tenz", "TenZ"),
    ("tenz", "TenZ"),
    ("eg jawgemo", "JAWGEMO"),
    ("g2 jawgemo", "JAWGEMO"),
    ("lev aspas", "Aspas"),
    ("mibr aspas", "Aspas"),
    ("100t bang", "Bang"),
    ("sen bang", "Bang"),
    ("apeks batujnax", "Batujnax"),
    ("s2g batujnax", "Batujnax"),
    ("loud less", "Less"),
    ("vit less", "Less"),
    ("prx jinggg", "Jinggg"),
    ("jingggxd", "Jinggg"),
    ("sen johnqt", "Johnqt"),
    ("johnqt", "Johnqt"),
    ("sen zekken", "Zekken"),
    ("zekken", "Zekken"),
    ("liquid nats", "nAts"),
    ("nats", "nAts"),
    ("navi suygetsu", "Suygetsu"),
    ("suygetsu", "Suygetsu"),
    ("edg zmjjkk", "ZmjjKK"),
    ("zmjjkk", "ZmjjKK"),
    ("sen reduxx", "Reduxx"),
    ("s2g lovers rock", "Lovers Rock"),
    ("sup lovers rock", "Lovers Rock"),
    ("loversrocku7-e7a3 emea rk1", "Lovers Rock"),
    ("vit sayonara", "Sayonara"),
    ("sen nightz", "Nightz"),
    ("nrg s0m", "S0M"),
    ("cloud9 oxy", "Oxy"),
    ("edg nobody", "Nobody"),
    ("edg smoggy", "Smoggy"),
    ("envy eggsterr", "Eggsterr"),
    ("envy inspire", "Inspire"),
    ("envy ion2x", "ion2x"),
    ("fnc kaajak", "Kaajak"),
    ("faceit#1 cozart", "Cozart"),
    ("prx something", "Something"),
    ("th wo0t", "Wo0t"),
    ("tln primmie", "Primmie"),
    ("ttv day0s", "day0s"),
    ("twitch dasnerth", "Dasnerth"),
    ("twitch temet", "Temet"),
    ("strogo", "StRoGo"),
];

pub fn submitted_archive_from_response(response: &ResponseData, title: String) -> SubmittedArchive {
    let aid = response
        .data
        .as_ref()
        .and_then(extract_aid_from_response_data);
    SubmittedArchive { aid, title }
}

pub async fn sync_archives_to_collection(
    ctx: &Context,
    bilibili: &BiliBili,
    cookie_file: Option<&str>,
    submissions: &[SubmittedArchive],
) -> AppResult<()> {
    let processors = ctx.live_streamer().postprocessor.as_deref().unwrap_or(&[]);
    let collection_configs: Vec<&BiliCollectionConfig> = processors
        .iter()
        .filter_map(|step| step.bili_collection_config())
        .collect();

    if collection_configs.is_empty() || submissions.is_empty() {
        return Ok(());
    }

    let canonical = normalize_name(&ctx.live_streamer().remark);
    for cfg in collection_configs {
        sync_archives_with_config(bilibili, cookie_file, &canonical, submissions, cfg).await?;
    }

    Ok(())
}

async fn sync_archives_with_config(
    bilibili: &BiliBili,
    cookie_file: Option<&str>,
    canonical: &str,
    submissions: &[SubmittedArchive],
    config: &BiliCollectionConfig,
) -> AppResult<()> {
    let state_path = resolve_state_path(config, cookie_file).await?;
    let state = load_state(&state_path).await?;
    let section_id = state
        .streamer_collections
        .get(canonical)
        .map(|entry| entry.section_id as u64)
        .ok_or_else(|| {
            AppError::Custom(format!(
                "No collection configured for {canonical} in {}",
                state_path.display()
            ))
        })?;

    info!(
        streamer = canonical,
        section_id,
        state_file = %state_path.display(),
        submissions = submissions.len(),
        "Syncing submitted archives into Bilibili collection"
    );

    for submission in submissions {
        let aid = match submission.aid {
            Some(aid) => aid,
            None => find_recent_archive_aid(bilibili, &submission.title, config.archive_pages).await?,
        };

        if section_contains_aid(bilibili, section_id, aid).await? {
            info!(aid, section_id, "Archive already exists in collection");
            continue;
        }

        let video = get_video_info_with_retry(bilibili, aid, config).await?;
        add_to_collection_with_retry(bilibili, section_id, aid, &video, config).await?;
    }

    Ok(())
}

async fn resolve_state_path(
    config: &BiliCollectionConfig,
    cookie_file: Option<&str>,
) -> AppResult<PathBuf> {
    let path = PathBuf::from(&config.state_file);
    if path.is_absolute() {
        return Ok(path);
    }

    if let Some(cookie_file) = cookie_file {
        let cookie_path = Path::new(cookie_file);
        if let Some(parent) = cookie_path.parent()
            && !parent.as_os_str().is_empty()
        {
            return Ok(parent.join(path));
        }
    }

    std::env::current_dir()
        .map(|cwd| cwd.join(path))
        .change_context(AppError::Unknown)
}

async fn load_state(path: &Path) -> AppResult<CollectionsState> {
    let raw = fs::read_to_string(path)
        .await
        .change_context(AppError::Unknown)?;
    serde_json::from_str(&raw).change_context(AppError::Unknown)
}

async fn find_recent_archive_aid(
    bilibili: &BiliBili,
    title: &str,
    archive_pages: u32,
) -> AppResult<u64> {
    for page in 1..=archive_pages.max(1) {
        let response: ResponseData = bilibili
            .client
            .get("https://member.bilibili.com/x/web/archives")
            .query(&[
                ("pn", page.to_string()),
                ("ps", "10".to_string()),
                ("tid", "0".to_string()),
                ("keyword", "".to_string()),
                ("order", "pubdate".to_string()),
                ("jsonp", "jsonp".to_string()),
            ])
            .send()
            .await
            .change_context(AppError::Unknown)?
            .json()
            .await
            .change_context(AppError::Unknown)?;

        let Some(data) = response.data else {
            continue;
        };
        let Some(archives) = data.get("arc_audits").and_then(Value::as_array) else {
            continue;
        };

        for item in archives {
            let archive = item.get("Archive").unwrap_or(item);
            let archive_title = archive
                .get("title")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if archive_title.eq_ignore_ascii_case(title) {
                if let Some(aid) = archive.get("aid").and_then(Value::as_u64) {
                    info!(aid, title, page, "Matched recent archive for collection sync");
                    return Ok(aid);
                }
            }
        }
    }

    bail!(AppError::Custom(format!(
        "Unable to find recent archive for title: {title}"
    )));
}

async fn get_video_info_with_retry(
    bilibili: &BiliBili,
    aid: u64,
    config: &BiliCollectionConfig,
) -> AppResult<VideoInfo> {
    let retries = config.video_info_retries.max(1);
    for attempt in 1..=retries {
        let response: ResponseData = bilibili
            .client
            .get("https://api.bilibili.com/x/web-interface/view")
            .query(&[("aid", aid.to_string())])
            .send()
            .await
            .change_context(AppError::Unknown)?
            .json()
            .await
            .change_context(AppError::Unknown)?;

        if let Some(data) = response.data
            && let Some(pages) = data.get("pages").and_then(Value::as_array)
            && let Some(first) = pages.first()
            && let Some(cid) = first.get("cid").and_then(Value::as_u64)
        {
            let title = data
                .get("title")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            return Ok(VideoInfo { title, cid });
        }

        warn!(
            aid,
            attempt,
            retries,
            code = response.code,
            "Video info not ready for collection sync"
        );
        if attempt < retries {
            sleep(Duration::from_secs(config.video_info_retry_delay_secs.max(1))).await;
        }
    }

    bail!(AppError::Custom(format!(
        "Unable to load video info for aid={aid}"
    )));
}

async fn section_contains_aid(bilibili: &BiliBili, section_id: u64, aid: u64) -> AppResult<bool> {
    let response: ResponseData = bilibili
        .client
        .get("https://member.bilibili.com/x2/creative/web/season/section")
        .query(&[("id", section_id.to_string())])
        .send()
        .await
        .change_context(AppError::Unknown)?
        .json()
        .await
        .change_context(AppError::Unknown)?;

    let Some(data) = response.data else {
        return Ok(false);
    };
    let Some(episodes) = data.get("episodes").and_then(Value::as_array) else {
        return Ok(false);
    };
    Ok(episodes
        .iter()
        .filter_map(|episode| episode.get("aid").and_then(Value::as_u64))
        .any(|existing_aid| existing_aid == aid))
}

async fn add_to_collection_with_retry(
    bilibili: &BiliBili,
    section_id: u64,
    aid: u64,
    video: &VideoInfo,
    config: &BiliCollectionConfig,
) -> AppResult<()> {
    let csrf = extract_csrf(bilibili)?;
    let retries = config.video_info_retries.max(1);

    for attempt in 1..=retries {
        let response: ResponseData = bilibili
            .client
            .post(format!(
                "https://member.bilibili.com/x2/creative/web/season/section/episodes/add?csrf={csrf}"
            ))
            .json(&json!({
                "sectionId": section_id,
                "episodes": [{
                    "aid": aid,
                    "cid": video.cid,
                    "title": video.title,
                    "charging_pay": 0
                }],
                "csrf": csrf,
            }))
            .send()
            .await
            .change_context(AppError::Unknown)?
            .json()
            .await
            .change_context(AppError::Unknown)?;

        if response.code == 0 {
            info!(aid, section_id, "Successfully added archive to collection");
            return Ok(());
        }

        if section_contains_aid(bilibili, section_id, aid).await? {
            info!(aid, section_id, "Archive already appeared in collection after add attempt");
            return Ok(());
        }

        warn!(
            aid,
            section_id,
            attempt,
            retries,
            code = response.code,
            "Add to collection failed, retrying"
        );
        if attempt < retries {
            sleep(Duration::from_secs(config.video_info_retry_delay_secs.max(1))).await;
        }
    }

    bail!(AppError::Custom(format!(
        "Unable to add aid={aid} to collection section={section_id}"
    )));
}

fn extract_csrf(bilibili: &BiliBili) -> AppResult<&str> {
    Ok(bilibili
        .login_info
        .cookie_info
        .get("cookies")
        .and_then(Value::as_array)
        .and_then(|cookies| {
            cookies
                .iter()
                .find(|cookie| cookie.get("name").and_then(Value::as_str) == Some("bili_jct"))
        })
        .and_then(|cookie| cookie.get("value"))
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::Custom("Unable to extract bili_jct csrf".to_string()))?)
}

fn extract_aid_from_response_data(data: &Value) -> Option<u64> {
    data.get("aid")
        .and_then(Value::as_u64)
        .or_else(|| {
            data.get("archive")
                .or_else(|| data.get("Archive"))
                .and_then(|archive| archive.get("aid"))
                .and_then(Value::as_u64)
        })
        .or_else(|| data.get("avid").and_then(Value::as_u64))
}

fn normalize_name(raw: &str) -> String {
    let key = raw.to_lowercase();
    let key = key.trim().trim_end_matches('_');
    PLAYER_ALIASES
        .iter()
        .find_map(|(alias, canonical)| (*alias == key).then_some((*canonical).to_string()))
        .unwrap_or_else(|| raw.trim().trim_end_matches('_').to_string())
}
