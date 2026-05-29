use futures::Stream;
use rand::distributions::uniform::{UniformFloat, UniformSampler};
use std::future::Future;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::info;
pub mod client;
pub mod downloader;
pub mod error;
pub mod uploader;

pub use uploader::bilibili;
pub use uploader::credential;

/// 全局投稿限速：相邻两次 submit 发起至少间隔这么多毫秒（0 = 不限速）。
static SUBMIT_MIN_INTERVAL_MS: AtomicU64 = AtomicU64::new(0);
/// 上一次 submit 发起的时刻，跨所有 uploader actor 共享。
static SUBMIT_LAST: LazyLock<tokio::sync::Mutex<Option<Instant>>> =
    LazyLock::new(|| tokio::sync::Mutex::new(None));

/// 设置全局投稿最小间隔。server 启动读到 config 后调用一次。
pub fn set_submit_min_interval(d: Duration) {
    SUBMIT_MIN_INTERVAL_MS.store(d.as_millis() as u64, Ordering::Relaxed);
}

/// 投稿限速闸门：在每次真正发 submit 请求前调用。
///
/// 持锁 sleep 把多个并发会话的 submit 串成「全局串行 + 最小间隔」，从源头压低
/// B 站 21566（投稿过于频繁）的触发概率。放在 retry 的「每次尝试」层而非整个
/// retry 外——这样限流会话 backoff 等待期间闸门是释放的，不会堵住其它会话。
pub async fn submit_gate() {
    let ms = SUBMIT_MIN_INTERVAL_MS.load(Ordering::Relaxed);
    if ms == 0 {
        return;
    }
    let min = Duration::from_millis(ms);
    let mut last = SUBMIT_LAST.lock().await;
    if let Some(t) = *last {
        let elapsed = t.elapsed();
        if elapsed < min {
            let wait = min - elapsed;
            info!("submit gate: pacing {:.1}s before next submit", wait.as_secs_f64());
            sleep(wait).await;
        }
    }
    *last = Some(Instant::now());
}

pub async fn retry<F, Fut, O, E: std::fmt::Display>(f: F) -> Result<O, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<O, E>>,
{
    retry_with_config(f, 3, None::<fn(&E) -> bool>).await
}

pub async fn retry_with_config<F, Fut, O, E, P>(
    mut f: F,
    max_retries: usize,
    should_retry: Option<P>,
) -> Result<O, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<O, E>>,
    E: std::fmt::Display,
    P: Fn(&E) -> bool,
{
    let mut retries = max_retries;
    let mut wait = 1;
    let mut jittered_wait_for;
    loop {
        match f().await {
            Err(e) if retries > 0 => {
                // 如果提供了 should_retry 条件，检查是否应该重试
                if let Some(ref predicate) = should_retry
                    && !predicate(&e) {
                        break Err(e);
                    }

                retries -= 1;
                let jitter_factor =
                    UniformFloat::<f64>::sample_single(0., 1., &mut rand::thread_rng());
                wait *= 2;

                jittered_wait_for = f64::min(jitter_factor + (wait as f64), 64.);
                info!(
                    "Retry attempt #{}. Sleeping {:?} before the next attempt. {e}",
                    max_retries - retries,
                    jittered_wait_for
                );
                sleep(Duration::from_secs_f64(jittered_wait_for)).await;
            }
            res => break res,
        }
    }
}

/// Retry with an explicit, fixed delay schedule (in seconds, with up to ±10% jitter).
/// Use for rate-limit errors whose cooldown is much longer than the default exponential
/// schedule's 64s cap (e.g. Bilibili 21566 / 601 which can require hours to clear).
///
/// `should_retry` returns true to consume the next slot in `schedule`. Returning false
/// breaks immediately with the error. After the schedule is exhausted the last error
/// is returned.
pub async fn retry_with_schedule<F, Fut, O, E, P>(
    mut f: F,
    schedule: &[u64],
    should_retry: P,
) -> Result<O, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<O, E>>,
    E: std::fmt::Display,
    P: Fn(&E) -> bool,
{
    let mut idx = 0;
    loop {
        match f().await {
            Err(e) if idx < schedule.len() => {
                if !should_retry(&e) {
                    break Err(e);
                }
                let base = schedule[idx] as f64;
                let jitter_factor =
                    UniformFloat::<f64>::sample_single(-0.1, 0.1, &mut rand::thread_rng());
                let wait = (base * (1.0 + jitter_factor)).max(1.0);
                info!(
                    "Rate-limit retry attempt #{}/{}. Sleeping {:.1}s before the next attempt. {e}",
                    idx + 1,
                    schedule.len(),
                    wait
                );
                sleep(Duration::from_secs_f64(wait)).await;
                idx += 1;
            }
            res => break res,
        }
    }
}

trait ReqwestClientBuilderExt {
    fn proxy_builder<U: reqwest::IntoUrl>(proxy: Option<U>) -> reqwest::ClientBuilder;
}

impl ReqwestClientBuilderExt for reqwest::Client {
    fn proxy_builder<U: reqwest::IntoUrl>(proxy: Option<U>) -> reqwest::ClientBuilder {
        match proxy {
            Some(proxy) => {
                tracing::debug!("使用代理: {}", proxy.as_str());
                Self::builder().proxy(reqwest::Proxy::all(proxy).unwrap())
            }
            None => Self::builder(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::uploader::bilibili::Vid;
    use std::str::FromStr;
    use tracing::Level;
    use tracing_subscriber::FmtSubscriber;
    use url::Url;

    #[test]
    fn it_works() {
        assert_eq!(Ok(Vid::Aid(10)), Vid::from_str("10"));
        assert_eq!(Ok(Vid::Aid(971158452)), Vid::from_str("971158452"));
        assert_eq!(Ok(Vid::Aid(971158452)), Vid::from_str("av971158452"));
        assert_eq!(
            Ok(Vid::Bvid("BV1ip4y1x7Gi".into())),
            Vid::from_str("BV1ip4y1x7Gi")
        );
    }

    #[tokio::test]
    async fn try_clone_stream() {
        let subscriber = FmtSubscriber::builder()
            .with_max_level(Level::INFO)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
        Url::parse("https://bilibili.com/").unwrap();
        let chunks: Vec<Result<_, ::std::io::Error>> = vec![Ok("hello"), Ok(" "), Ok("world")];
        let _stream = futures::stream::iter(chunks);
    }
}
