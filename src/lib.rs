//! # chan_downloader
//!
//! `chan_downloader` is a collection of utilities to
//! download images/videos from a 4chan thread

use log::{info, warn};
use reqwest::{Client, Error, StatusCode, Url, header};
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    fmt,
    io,
    sync::{Arc, Mutex, OnceLock, Weak},
    time::Duration,
};
use tokio::{fs, io::AsyncWriteExt, sync::Mutex as AsyncMutex};

/// Attempts per request when it fails for a transient reason. With the waits
/// between attempts, retries cover about a minute: the length of the longest
/// stalls seen on 4chan's CDN
const ATTEMPTS: u32 = 5;

/// Wait before the first retry, doubled for each next one
const RETRY_DELAY: Duration = if cfg!(test) {
    Duration::from_millis(10)
} else {
    Duration::from_secs(1)
};

/// Represents a 4chan thread
#[derive(Debug)]
pub struct Thread {
    pub board: String,
    pub id:    u32,
}

#[derive(Debug)]
pub struct Link {
    pub url:  String,
    pub name: String,
}

/// Returns an HTTP client suited to the downloads: it identifies the
/// downloader, and gives up on connections that stall so that the requests can
/// be retried.
///
/// 4chan's CDN sometimes stops answering on an HTTP/2 connection for about a
/// minute: pings detect it, and the connection is dropped instead of being
/// reused by the retries.
pub fn client() -> Result<Client, Error> {
    Client::builder()
        .user_agent(concat!("chan-downloader/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(10))
        .http2_keep_alive_interval(Duration::from_secs(5))
        .http2_keep_alive_timeout(Duration::from_secs(5))
        .build()
}

/// Whether a request that failed with `err` is worth retrying
fn is_transient(err: &Error) -> bool {
    match err.status() {
        Some(status) => status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS,
        None =>
            err.is_timeout() || err.is_connect() || err.is_request() || err.is_body() || err.is_decode(),
    }
}

/// Runs `request` until it succeeds, fails for a non transient reason, or
/// [`ATTEMPTS`] attempts were made
async fn with_retries<T, E, F, Fut>(
    url: &str,
    is_transient: impl Fn(&E) -> bool,
    mut request: F,
) -> Result<T, E>
where
    E: fmt::Display,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut attempt = 1;
    loop {
        match request().await {
            Err(err) if attempt < ATTEMPTS && is_transient(&err) => {
                warn!("Attempt {attempt} for {url} failed, retrying: {err}");
                tokio::time::sleep(RETRY_DELAY * 2_u32.pow(attempt - 1)).await;
                attempt += 1;
            },
            result => return result,
        }
    }
}

/// Why a download failed
#[derive(Debug)]
pub enum DownloadError {
    /// The request or the response body failed
    Http(Error),
    /// The local file could not be read or written
    Io(io::Error),
    /// The server keeps answering resume requests inconsistently, even after
    /// the transfer was restarted from scratch
    Resume(String),
}

impl fmt::Display for DownloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http(err) => write!(f, "{err}"),
            Self::Io(err) => write!(f, "{err}"),
            Self::Resume(reason) => write!(f, "cannot resume download: {reason}"),
        }
    }
}

impl std::error::Error for DownloadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Http(err) => Some(err),
            Self::Io(err) => Some(err),
            Self::Resume(_) => None,
        }
    }
}

impl From<Error> for DownloadError {
    fn from(err: Error) -> Self {
        Self::Http(err)
    }
}

impl From<io::Error> for DownloadError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

impl DownloadError {
    /// HTTP status of the failure, if it came from a response
    #[must_use]
    pub fn status(&self) -> Option<StatusCode> {
        match self {
            Self::Http(err) => err.status(),
            _ => None,
        }
    }
}

/// Whether a failed download is worth retrying: each attempt resumes from the
/// bytes already on disk
fn is_transient_download(err: &DownloadError) -> bool {
    match err {
        DownloadError::Http(err) => is_transient(err),
        DownloadError::Io(_) => true,
        DownloadError::Resume(_) => false,
    }
}

/// Lock of each path being written, so that a file queued twice (e.g. the
/// same thread given several times) has only one writer: the others wait,
/// then see the finished file
fn path_lock(path: &str) -> Arc<AsyncMutex<()>> {
    static LOCKS: OnceLock<Mutex<HashMap<String, Weak<AsyncMutex<()>>>>> = OnceLock::new();
    let locks = LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut locks = locks.lock().unwrap();
    if let Some(lock) = locks.get(path).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(AsyncMutex::new(()));
    locks.insert(String::from(path), Arc::downgrade(&lock));
    lock
}

/// Saves the image from the url to the given path, retrying on transient
/// failures. Returns the path on success
///
/// The transfer resumes from the `.part` file left by an interrupted run, and
/// the file only appears at `path` once it is complete. A path that already
/// exists is left untouched (and its stale `.part` cleaned up).
///
/// # Examples
///
/// ```
/// use std::{env, fs::remove_file};
/// let client = chan_downloader::client().unwrap();
/// let workpath = env::current_dir().unwrap().join("1745612650141704.png");
/// let url = "https://i.4cdn.org/g/1745612650141704.png";
/// async {
///     let answer = chan_downloader::save_image(url, workpath.to_str().unwrap(), &client)
///         .await
///         .unwrap();
///     assert_eq!(workpath.to_str().unwrap(), answer);
///     remove_file(answer).unwrap();
/// };
/// ```
pub async fn save_image(url: &str, path: &str, client: &Client) -> Result<String, DownloadError> {
    info!(target: "image_events", "Saving image to: {}", path);
    let lock = path_lock(path);
    let _writer = lock.lock().await;
    if fs::metadata(path).await.is_ok() {
        // Saved by another worker while this one waited, or by a previous run
        info!("Image {} already saved. Skipped", path);
        let _ = fs::remove_file(format!("{path}.part")).await;
        return Ok(String::from(path));
    }
    with_retries(url, is_transient_download, || download(url, path, client)).await?;
    info!("Saved image to: {}", path);
    Ok(String::from(path))
}

/// Streams the file to `<path>.part`, renamed to `path` once complete, so a
/// failed transfer never leaves a truncated file that later runs would skip.
///
/// The `.part` file is kept on failure and the transfer resumes where it
/// stopped: the next attempt requests the missing bytes with a `Range` header
/// and only appends when the server confirms the offset with a matching
/// `Content-Range`. A server that ignores `Range` (200) restarts the file
/// from scratch; one that rejects it (416) or answers it inconsistently makes
/// the transfer restart once, then fail.
async fn download(url: &str, path: &str, client: &Client) -> Result<(), DownloadError> {
    let part = format!("{path}.part");
    let mut restarted = false;
    loop {
        let offset = fs::metadata(&part).await.map_or(0, |meta| meta.len());
        let mut request = client.get(url);
        if offset > 0 {
            request = request.header(header::RANGE, format!("bytes={offset}-"));
        }
        let response = request.send().await?;
        let status = response.status();
        if status == StatusCode::RANGE_NOT_SATISFIABLE && offset > 0 {
            // The saved bytes don't match the content anymore: start over, once
            if restarted {
                return Err(DownloadError::Resume(format!(
                    "{url} rejects the saved {offset} bytes"
                )));
            }
            warn!("{url} rejects resuming from {offset} bytes, restarting");
            let _ = fs::remove_file(&part).await;
            restarted = true;
            continue;
        }
        let append = if status == StatusCode::PARTIAL_CONTENT {
            let start = response
                .headers()
                .get(header::CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .and_then(content_range_start);
            match start {
                Some(start) if start == offset => true,
                start => {
                    // A 206 that doesn't continue the saved bytes can't be
                    // appended to them: start over, once
                    if restarted {
                        return Err(DownloadError::Resume(format!(
                            "{url} keeps answering the resume from {offset} with an inconsistent \
                             Content-Range ({start:?})"
                        )));
                    }
                    warn!("{url} answered the resume from {offset} inconsistently, restarting");
                    let _ = fs::remove_file(&part).await;
                    restarted = true;
                    continue;
                },
            }
        } else {
            false
        };
        // A 200 after a Range request means the server ignored it: the file
        // is truncated and written from scratch. Error statuses fail here.
        let mut response = response.error_for_status()?;
        let mut file = open_part(&part, append).await?;
        let transfer = async {
            while let Some(chunk) = response.chunk().await? {
                file.write_all(&chunk).await?;
            }
            file.flush().await?;
            Ok::<(), DownloadError>(())
        };
        // The `.part` file keeps the valid bytes on failure (including a
        // cancellation dropping this future): the next attempt resumes there
        transfer.await?;
        return publish(&part, path).await;
    }
}

/// Opens the `.part` file to append the resumed bytes, or truncated to start
/// over
async fn open_part(part: &str, append: bool) -> Result<fs::File, DownloadError> {
    Ok(fs::OpenOptions::new()
        .create(true)
        .write(true)
        .append(append)
        .truncate(!append)
        .open(part)
        .await?)
}

/// Publishes the complete `.part` file as the final file, atomically. A stale
/// final file is replaced: `rename` fails on an existing destination on
/// Windows
async fn publish(part: &str, path: &str) -> Result<(), DownloadError> {
    match fs::rename(part, path).await {
        Ok(()) => Ok(()),
        Err(err) if fs::metadata(path).await.is_ok() => {
            warn!("Replacing the existing {path}: {err}");
            fs::remove_file(path).await?;
            fs::rename(part, path).await?;
            Ok(())
        },
        Err(err) => Err(err.into()),
    }
}

/// Start offset of a `Content-Range: bytes <start>-<end>/<total>` header, if
/// it is present and consistent (`end` reaches `total`)
fn content_range_start(value: &str) -> Option<u64> {
    let (range, total) = value.strip_prefix("bytes ")?.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    let start = start.trim().parse::<u64>().ok()?;
    let end = end.trim().parse::<u64>().ok()?;
    if end < start {
        return None;
    }
    match total.trim() {
        "*" => {},
        total if total.parse::<u64>().ok() == Some(end + 1) => {},
        _ => return None,
    }
    Some(start)
}

/// Result of [`get_page_content_since`]
#[derive(Debug, PartialEq, Eq)]
pub enum Page {
    /// The page changed: its content and `Last-Modified` header
    Modified {
        content:       String,
        last_modified: Option<String>,
    },
    NotModified,
}

/// Returns the page content from the given url, unless it wasn't modified
/// since `last_modified` (the `Last-Modified` header of a previous load).
/// Retries on transient failures, and fails if the server answers with an
/// error status (e.g. 403 or 404)
pub async fn get_page_content_since(
    url: &str,
    client: &Client,
    last_modified: Option<&str>,
) -> Result<Page, Error> {
    info!(target: "page_events", "Loading page: {}", url);
    let page = with_retries(url, is_transient, async || {
        let mut request = client.get(url);
        if let Some(since) = last_modified {
            request = request.header(header::IF_MODIFIED_SINCE, since);
        }
        let response = request.send().await?.error_for_status()?;
        if response.status() == StatusCode::NOT_MODIFIED {
            return Ok(Page::NotModified);
        }
        let last_modified = response
            .headers()
            .get(header::LAST_MODIFIED)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        Ok(Page::Modified {
            content: response.text().await?,
            last_modified,
        })
    })
    .await?;
    info!("Loaded page: {}", url);
    Ok(page)
}

/// Returns the page content from the given url, retrying on transient
/// failures. Fails if the server answers with an error status (e.g. 403 or 404)
///
/// # Examples
///
/// ```
/// use reqwest::Client;
/// use std::io;
/// let client = Client::builder().user_agent("reqwest").build().unwrap();
/// let url = "https://raw.githubusercontent.com/mariot/chan-downloader/master/.gitignore";
/// async {
///     let result = chan_downloader::get_page_content(url, &client)
///         .await
///         .unwrap();
///     assert_eq!(result, "/target/\nCargo.lock\n**/*.rs.bk\n");
/// };
/// ```
pub async fn get_page_content(url: &str, client: &Client) -> Result<String, Error> {
    match get_page_content_since(url, client, None).await? {
        Page::Modified { content, .. } => Ok(content),
        Page::NotModified => unreachable!("not modified without If-Modified-Since"),
    }
}

/// Returns the board name and thread id.
///
/// # Examples
///
/// ```
/// let url = "https://boards.4chan.org/wg/thread/6872254";
/// let thread = chan_downloader::get_thread_info(url);
///
/// assert_eq!(thread.board, "wg");
/// assert_eq!(thread.id, 6872254);
/// ```
#[must_use]
pub fn get_thread_info(url: &str) -> Thread {
    info!(target: "thread_events", "Getting thread info from: {}", url);
    let url_vec: Vec<&str> = url.split('/').collect();
    let board_name = url_vec[3];
    let thread_vec: Vec<&str> = url_vec[5].split('#').collect();
    let thread_id = thread_vec[0];
    info!("Got thread info from: {}", url);

    Thread {
        board: board_name.to_owned(),
        id:    thread_id.parse::<u32>().expect("failed to parse thread id"),
    }
}

/// Returns the image/video links from a page, without duplicates and in page
/// order. Supported extensions: jpg, png, gif, webm, mp4 and pdf.
///
/// # Examples
///
/// ```
/// use reqwest::Client;
/// let client = Client::builder().user_agent("reqwest").build().unwrap();
/// let url = "https://boards.4chan.org/wg/thread/6872254";
/// async {
///     match chan_downloader::get_page_content(url, &client).await {
///         Ok(page_string) => {
///             let links_iter = chan_downloader::get_image_links(page_string.as_str());
///
///             for link in links_iter {
///                 println!("{} and {}", link.name, link.url);
///             }
///         },
///         Err(err) => eprintln!("Error: {}", err),
///     }
/// };
/// ```
///
/// Sample image links:
//    - https://i.4pcdn.org/x/1438502659556.jpg
//    - https://img.4plebs.org/boards/x/image/1660/66/1660662319160984.png
//    - https://i.4cdn.org/sp/1661019073822058.jpg
#[must_use]
pub fn get_image_links(page_content: &str) -> Vec<Link> {
    info!(target: "link_events", "Getting image links");
    let reg = regex!(
        r"(//i(?:s|mg)?(?:\d*)?\.(?:4cdn|4chan|4plebs|4pcdn)\.org/(?:\w+/){1,3}(?:\d+/){0,2}(\d+\.(?:jpg|png|gif|webm|mp4|pdf)))"
    );

    // A link appears several times per post: twice for images, three times
    // for videos (extra `<link itemprop="contentUrl">`)
    let mut seen = HashSet::new();
    let links_v: Vec<Link> = reg
        .captures_iter(page_content)
        .filter(|cap| seen.insert(cap[1].to_owned()))
        .map(|cap| Link {
            url:  String::from(&cap[1]),
            name: String::from(&cap[2]),
        })
        .collect();
    info!("Got {} image links from page", links_v.len());
    links_v
}

/// Returns the 4chan API url of a thread, or `None` if the url is not a 4chan
/// thread (e.g. 4plebs)
///
/// # Examples
///
/// ```
/// let url = "https://boards.4chan.org/wg/thread/6872254/sticky-thread";
/// assert_eq!(
///     chan_downloader::get_api_url(url).unwrap(),
///     "https://a.4cdn.org/wg/thread/6872254.json"
/// );
/// assert!(chan_downloader::get_api_url("https://archive.4plebs.org/x/thread/32661196").is_none());
/// ```
#[must_use]
pub fn get_api_url(url: &str) -> Option<String> {
    let host = url.split('/').nth(2)?;
    if host != "boards.4chan.org" && host != "boards.4channel.org" {
        return None;
    }
    let thread = get_thread_info(url);
    Some(format!(
        "https://a.4cdn.org/{}/thread/{}.json",
        thread.board, thread.id
    ))
}

#[derive(Deserialize)]
struct ApiThread {
    posts: Vec<ApiPost>,
}

#[derive(Deserialize)]
struct ApiPost {
    tim:         Option<u64>,
    ext:         Option<String>,
    filename:    Option<String>,
    #[serde(default)]
    filedeleted: u8,
}

/// Returns the image/video links of a thread from its 4chan API json (see
/// [`get_api_url`]), in post order.
///
/// # Examples
///
/// ```
/// let json = r#"{"posts": [{"no": 1, "tim": 1489266570954, "ext": ".jpg"}, {"no": 2}]}"#;
/// let links = chan_downloader::get_api_image_links(json, "wg").unwrap();
///
/// assert_eq!(links.len(), 1);
/// assert_eq!(links[0].url, "//i.4cdn.org/wg/1489266570954.jpg");
/// assert_eq!(links[0].name, "1489266570954.jpg");
/// ```
pub fn get_api_image_links(json: &str, board: &str) -> Result<Vec<Link>, serde_json::Error> {
    info!(target: "link_events", "Getting image links from API");
    let thread: ApiThread = serde_json::from_str(json)?;
    let links_v: Vec<Link> = thread
        .posts
        .into_iter()
        .filter(|post| post.filedeleted == 0)
        .filter_map(|post| {
            let ext = post.ext?;
            // /f/ serves files under their original name
            let name = if board == "f" {
                format!("{}{}", post.filename?, ext)
            } else {
                format!("{}{}", post.tim?, ext)
            };
            let mut url = Url::parse("https://i.4cdn.org").ok()?;
            url.path_segments_mut().ok()?.push(board).push(&name);
            Some(Link {
                url: url.as_str().trim_start_matches("https:").to_owned(),
                name,
            })
        })
        .collect();
    info!("Got {} image links from API", links_v.len());
    Ok(links_v)
}

/// Initialize a [`Regex`] once
#[macro_export]
macro_rules! regex {
    ($re:expr $(,)?) => {{
        static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
        RE.get_or_init(|| regex::Regex::new($re).unwrap())
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::Client;
    use wiremock::{
        Mock,
        MockServer,
        Request,
        ResponseTemplate,
        matchers::{header as request_header, method, path},
    };

    #[test]
    fn it_gets_4chan_thread_info() {
        let url = "https://boards.4chan.org/wg/thread/6872254";
        let thread = get_thread_info(url);
        assert_eq!(thread.board, "wg");
        assert_eq!(thread.id, 6872254);
    }

    #[test]
    fn it_gets_4plebs_thread_info() {
        let url = "https://archive.4plebs.org/x/thread/32661196";
        let thread = get_thread_info(url);
        assert_eq!(thread.board, "x");
        assert_eq!(thread.id, 32661196);
    }

    #[test]
    fn it_gets_4chan_image_links() {
        let links_iter = get_image_links(
            r#"
            <a href="//i.4cdn.org/wg/1489266570954.jpg" target="_blank">stickyop.jpg</a>
            <a href="//i.4cdn.org/wg/1489266570954.jpg" target="_blank">stickyop.jpg</a>
        "#,
        );
        assert_eq!(links_iter.len(), 1);
        for link in links_iter {
            assert_eq!(link.url, "//i.4cdn.org/wg/1489266570954.jpg");
            assert_eq!(link.name, "1489266570954.jpg");
        }
    }

    #[test]
    fn it_gets_4plebs_image_links() {
        let links_iter = get_image_links(
            r#"
            <a href="https://img.4plebs.org/boards/x/image/1660/66/1660662319160984.png" target="_blank"></a>
            <a href="https://img.4plebs.org/boards/x/image/1660/66/1660662319160984.png" target="_blank"></a>
        "#,
        );
        assert_eq!(links_iter.len(), 1);
        for link in links_iter {
            assert_eq!(
                link.url,
                "//img.4plebs.org/boards/x/image/1660/66/1660662319160984.png"
            );
            assert_eq!(link.name, "1660662319160984.png");
        }
    }

    #[test]
    fn it_gets_thread_info_with_slug() {
        let url = "https://boards.4chan.org/p/thread/4535213/bdp-bird-posting-discussion";
        let thread = get_thread_info(url);
        assert_eq!(thread.board, "p");
        assert_eq!(thread.id, 4535213);
    }

    #[test]
    fn it_gets_thread_info_with_post_anchor() {
        let thread = get_thread_info("https://boards.4chan.org/wg/thread/6872254#p6872300");
        assert_eq!(thread.board, "wg");
        assert_eq!(thread.id, 6872254);
    }

    #[test]
    #[should_panic(expected = "failed to parse thread id")]
    fn it_panics_on_non_numeric_thread_id() {
        let _ = get_thread_info("https://boards.4chan.org/wg/thread/catalog");
    }

    #[test]
    fn it_keeps_one_link_per_image_in_page_order() {
        // 4chan pages contain each full-size image link twice
        let links = get_image_links(
            r#"
            <a href="//i.4cdn.org/wg/1111111111111.png">a.png</a>
            <a href="//i.4cdn.org/wg/1111111111111.png"><img src="//i.4cdn.org/wg/1111111111111s.jpg"></a>
            <a href="//i.4cdn.org/wg/2222222222222.webm">b.webm</a>
            <a href="//i.4cdn.org/wg/2222222222222.webm"><img src="//i.4cdn.org/wg/2222222222222s.jpg"></a>
        "#,
        );
        let names: Vec<&str> = links.iter().map(|l| l.name.as_str()).collect();
        assert_eq!(names, ["1111111111111.png", "2222222222222.webm"]);
        assert_eq!(links[1].url, "//i.4cdn.org/wg/2222222222222.webm");
    }

    #[test]
    fn it_matches_all_supported_extensions_and_hosts() {
        let links = get_image_links(
            r#"
            //i.4cdn.org/g/1.jpg //i.4cdn.org/g/1.jpg
            //is2.4chan.org/g/2.gif //is2.4chan.org/g/2.gif
            //img.4plebs.org/boards/x/image/1660/66/3.webm //img.4plebs.org/boards/x/image/1660/66/3.webm
            //i.4cdn.org/3/4.mp4 //i.4cdn.org/3/4.mp4
            //img.4plebs.org/boards/x/image/1660/66/5.mp4 //img.4plebs.org/boards/x/image/1660/66/5.mp4
            //i.4cdn.org/po/6.pdf //i.4cdn.org/po/6.pdf
            //i.4pcdn.org/x/7.png //i.4pcdn.org/x/7s.jpg
        "#,
        );
        let names: Vec<&str> = links.iter().map(|l| l.name.as_str()).collect();
        assert_eq!(names, [
            "1.jpg", "2.gif", "3.webm", "4.mp4", "5.mp4", "6.pdf", "7.png"
        ]);
    }

    #[test]
    fn it_gets_each_video_once() {
        // Trimmed from a real /3/ thread: videos are linked three times
        let links = get_image_links(
            r#"
            <div class="fileText"><a href="//i.4cdn.org/3/1111111111111.png" target="_blank">a.png</a></div>
            <a class="fileThumb" href="//i.4cdn.org/3/1111111111111.png"><img src="//i.4cdn.org/3/1111111111111s.jpg"></a>
            <link itemprop="contentUrl" href="//i.4cdn.org/3/2222222222222.mp4">
            <div class="fileText"><a href="//i.4cdn.org/3/2222222222222.mp4" target="_blank">b.mp4</a></div>
            <a class="fileThumb" href="//i.4cdn.org/3/2222222222222.mp4"><img src="//i.4cdn.org/3/2222222222222s.jpg"></a>
            <link itemprop="contentUrl" href="//i.4cdn.org/3/3333333333333.webm">
            <div class="fileText"><a href="//i.4cdn.org/3/3333333333333.webm" target="_blank">c.webm</a></div>
            <a class="fileThumb" href="//i.4cdn.org/3/3333333333333.webm"><img src="//i.4cdn.org/3/3333333333333s.jpg"></a>
            <div class="fileText"><a href="//i.4cdn.org/3/4444444444444.jpg" target="_blank">d.jpg</a></div>
            <a class="fileThumb" href="//i.4cdn.org/3/4444444444444.jpg"><img src="//i.4cdn.org/3/4444444444444s.jpg"></a>
        "#,
        );
        let names: Vec<&str> = links.iter().map(|l| l.name.as_str()).collect();
        assert_eq!(names, [
            "1111111111111.png",
            "2222222222222.mp4",
            "3333333333333.webm",
            "4444444444444.jpg"
        ]);
    }

    #[test]
    fn it_ignores_other_hosts_and_extensions() {
        let links = get_image_links(
            r#"
            //i.example.org/g/1.jpg //i.example.org/g/1.jpg
            //i.4cdn.org/g/2.txt //i.4cdn.org/g/2.txt
        "#,
        );
        assert!(links.is_empty());
    }

    #[test]
    fn it_finds_no_links_in_empty_page() {
        assert!(get_image_links("").is_empty());
    }

    #[tokio::test]
    async fn it_gets_page_content() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/wg/thread/1"))
            .respond_with(ResponseTemplate::new(200).set_body_string("<html>thread</html>"))
            .mount(&server)
            .await;

        let url = format!("{}/wg/thread/1", server.uri());
        let result = get_page_content(&url, &Client::new()).await.unwrap();
        assert_eq!(result, "<html>thread</html>");
    }

    #[test]
    fn it_gets_api_url() {
        for (url, api) in [
            (
                "https://boards.4chan.org/wg/thread/6872254",
                "https://a.4cdn.org/wg/thread/6872254.json",
            ),
            (
                "https://boards.4chan.org/p/thread/4535213/bdp-bird-posting-discussion",
                "https://a.4cdn.org/p/thread/4535213.json",
            ),
            (
                "https://boards.4chan.org/g/thread/105076684#p105076700",
                "https://a.4cdn.org/g/thread/105076684.json",
            ),
            (
                "https://boards.4channel.org/g/thread/105076684",
                "https://a.4cdn.org/g/thread/105076684.json",
            ),
        ] {
            assert_eq!(get_api_url(url).as_deref(), Some(api), "{url}");
        }
    }

    #[test]
    fn it_has_no_api_url_for_other_sites() {
        assert!(get_api_url("https://archive.4plebs.org/x/thread/32661196").is_none());
        assert!(get_api_url("http://127.0.0.1:8080/wg/thread/1").is_none());
        assert!(get_api_url("not a url").is_none());
    }

    #[test]
    fn it_gets_api_image_links() {
        // Trimmed from https://a.4cdn.org/3/thread/1032629.json
        let json = r#"{"posts": [
            {"no": 1032629, "tim": 1789732438865756, "ext": ".png", "filename": "file", "com": "OP"},
            {"no": 1032630, "com": "text only reply"},
            {"no": 1032631, "tim": 1792994168478222, "ext": ".mp4", "filename": "0000-0200-Merged"},
            {"no": 1032632, "tim": 1792994168470000, "ext": ".jpg", "filename": "gone", "filedeleted": 1},
            {"no": 1032633, "tim": 1789749874484566, "ext": ".webm", "filename": "clip"}
        ]}"#;
        let links = get_api_image_links(json, "3").unwrap();
        let got: Vec<(&str, &str)> = links.iter().map(|l| (l.url.as_str(), l.name.as_str())).collect();
        assert_eq!(got, [
            ("//i.4cdn.org/3/1789732438865756.png", "1789732438865756.png"),
            ("//i.4cdn.org/3/1792994168478222.mp4", "1792994168478222.mp4"),
            ("//i.4cdn.org/3/1789749874484566.webm", "1789749874484566.webm"),
        ]);
    }

    #[test]
    fn it_uses_original_filenames_on_f() {
        let json = r#"{"posts": [
            {"no": 3524333, "tim": 1744587482715200, "ext": ".swf", "filename": "jesus on the cross"}
        ]}"#;
        let links = get_api_image_links(json, "f").unwrap();
        assert_eq!(links[0].url, "//i.4cdn.org/f/jesus%20on%20the%20cross.swf");
        assert_eq!(links[0].name, "jesus on the cross.swf");
    }

    #[test]
    fn it_rejects_invalid_api_json() {
        assert!(get_api_image_links("<html>Please wait</html>", "g").is_err());
        assert!(get_api_image_links(r#"{"error": "nope"}"#, "g").is_err());
    }

    #[tokio::test]
    async fn it_fails_to_get_page_content_on_error_status() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(403).set_body_string("<html>Please wait</html>"))
            .mount(&server)
            .await;

        let url = format!("{}/x/thread/1", server.uri());
        let err = get_page_content(&url, &Client::new()).await.unwrap_err();
        assert_eq!(err.status(), Some(reqwest::StatusCode::FORBIDDEN));
    }

    #[tokio::test]
    async fn it_saves_image() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/wg/1.png"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"\x89PNG fake".to_vec()))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("1.png");
        let dest = dest.to_str().unwrap();
        let url = format!("{}/wg/1.png", server.uri());
        let answer = save_image(&url, dest, &Client::new()).await.unwrap();
        assert_eq!(answer, dest);
        assert_eq!(std::fs::read(dest).unwrap(), b"\x89PNG fake");
        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            1,
            "temporary file left"
        );
    }

    #[tokio::test]
    async fn it_fails_to_save_missing_image() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("1.png");
        let url = format!("{}/wg/1.png", server.uri());
        let err = save_image(&url, dest.to_str().unwrap(), &Client::new())
            .await
            .unwrap_err();
        assert_eq!(err.status(), Some(reqwest::StatusCode::NOT_FOUND));
        assert!(!dest.exists());
    }

    #[tokio::test]
    async fn it_keeps_the_partial_file_when_transfer_fails() {
        use std::{
            io::{Read, Write},
            net::TcpListener,
        };
        // Server announcing 100 bytes but closing after 10
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0; 1024];
            let _ = stream.read(&mut request);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n0123456789")
                .unwrap();
        });

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("1.png");
        let url = format!("http://{addr}/wg/1.png");
        assert!(
            save_image(&url, dest.to_str().unwrap(), &Client::new())
                .await
                .is_err()
        );
        assert!(!dest.exists());
        // The valid bytes are kept for the next run to resume from
        let part = dir.path().join("1.png.part");
        assert_eq!(std::fs::read(&part).unwrap(), b"0123456789");
    }

    #[tokio::test]
    async fn it_resumes_a_partial_download() {
        let content: Vec<u8> = (0..100u8).collect();
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(request_header("Range", "bytes=10-"))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("Content-Range", "bytes 10-99/100")
                    .set_body_bytes(content[10..].to_vec()),
            )
            .expect(1)
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("1.png");
        std::fs::write(dir.path().join("1.png.part"), &content[..10]).unwrap();
        let url = format!("{}/wg/1.png", server.uri());
        save_image(&url, dest.to_str().unwrap(), &Client::new())
            .await
            .unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), content);
        assert!(!dir.path().join("1.png.part").exists());
    }

    #[tokio::test]
    async fn it_restarts_when_the_server_ignores_the_range() {
        let content: Vec<u8> = (0..100u8).collect();
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(content.clone()))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("1.png");
        std::fs::write(dir.path().join("1.png.part"), b"stale bytes").unwrap();
        let url = format!("{}/wg/1.png", server.uri());
        save_image(&url, dest.to_str().unwrap(), &Client::new())
            .await
            .unwrap();
        // The file is the fresh body, not the stale bytes with it appended
        assert_eq!(std::fs::read(&dest).unwrap(), content);
    }

    #[tokio::test]
    async fn it_restarts_on_a_mismatched_content_range() {
        let content: Vec<u8> = (0..100u8).collect();
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(request_header("Range", "bytes=10-"))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("Content-Range", "bytes 0-99/100")
                    .set_body_bytes(content.clone()),
            )
            .expect(1)
            .with_priority(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(content.clone()))
            .expect(1)
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("1.png");
        std::fs::write(dir.path().join("1.png.part"), &content[..10]).unwrap();
        let url = format!("{}/wg/1.png", server.uri());
        save_image(&url, dest.to_str().unwrap(), &Client::new())
            .await
            .unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), content);
        assert!(!dir.path().join("1.png.part").exists());
    }

    #[tokio::test]
    async fn it_fails_when_the_content_range_never_matches() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("Content-Range", "bytes 5-99/100")
                    .set_body_bytes(vec![0; 95]),
            )
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("1.png");
        let url = format!("{}/wg/1.png", server.uri());
        let err = save_image(&url, dest.to_str().unwrap(), &Client::new())
            .await
            .unwrap_err();
        assert!(matches!(err, DownloadError::Resume(_)), "{err:?}");
        assert!(!dest.exists());
    }

    #[tokio::test]
    async fn it_restarts_when_the_saved_range_is_not_satisfiable() {
        let content: Vec<u8> = (0..100u8).collect();
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(request_header("Range", "bytes=10-"))
            .respond_with(ResponseTemplate::new(416))
            .expect(1)
            .with_priority(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(content.clone()))
            .expect(1)
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("1.png");
        std::fs::write(dir.path().join("1.png.part"), &content[..10]).unwrap();
        let url = format!("{}/wg/1.png", server.uri());
        save_image(&url, dest.to_str().unwrap(), &Client::new())
            .await
            .unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), content);
    }

    #[tokio::test]
    async fn it_allows_only_one_writer_per_path() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(b"image".to_vec())
                    .set_delay(Duration::from_millis(50)),
            )
            .expect(1)
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("1.png");
        let url = format!("{}/wg/1.png", server.uri());
        let client = Client::new();
        let (first, second) = tokio::join!(
            save_image(&url, dest.to_str().unwrap(), &client),
            save_image(&url, dest.to_str().unwrap(), &client)
        );
        first.unwrap();
        second.unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"image");
    }

    #[tokio::test]
    async fn it_cleans_a_stale_part_when_the_file_exists() {
        let server = MockServer::start().await;
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("1.png");
        let part = dir.path().join("1.png.part");
        std::fs::write(&dest, b"done").unwrap();
        std::fs::write(&part, b"stale").unwrap();
        let url = format!("{}/wg/1.png", server.uri());
        save_image(&url, dest.to_str().unwrap(), &Client::new())
            .await
            .unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"done");
        assert!(!part.exists(), "stale .part left behind");
    }

    #[tokio::test]
    async fn it_publishes_over_a_stale_final_file() {
        // `rename` fails on an existing destination on Windows
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("1.png.part");
        let dest = dir.path().join("1.png");
        std::fs::write(&part, b"new").unwrap();
        std::fs::write(&dest, b"stale").unwrap();
        publish(part.to_str().unwrap(), dest.to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"new");
        assert!(!part.exists());
    }

    #[test]
    fn it_parses_content_range_starts() {
        assert_eq!(content_range_start("bytes 10-99/100"), Some(10));
        assert_eq!(content_range_start("bytes 0-99/*"), Some(0));
        assert_eq!(content_range_start("bytes 10-99/99"), None);
        assert_eq!(content_range_start("bytes 90-89/100"), None);
        assert_eq!(content_range_start("bytes 10-99"), None);
        assert_eq!(content_range_start("items 10-99/100"), None);
        assert_eq!(content_range_start("garbage"), None);
    }

    #[tokio::test]
    async fn it_retries_transient_failures() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(1)
            .with_priority(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"image".to_vec()))
            .expect(1)
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("1.png");
        let url = format!("{}/wg/1.png", server.uri());
        save_image(&url, dest.to_str().unwrap(), &Client::new())
            .await
            .unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"image");
    }

    #[tokio::test]
    async fn it_retries_stalled_responses() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(2)))
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("thread"))
            .mount(&server)
            .await;

        let client = Client::builder()
            .read_timeout(Duration::from_millis(200))
            .build()
            .unwrap();
        let url = format!("{}/wg/thread/1", server.uri());
        assert_eq!(get_page_content(&url, &client).await.unwrap(), "thread");
    }

    #[tokio::test]
    async fn it_gives_up_after_all_attempts() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .expect(u64::from(ATTEMPTS))
            .mount(&server)
            .await;

        let url = format!("{}/wg/thread/1", server.uri());
        let err = get_page_content(&url, &Client::new()).await.unwrap_err();
        assert_eq!(err.status(), Some(StatusCode::SERVICE_UNAVAILABLE));
    }

    #[tokio::test]
    async fn it_does_not_retry_client_errors() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(&server)
            .await;

        let url = format!("{}/wg/thread/1", server.uri());
        assert!(get_page_content(&url, &Client::new()).await.is_err());
    }

    #[tokio::test]
    async fn it_loads_page_only_if_modified() {
        const DATE: &str = "Mon, 26 Oct 2026 12:00:00 GMT";
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            // `header()` would split the date on its comma
            .and(|request: &Request| {
                request
                    .headers
                    .get("If-Modified-Since")
                    .is_some_and(|value| value == DATE)
            })
            .respond_with(ResponseTemplate::new(304))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Last-Modified", DATE)
                    .set_body_string("thread"),
            )
            .mount(&server)
            .await;

        let url = format!("{}/wg/thread/1.json", server.uri());
        let client = Client::new();
        assert_eq!(
            get_page_content_since(&url, &client, None).await.unwrap(),
            Page::Modified {
                content:       String::from("thread"),
                last_modified: Some(String::from(DATE)),
            }
        );
        assert_eq!(
            get_page_content_since(&url, &client, Some(DATE)).await.unwrap(),
            Page::NotModified
        );
    }

    #[test]
    fn it_builds_the_client() {
        assert!(client().is_ok());
    }

    // Tests against the live sites. Run with `cargo test -- --ignored`
    #[tokio::test]
    #[ignore = "hits the network"]
    async fn it_saves_live_4chan_image() {
        let client = Client::builder().user_agent("reqwest").build().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("1745612650141704.png");
        let url = "https://i.4cdn.org/g/1745612650141704.png";
        save_image(url, dest.to_str().unwrap(), &client).await.unwrap();
        assert!(dest.metadata().unwrap().len() > 0);
    }

    #[tokio::test]
    #[ignore = "hits the network"]
    async fn it_saves_live_4plebs_image() {
        let client = Client::builder().user_agent("reqwest").build().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("1614942709612.jpg");
        let url = "https://img.4plebs.org/boards/x/image/1614/94/1614942709612.jpg";
        save_image(url, dest.to_str().unwrap(), &client).await.unwrap();
        assert!(dest.metadata().unwrap().len() > 0);
    }
}
