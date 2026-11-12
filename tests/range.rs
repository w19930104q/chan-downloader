//! Regression tests for resumable downloads against a local Range fixture:
//! interruption across processes, cancellation, partial writes and servers
//! that don't support `Range`.

use std::{
    env,
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    path::Path,
    process::{Command, Stdio},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use chan_downloader::save_image;
use reqwest::Client;

/// How the fixture answers GET requests
#[derive(Clone)]
enum Mode {
    /// 200 with the full body, `Range` ignored
    Full,
    /// 206 with the requested tail when `Range` is sent, 200 otherwise
    Range,
    /// The first request announces the full body but closes after `after`
    /// bytes, the next ones 404: the transfer fails with partial bytes on disk
    TruncateThenGone(usize),
    /// Like `Range`, but the body trickles byte by byte
    Trickle(Duration),
}

/// Local HTTP server serving one file, recording the `Range` header of each
/// request
struct Fixture {
    port:     u16,
    requests: Arc<Mutex<Vec<Option<String>>>>,
    mode:     Arc<Mutex<Mode>>,
}

impl Fixture {
    fn start(content: Vec<u8>, mode: Mode) -> Fixture {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let fixture = Fixture {
            port,
            requests: Arc::new(Mutex::new(Vec::new())),
            mode: Arc::new(Mutex::new(mode)),
        };
        let (requests, mode) = (fixture.requests.clone(), fixture.mode.clone());
        thread::spawn(move || {
            for stream in listener.incoming() {
                let (requests, mode, content) = (requests.clone(), mode.clone(), content.clone());
                thread::spawn(move || handle(stream.unwrap(), &content, &mode, &requests));
            }
        });
        fixture
    }

    fn url(&self) -> String {
        format!("http://127.0.0.1:{}/wg/1.png", self.port)
    }

    fn set_mode(&self, mode: Mode) {
        *self.mode.lock().unwrap() = mode;
    }

    /// `Range` headers of the requests received so far
    fn ranges(&self) -> Vec<Option<String>> {
        self.requests.lock().unwrap().clone()
    }
}

fn handle(
    mut stream: TcpStream,
    content: &[u8],
    mode: &Mutex<Mode>,
    requests: &Mutex<Vec<Option<String>>>,
) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut range = None;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            return;
        }
        let line = line.trim();
        if line.is_empty() {
            break;
        }
        if let Some(value) = line.to_ascii_lowercase().strip_prefix("range:") {
            range = Some(value.trim().to_owned());
        }
    }
    requests.lock().unwrap().push(range.clone());
    let offset = range
        .as_deref()
        .and_then(|range| range.strip_prefix("bytes="))
        .and_then(|range| range.strip_suffix('-'))
        .and_then(|start| start.parse::<usize>().ok())
        .unwrap_or(0);
    let total = content.len();
    let mode = mode.lock().unwrap().clone();
    let trickle = match mode {
        Mode::Trickle(delay) => Some(delay),
        _ => None,
    };
    let (head, body): (String, &[u8]) = match mode {
        Mode::TruncateThenGone(after) if requests.lock().unwrap().len() == 1 => (
            format!("HTTP/1.1 200 OK\r\nContent-Length: {total}\r\nConnection: close\r\n\r\n"),
            &content[..after],
        ),
        Mode::TruncateThenGone(_) => (
            String::from("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"),
            &[],
        ),
        Mode::Range | Mode::Trickle(_) if range.is_some() && offset < total => (
            format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes \
                 {offset}-{last}/{total}\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n",
                last = total - 1,
                length = total - offset,
            ),
            &content[offset..],
        ),
        _ => (
            format!("HTTP/1.1 200 OK\r\nContent-Length: {total}\r\nConnection: close\r\n\r\n"),
            content,
        ),
    };
    if stream.write_all(head.as_bytes()).is_err() {
        return;
    }
    match trickle {
        Some(delay) => {
            for byte in body {
                if stream.write_all(&[*byte]).is_err() {
                    return; // the client went away (killed, cancelled)
                }
                let _ = stream.flush();
                thread::sleep(delay);
            }
        },
        None => {
            let _ = stream.write_all(body);
        },
    }
}

/// Waits until the `.part` file holds at least `at_least` bytes
async fn wait_for_part(part: &Path, at_least: u64) {
    for _ in 0..200 {
        if part.metadata().map_or(0, |meta| meta.len()) >= at_least {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("{} didn't reach {at_least} bytes", part.display());
}

/// Worker run in a child process by `it_resumes_after_an_interrupted_run`:
/// downloads until killed. Does nothing in a normal test run.
#[tokio::test]
async fn download_worker() {
    let (Ok(url), Ok(dest)) = (env::var("RANGE_FIXTURE_URL"), env::var("RANGE_FIXTURE_DEST")) else {
        return;
    };
    let client = chan_downloader::client().unwrap();
    let _ = save_image(&url, &dest, &client).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn it_resumes_after_an_interrupted_run() {
    let content: Vec<u8> = (0..100u8).map(|i| i.wrapping_mul(37)).collect();
    let fixture = Fixture::start(content.clone(), Mode::Trickle(Duration::from_millis(25)));
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("1.png");
    let part = dir.path().join("1.png.part");

    // A first process downloads until it is killed mid-transfer
    let mut child = Command::new(env::current_exe().unwrap())
        .args(["--exact", "download_worker", "--nocapture"])
        .env("RANGE_FIXTURE_URL", fixture.url())
        .env("RANGE_FIXTURE_DEST", &dest)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_for_part(&part, 5).await;
    child.kill().unwrap();
    child.wait().unwrap();
    let saved = part.metadata().unwrap().len();
    assert!((5..100).contains(&saved), "interrupted at {saved} bytes");
    assert!(!dest.exists());

    // A rerun resumes from the saved bytes
    save_image(&fixture.url(), dest.to_str().unwrap(), &Client::new())
        .await
        .unwrap();
    assert_eq!(std::fs::read(&dest).unwrap(), content);
    assert!(!part.exists());
    let want = format!("bytes={saved}-");
    assert!(
        fixture
            .ranges()
            .iter()
            .any(|range| range.as_deref() == Some(want.as_str())),
        "no resume from {saved} in {:?}",
        fixture.ranges()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn it_keeps_progress_when_cancelled() {
    let content: Vec<u8> = (0..100u8).map(|i| i.wrapping_mul(13)).collect();
    let fixture = Fixture::start(content.clone(), Mode::Trickle(Duration::from_millis(25)));
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("1.png");
    let part = dir.path().join("1.png.part");
    let url = fixture.url();

    let task = tokio::spawn({
        let (url, dest) = (url.clone(), dest.clone());
        async move { save_image(&url, dest.to_str().unwrap(), &Client::new()).await }
    });
    wait_for_part(&part, 5).await;
    task.abort();
    let _ = task.await;
    let saved = part.metadata().unwrap().len();
    assert!((5..100).contains(&saved), "cancelled at {saved} bytes");
    assert!(!dest.exists());

    save_image(&url, dest.to_str().unwrap(), &Client::new())
        .await
        .unwrap();
    assert_eq!(std::fs::read(&dest).unwrap(), content);
    let want = format!("bytes={saved}-");
    assert!(
        fixture
            .ranges()
            .iter()
            .any(|range| range.as_deref() == Some(want.as_str())),
        "no resume from {saved} in {:?}",
        fixture.ranges()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn it_keeps_and_resumes_a_partial_write() {
    let content: Vec<u8> = (0..100u8).map(|i| i.wrapping_mul(7)).collect();
    let fixture = Fixture::start(content.clone(), Mode::TruncateThenGone(10));
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("1.png");
    let part = dir.path().join("1.png.part");
    let url = fixture.url();

    // The transfer fails after 10 bytes: they stay on disk, no final file
    assert!(
        save_image(&url, dest.to_str().unwrap(), &Client::new())
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&part).unwrap(), &content[..10]);
    assert!(!dest.exists());

    // The next run resumes from them
    fixture.set_mode(Mode::Range);
    save_image(&url, dest.to_str().unwrap(), &Client::new())
        .await
        .unwrap();
    assert_eq!(std::fs::read(&dest).unwrap(), content);
    assert!(!part.exists());
    assert!(
        fixture
            .ranges()
            .iter()
            .any(|range| range.as_deref() == Some("bytes=10-")),
        "no resume from 10 in {:?}",
        fixture.ranges()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn it_restarts_safely_when_the_server_ignores_range() {
    let content: Vec<u8> = (0..50u8).collect();
    let fixture = Fixture::start(content.clone(), Mode::Full);
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("1.png");
    std::fs::write(dir.path().join("1.png.part"), b"stale!").unwrap();

    save_image(&fixture.url(), dest.to_str().unwrap(), &Client::new())
        .await
        .unwrap();
    // The file is the fresh body, not the stale bytes with it appended
    assert_eq!(std::fs::read(&dest).unwrap(), content);
    assert_eq!(fixture.ranges(), vec![Some(String::from("bytes=6-"))]);
}
