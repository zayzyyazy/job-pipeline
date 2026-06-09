#[cfg(any(
  target_os = "macos",
  target_os = "windows",
  target_os = "linux"
))]
use serde_json::json;
#[cfg(any(
  target_os = "macos",
  target_os = "windows",
  target_os = "linux"
))]
use std::io::{self, ErrorKind, Read, Write};
#[cfg(any(
  target_os = "macos",
  target_os = "windows",
  target_os = "linux"
))]
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
#[cfg(any(
  target_os = "macos",
  target_os = "windows",
  target_os = "linux"
))]
use std::path::{Path, PathBuf};
#[cfg(any(
  target_os = "macos",
  target_os = "windows",
  target_os = "linux"
))]
use std::process::{Child, Command, Stdio};
#[cfg(any(
  target_os = "macos",
  target_os = "windows",
  target_os = "linux"
))]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(any(
  target_os = "macos",
  target_os = "windows",
  target_os = "linux"
))]
use std::sync::{Arc, Mutex};
#[cfg(any(
  target_os = "macos",
  target_os = "windows",
  target_os = "linux"
))]
use std::{thread, time::Duration};
#[cfg(any(
  target_os = "macos",
  target_os = "windows",
  target_os = "linux"
))]
use tauri::{Emitter, Listener, Manager, RunEvent, Url};

/// Emitted after GET /health succeeds (Rust poller thread).
#[cfg(any(
  target_os = "macos",
  target_os = "windows",
  target_os = "linux"
))]
const EV_BACKEND_READY: &str = "job_pipeline_backend_ready";
#[cfg(any(
  target_os = "macos",
  target_os = "windows",
  target_os = "linux"
))]
const EV_BACKEND_FAIL: &str = "job_pipeline_backend_failed";

#[cfg(any(
  target_os = "macos",
  target_os = "windows",
  target_os = "linux"
))]
#[derive(Default)]
pub struct BackendLifecycle {
  child: Mutex<Option<Child>>,
  spawned_here: AtomicBool,
}

#[cfg(any(
  target_os = "macos",
  target_os = "windows",
  target_os = "linux"
))]
impl BackendLifecycle {
  fn record_spawn(&self, child: Child) {
    let mut g = match self.child.lock() {
      Ok(v) => v,
      Err(p) => p.into_inner(),
    };
    let _ = g.replace(child);
    self.spawned_here.store(true, Ordering::SeqCst);
  }

  /// Stop the Python subprocess only when this app instance started it.
  fn shutdown_managed(&self) {
    if !self.spawned_here.load(Ordering::SeqCst) {
      return;
    }
    let mut guard = match self.child.lock() {
      Ok(g) => g,
      Err(poisoned) => poisoned.into_inner(),
    };
    if let Some(mut ch) = guard.take() {
      let _ = ch.kill();
      let _ = ch.wait();
    }
    self.spawned_here.store(false, Ordering::SeqCst);
  }

}

#[cfg(any(
  target_os = "macos",
  target_os = "windows",
  target_os = "linux"
))]
fn flask_port() -> u16 {
  fn parse_u16(raw: Result<String, std::env::VarError>) -> Option<u16> {
    raw.ok()
      .and_then(|s| s.trim().parse().ok())
      .filter(|&p| p > 0)
  }
  parse_u16(std::env::var("FLASK_RUN_PORT"))
    .or_else(|| parse_u16(std::env::var("PORT")))
    .unwrap_or(5000)
}

#[cfg(any(
  target_os = "macos",
  target_os = "windows",
  target_os = "linux"
))]
fn flask_app_base_url(port: u16) -> Url {
  Url::parse(&format!("http://127.0.0.1:{port}/")).expect("hard-coded url parses")
}

/// Cheap GET /health check without extra crates.
#[cfg(any(
  target_os = "macos",
  target_os = "windows",
  target_os = "linux"
))]
fn health_returns_ok(port: u16) -> bool {
  let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
  let mut stream =
    match TcpStream::connect_timeout(&addr, Duration::from_millis(350)) {
      Ok(s) => s,
      Err(_) => return false,
    };
  let _ = stream.set_read_timeout(Some(Duration::from_millis(700)));
  if stream
    .write_all(b"GET /health HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
    .is_err()
  {
    return false;
  }
  let mut buf = [0u8; 1024];
  let Ok(n) = stream.read(&mut buf) else {
    return false;
  };
  let head = String::from_utf8_lossy(&buf[..n]);
  if !head.contains("200 ") {
    return false;
  }
  head.contains("\"ok\":true")
    || head.contains("\"ok\": true")
    || head.contains("\"ok\":  true") // tolerant
}

#[cfg(any(
  target_os = "macos",
  target_os = "windows",
  target_os = "linux"
))]
fn backend_autostart_allowed() -> bool {
  matches!(std::env::var("TAURI_SKIP_BACKEND"), Ok(ref v) if v == "1") == false
}

#[cfg(any(
  target_os = "macos",
  target_os = "windows",
  target_os = "linux"
))]
fn python_command_for(backend_root: &Path) -> io::Result<Command> {
  let app_py = backend_root.join("app.py");
  if !app_py.is_file() {
    return Err(io::Error::new(
      ErrorKind::NotFound,
      format!("missing app.py at {}", backend_root.display()),
    ));
  }

  #[cfg(windows)]
  {
    let wv_py = backend_root.join(".venv").join("Scripts").join("python.exe");
    if wv_py.is_file() {
      let mut cmd = Command::new(wv_py);
      cmd.arg(&app_py);
      return Ok(cmd);
    }
    let mut cmd = Command::new("python");
    cmd.arg(&app_py);
    return Ok(cmd);
  }

  #[cfg(not(windows))]
  {
    for rel in ["bin/python3", "bin/python"] {
      let p = backend_root.join(".venv").join(rel);
      if p.is_file() {
        let mut cmd = Command::new(&p);
        cmd.arg(&app_py);
        return Ok(cmd);
      }
    }
    let mut cmd = Command::new("python3");
    cmd.arg(&app_py);
    Ok(cmd)
  }
}

#[cfg(any(
  target_os = "macos",
  target_os = "windows",
  target_os = "linux"
))]
fn embedded_writable_data_dir() -> Option<PathBuf> {
  #[cfg(target_os = "macos")]
  {
    let home = std::env::var_os("HOME")?;
    return Some(PathBuf::from(home).join("Library/Application Support/Job Pipeline"));
  }
  #[cfg(target_os = "windows")]
  {
    let ad = std::env::var_os("APPDATA")?;
    return Some(PathBuf::from(ad).join("Job Pipeline"));
  }
  #[cfg(all(not(target_os = "macos"), not(target_os = "windows")))]
  {
    let home = std::env::var_os("HOME")?;
    let h = PathBuf::from(home);
    let base = std::env::var_os("XDG_DATA_HOME")
      .map(PathBuf::from)
      .unwrap_or_else(|| h.join(".local/share"));
    return Some(base.join("job-pipeline"));
  }
}

fn spawn_embedded_flask(backend_root: &Path) -> io::Result<Child> {
  let mut cmd = python_command_for(backend_root)?;
  cmd.env("JOB_PIPELINE_EMBEDDED", "1");
  cmd.env(
    "FLASK_RUN_PORT",
    flask_port().to_string(),
  );
  if let Some(dir) = embedded_writable_data_dir() {
    let _ = std::fs::create_dir_all(&dir);
    cmd.env("JOB_PIPELINE_DATA_DIR", dir.as_os_str());
  }
  cmd.current_dir(backend_root);
  cmd.stdin(Stdio::null());
  #[cfg(debug_assertions)]
  {
    cmd.stdout(Stdio::inherit()).stderr(Stdio::inherit());
  }
  #[cfg(not(debug_assertions))]
  {
    cmd.stdout(Stdio::null()).stderr(Stdio::null());
  }
  cmd.spawn()
}

#[cfg(any(
  target_os = "macos",
  target_os = "windows",
  target_os = "linux"
))]
fn resolve_backend_root<R: tauri::Runtime>(handle: &tauri::AppHandle<R>) -> Option<PathBuf> {
  fn has_bundle(p: &Path) -> bool {
    p.join("app.py").is_file()
  }

  /// Tauri copies `bundle-resources/job-pipeline` under `Contents/Resources/` with the
  /// `bundle-resources/` prefix preserved — not as `Resources/job-pipeline/` alone.
  fn bundled_candidates(resources_root: &Path) -> [PathBuf; 2] {
    [
      resources_root.join("bundle-resources").join("job-pipeline"),
      resources_root.join("job-pipeline"),
    ]
  }

  if let Ok(custom) = std::env::var("JOB_PIPELINE_ROOT") {
    let p = PathBuf::from(custom);
    if has_bundle(&p) {
      return Some(p);
    }
  }

  #[cfg(debug_assertions)]
  {
    if let Some(dev) = Path::new(env!("CARGO_MANIFEST_DIR")).parent() {
      if has_bundle(dev) {
        return Some(dev.to_path_buf());
      }
    }
  }

  if let Ok(res) = handle.path().resource_dir() {
    for b in bundled_candidates(&res) {
      if has_bundle(&b) {
        return Some(b);
      }
    }
  }

  // Fallback: locate .../Contents/Resources next to this binary (macOS .app).
  if let Ok(exe) = std::env::current_exe() {
    if let Some(contents_macos) = exe.parent() {
      if let Some(contents) = contents_macos.parent() {
        let res = contents.join("Resources");
        for b in bundled_candidates(&res) {
          if has_bundle(&b) {
            return Some(b);
          }
        }
      }
    }
  }

  None
}

#[cfg(any(
  target_os = "macos",
  target_os = "windows",
  target_os = "linux"
))]
fn schedule_splash_flask_port_inject<R: tauri::Runtime>(handle: tauri::AppHandle<R>) {
  let port = flask_port();
  let h = handle.clone();
  thread::spawn(move || {
    let js = format!(
      "window.__JOB_PIPELINE_FLASK_PORT__={port};try{{window.dispatchEvent(new CustomEvent('job_pipeline_flask_port'));}}catch(_e){{}}"
    );
    for _ in 0..100usize {
      if let Some(w) = h.get_webview_window("main") {
        let _ = w.eval(&js);
        return;
      }
      thread::sleep(Duration::from_millis(40));
    }
  });
}

fn register_backend_startup<R: tauri::Runtime>(
  app: &mut tauri::App<R>,
  life: Arc<BackendLifecycle>,
) {
  let h_ready = app.handle().clone();
  app.listen(EV_BACKEND_READY, move |_e| {
    let h = h_ready.clone();
    thread::spawn(move || {
      let port = flask_port();
      let url = flask_app_base_url(port);
      for _ in 0..50usize {
        if let Some(win) = h.get_webview_window("main") {
          match win.navigate(url.clone()) {
            Ok(()) => return,
            Err(e) => {
              let _ = h.emit(
                EV_BACKEND_FAIL,
                json!({ "detail": format!("Navigate failed: {e}") }),
              );
              return;
            }
          }
        }
        thread::sleep(Duration::from_millis(100));
      }
      let _ = h.emit(
        EV_BACKEND_FAIL,
        json!({"detail": "Main window not ready; cannot open dashboard."}),
      );
    });
  });

  let h_fail = app.handle().clone();
  app.listen(EV_BACKEND_FAIL, move |ev| {
    let detail_msg = serde_json::from_str::<serde_json::Value>(ev.payload())
      .ok()
      .and_then(|v| {
        v.get("detail").map(|x| match x {
          serde_json::Value::String(s) => s.clone(),
          other => serde_json::to_string(other).unwrap_or_else(|_| "Backend error".into()),
        })
      })
      .unwrap_or_else(|| "Backend failed to start.".to_string());

    if let Some(win) = h_fail.get_webview_window("main") {
      let arg = serde_json::to_string(&detail_msg).unwrap_or_else(|_| "\"Error\"".to_string());
      let script = format!(
        "window.__JOB_PIPELINE_BACKEND_ERROR__&&window.__JOB_PIPELINE_BACKEND_ERROR__({arg})"
      );
      let _ = win.eval(script);
    }
  });

  let hk = app.handle().clone();
  let lifecycle = Arc::clone(&life);
  thread::spawn(move || backend_warm_loop(hk, lifecycle));
}

#[cfg(any(
  target_os = "macos",
  target_os = "windows",
  target_os = "linux"
))]
fn backend_warm_loop<R: tauri::Runtime>(hk: tauri::AppHandle<R>, lifecycle: Arc<BackendLifecycle>) {
  let port = flask_port();

  // Already listening (e.g. manual ./scripts/run_flask.sh — do not take ownership).
  if health_returns_ok(port) {
    let _ = hk.emit(EV_BACKEND_READY, ());
    return;
  }

  // Debug-only: Flask was not up and spawning is explicitly disabled.
  if !backend_autostart_allowed() {
    let tail = concat!(
      "Flask was not reachable and TAURI_SKIP_BACKEND=1 disables auto-start. ",
      "Start the server manually (./scripts/run_flask.sh)."
    );
    let _ = hk.emit(EV_BACKEND_FAIL, json!({ "detail": tail }));
    return;
  }

  let Some(backend_root) = resolve_backend_root(&hk) else {
    let hint = concat!(
      "Could not locate the Flask project (no app.py).\n\n",
      "Rebuild after syncing Python into the bundle:\n",
      "./scripts/sync_py_bundle_for_tauri.sh\n\n",
      "Or set JOB_PIPELINE_ROOT to your project folder (contains app.py).",
    )
    .to_string();
    let _ = hk.emit(EV_BACKEND_FAIL, json!({ "detail": hint }));
    return;
  };

  let child = match spawn_embedded_flask(&backend_root) {
    Ok(c) => c,
    Err(e) => {
      let _ =
        hk.emit(EV_BACKEND_FAIL, json!({ "detail": format!("Failed to start Python backend: {e}")}));
      return;
    }
  };
  lifecycle.record_spawn(child);

  for _ in 0..120usize {
    if health_returns_ok(port) {
      let _ = hk.emit(EV_BACKEND_READY, ());
      return;
    }
    thread::sleep(Duration::from_millis(450));
  }

  lifecycle.shutdown_managed();
  let detail = concat!(
    "Timed out waiting for http://127.0.0.1:PORT/health. ",
    "Check that Python deps are installed (.venv recommended) and port is free.",
  )
  .replace("PORT", &port.to_string());
  let _ = hk.emit(
    EV_BACKEND_FAIL,
    json!({ "detail": detail}),
  );
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
  tauri::Builder::default()
    .setup(|app| {
      #[cfg(debug_assertions)]
      app.handle().plugin(
        tauri_plugin_log::Builder::default()
          .level(log::LevelFilter::Info)
          .build(),
      )?;
      app.handle().plugin(tauri_plugin_opener::init())?;
      app.handle().plugin(tauri_plugin_clipboard_manager::init())?;

      #[cfg(any(
        target_os = "macos",
        target_os = "windows",
        target_os = "linux"
      ))]
      {
        let lifecycle = Arc::new(BackendLifecycle::default());
        app.manage(Arc::clone(&lifecycle));
        schedule_splash_flask_port_inject(app.handle().clone());
        register_backend_startup(app, lifecycle);
      }

      Ok(())
    })
    .build(tauri::generate_context!())
    .expect("error while building Job Pipeline Tauri app")
    .run(|handle, event| {
      #[cfg(any(
        target_os = "macos",
        target_os = "windows",
        target_os = "linux"
      ))]
      if matches!(event, RunEvent::Exit) {
        if let Some(life) = handle.try_state::<Arc<BackendLifecycle>>() {
          life.shutdown_managed();
        }
      }
    });
}
