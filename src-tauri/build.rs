fn main() {
  let dest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
    .join("bundle-resources")
    .join("job-pipeline");
  if std::fs::create_dir_all(&dest).is_err() {
    println!("cargo:warning=could not create {}", dest.display());
  }
  println!("cargo:rerun-if-changed=../app.py");
  println!("cargo:rerun-if-changed=../config.py");
  tauri_build::build()
}
