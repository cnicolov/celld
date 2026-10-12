// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// This is a Cargo build host, not the injected node filesystem.
#![allow(clippy::disallowed_methods)]

use base64::Engine as _;
use sha2::{Digest, Sha256};
use std::env;
use std::fs;
use std::io::Write as _;
use std::path::PathBuf;

fn main() {
    let mut cargo = std::io::stdout().lock();
    writeln!(cargo, "cargo:rerun-if-env-changed=CELLD_MONITOR_ASSETS").unwrap();
    let assets = env::var_os("CELLD_MONITOR_ASSETS")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap())
                .join("../../target/monitor-ui")
        });
    writeln!(cargo, "cargo:rerun-if-changed={}", assets.display()).unwrap();
    let mut generated = String::from("pub static ASSETS: &[Asset] = &[\n");
    let mut hashes = Vec::new();
    let built = assets.join("index.html").is_file();
    assert!(built || env::var_os("CARGO_FEATURE_MONITOR_UI").is_none(),
        "monitor-ui requires built assets: run trunk build --release --locked --config crates/monitor/Trunk.toml first");
    if built {
        let mut paths = fs::read_dir(&assets)
            .expect("read monitor assets")
            .map(|entry| entry.expect("read monitor asset").path())
            .filter(|path| path.is_file())
            .collect::<Vec<_>>();
        paths.sort();
        for path in paths {
            let name = path
                .file_name()
                .unwrap()
                .to_str()
                .expect("UTF-8 asset name");
            let content_type = match path.extension().and_then(|extension| extension.to_str()) {
                Some("html") => "text/html; charset=utf-8",
                Some("js") => "text/javascript; charset=utf-8",
                Some("css") => "text/css; charset=utf-8",
                Some("wasm") => "application/wasm",
                Some("svg") => "image/svg+xml",
                _ => continue,
            };
            writeln!(cargo, "cargo:rerun-if-changed={}", path.display()).unwrap();
            let absolute = path.canonicalize().expect("canonical monitor asset");
            generated.push_str(&format!("Asset {{ name: {name:?}, content_type: {content_type:?}, bytes: include_bytes!({:?}) }},\n", absolute.to_str().unwrap()));
            if name == "index.html" {
                let html = fs::read_to_string(&path).expect("UTF-8 monitor HTML");
                let mut remaining = html.as_str();
                while let Some(start) = remaining.find("<script") {
                    remaining = &remaining[start..];
                    let Some(header_end) = remaining.find('>') else {
                        break;
                    };
                    let header = &remaining[..header_end];
                    remaining = &remaining[header_end + 1..];
                    let Some(end) = remaining.find("</script>") else {
                        break;
                    };
                    if !header.contains("src=") {
                        let hash = base64::engine::general_purpose::STANDARD
                            .encode(Sha256::digest(&remaining.as_bytes()[..end]));
                        hashes.push(format!("'sha256-{hash}'"));
                    }
                    remaining = &remaining[end + "</script>".len()..];
                }
            }
        }
    }
    generated.push_str("];");
    let csp = format!("default-src 'self'; script-src 'self' 'wasm-unsafe-eval' {}; style-src 'self' 'unsafe-inline'; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'", hashes.join(" "));
    generated.push_str(&format!(
        "\npub const CONTENT_SECURITY_POLICY: &str = {csp:?};\npub const BUILT: bool = {built};\n"
    ));
    let output = PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("monitor_assets.rs");
    fs::write(output, generated).expect("write embedded monitor assets");
}
