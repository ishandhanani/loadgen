// SPDX-License-Identifier: Apache-2.0

//! Static S3 credentials read from a shell-style `export NAME=value` file.

use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};

/// Credentials and endpoint for the S3-compatible trace store.
#[derive(Debug, Clone)]
pub struct S3Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub endpoint_url: String,
    pub region: String,
}

/// Parse `[export ]NAME=value` lines. Values may be single- or double-quoted.
///
/// Required keys: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `S3_ENDPOINT_URL`.
/// Optional: `AWS_SESSION_TOKEN`, `AWS_REGION` or `AWS_DEFAULT_REGION` (default `us-east-1`).
pub fn load_credentials(path: &Path) -> Result<S3Credentials> {
    let text = fs::read_to_string(path).with_context(|| {
        format!(
            "failed to read S3 credentials file {} (pass --creds to point at the `export`-style env file)",
            path.display()
        )
    })?;
    let mut access_key_id = None;
    let mut secret_access_key = None;
    let mut session_token = None;
    let mut endpoint_url = None;
    let mut region = None;
    for line in text.lines() {
        let Some((name, value)) = parse_env_line(line) else {
            continue;
        };
        match name {
            "AWS_ACCESS_KEY_ID" => access_key_id = Some(value),
            "AWS_SECRET_ACCESS_KEY" => secret_access_key = Some(value),
            "AWS_SESSION_TOKEN" => session_token = Some(value),
            "S3_ENDPOINT_URL" => endpoint_url = Some(value),
            "AWS_REGION" => region = Some(value),
            "AWS_DEFAULT_REGION" if region.is_none() => region = Some(value),
            _ => {}
        }
    }
    let missing: Vec<&str> = [
        ("AWS_ACCESS_KEY_ID", access_key_id.is_none()),
        ("AWS_SECRET_ACCESS_KEY", secret_access_key.is_none()),
        ("S3_ENDPOINT_URL", endpoint_url.is_none()),
    ]
    .into_iter()
    .filter_map(|(name, missing)| missing.then_some(name))
    .collect();
    if !missing.is_empty() {
        bail!(
            "S3 credentials file {} is missing {}",
            path.display(),
            missing.join(", ")
        );
    }
    Ok(S3Credentials {
        access_key_id: access_key_id.expect("checked above"),
        secret_access_key: secret_access_key.expect("checked above"),
        session_token: session_token.filter(|token| !token.is_empty()),
        endpoint_url: endpoint_url.expect("checked above"),
        region: region.unwrap_or_else(|| "us-east-1".to_string()),
    })
}

/// Parse one `[export ]NAME=value` line; returns `None` for comments, blanks, and non-assignments.
pub fn parse_env_line(line: &str) -> Option<(&str, String)> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let line = line.strip_prefix("export ").map_or(line, str::trim_start);
    let (name, value) = line.split_once('=')?;
    let name = name.trim();
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return None;
    }
    let value = value.trim();
    let value = strip_quotes(value, '"')
        .or_else(|| strip_quotes(value, '\''))
        .unwrap_or(value);
    Some((name, value.to_string()))
}

fn strip_quotes(value: &str, quote: char) -> Option<&str> {
    value
        .strip_prefix(quote)
        .and_then(|rest| rest.strip_suffix(quote))
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn parses_export_lines_and_quotes() {
        assert_eq!(
            parse_env_line("export S3_ENDPOINT_URL=\"https://s3.example\""),
            Some(("S3_ENDPOINT_URL", "https://s3.example".to_string()))
        );
        assert_eq!(
            parse_env_line("AWS_ACCESS_KEY_ID='abc123'"),
            Some(("AWS_ACCESS_KEY_ID", "abc123".to_string()))
        );
        assert_eq!(parse_env_line("# comment"), None);
        assert_eq!(parse_env_line("lowercase=1"), None);
    }

    #[test]
    fn reports_missing_keys() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "export AWS_ACCESS_KEY_ID=a").unwrap();
        let error = load_credentials(file.path()).unwrap_err().to_string();
        assert!(error.contains("AWS_SECRET_ACCESS_KEY"));
        assert!(error.contains("S3_ENDPOINT_URL"));
    }

    #[test]
    fn loads_complete_file() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "export AWS_ACCESS_KEY_ID=a").unwrap();
        writeln!(file, "export AWS_SECRET_ACCESS_KEY=b").unwrap();
        writeln!(file, "export S3_ENDPOINT_URL=http://127.0.0.1:9000").unwrap();
        let creds = load_credentials(file.path()).unwrap();
        assert_eq!(creds.endpoint_url, "http://127.0.0.1:9000");
        assert_eq!(creds.region, "us-east-1");
        assert!(creds.session_token.is_none());
    }
}
