// SPDX-License-Identifier: Apache-2.0

//! Trace shard discovery and streaming from the request-trace object store.
//!
//! The store layout is
//! `request-trace/v1/env=prd/cluster=<C>/model=<M>/date=YYYY-MM-DD/hour=HH/trace-*.jsonl.gz`.
//! The cluster is not known up front, so the `env=prd/` prefix is listed once
//! for `cluster=` partitions and every cluster is probed for each shard hour.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use anyhow::{Context, Result, bail};
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncRead;

use crate::creds::S3Credentials;
use crate::hours::ShardHour;

/// Bucket holding the request traces.
pub const BUCKET: &str = "dynamo-request-traces";
/// Prefix under which `cluster=` partitions live.
pub const BASE_PREFIX: &str = "request-trace/v1/env=prd/";
/// Concurrent listing calls while resolving shards for one bundle.
const LIST_CONCURRENCY: usize = 8;

/// One object in the shard store, with the listing metadata used for cache validation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ShardRef {
    pub key: String,
    pub etag: String,
    pub size: u64,
}

pub type ShardReader = Pin<Box<dyn AsyncRead + Send>>;

/// Minimal object-store surface needed to find and stream shards.
pub trait ShardSource: Send + Sync {
    /// Immediate child "directories" under `prefix` (each returned value ends with `/`).
    fn list_prefixes<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<Vec<String>>>;

    /// All objects under `prefix`.
    fn list_objects<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<Vec<ShardRef>>>;

    /// Stream one object's compressed bytes.
    fn open<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<ShardReader>>;
}

/// Resolve every `.jsonl.gz` shard for `model` across all clusters and the given hours.
///
/// The result is sorted by key and free of duplicates so it can be compared
/// against the shard list a previous job recorded.
pub async fn list_model_shards(
    source: &dyn ShardSource,
    model: &str,
    hours: &[ShardHour],
) -> Result<Vec<ShardRef>> {
    let clusters: Vec<String> = source
        .list_prefixes(BASE_PREFIX)
        .await
        .context("failed to list cluster partitions")?
        .into_iter()
        .filter(|prefix| {
            prefix
                .trim_end_matches('/')
                .rsplit('/')
                .next()
                .is_some_and(|segment| segment.starts_with("cluster="))
        })
        .collect();
    if clusters.is_empty() {
        bail!("no cluster= partitions found under {BASE_PREFIX}");
    }
    let prefixes: Vec<String> = clusters
        .iter()
        .flat_map(|cluster| {
            hours.iter().map(move |hour| {
                format!(
                    "{cluster}model={model}/date={}/hour={}/",
                    hour.date(),
                    hour.hour()
                )
            })
        })
        .collect();
    let listings: Vec<_> = prefixes
        .iter()
        .map(|prefix| async move {
            source
                .list_objects(prefix)
                .await
                .with_context(|| format!("failed to list shards under {prefix}"))
        })
        .collect();
    let mut listings = futures_util::stream::iter(listings).buffer_unordered(LIST_CONCURRENCY);
    let mut shards = BTreeMap::new();
    while let Some(listing) = listings.next().await {
        for shard in listing? {
            if shard.key.ends_with(".jsonl.gz") {
                shards.insert(shard.key.clone(), shard);
            }
        }
    }
    Ok(shards.into_values().collect())
}

/// S3-compatible store reached through a fixed endpoint with path-style addressing.
pub struct S3ShardSource {
    client: aws_sdk_s3::Client,
    bucket: String,
}

impl S3ShardSource {
    pub fn new(creds: &S3Credentials, bucket: &str) -> Self {
        let credentials = Credentials::new(
            creds.access_key_id.clone(),
            creds.secret_access_key.clone(),
            creds.session_token.clone(),
            None,
            "dynamo-traces-env-file",
        );
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(creds.region.clone()))
            .endpoint_url(creds.endpoint_url.clone())
            .credentials_provider(credentials)
            .force_path_style(true)
            .build();
        Self {
            client: aws_sdk_s3::Client::from_conf(config),
            bucket: bucket.to_string(),
        }
    }
}

impl ShardSource for S3ShardSource {
    fn list_prefixes<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<Vec<String>>> {
        Box::pin(async move {
            let mut pages = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix)
                .delimiter("/")
                .into_paginator()
                .send();
            let mut prefixes = Vec::new();
            while let Some(page) = pages.next().await {
                let page = page.map_err(|error| {
                    anyhow::anyhow!(
                        "ListObjectsV2 {prefix} failed: {}",
                        display_sdk_error(&error)
                    )
                })?;
                for common in page.common_prefixes() {
                    if let Some(prefix) = common.prefix() {
                        prefixes.push(prefix.to_string());
                    }
                }
            }
            Ok(prefixes)
        })
    }

    fn list_objects<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<Vec<ShardRef>>> {
        Box::pin(async move {
            let mut pages = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix)
                .into_paginator()
                .send();
            let mut objects = Vec::new();
            while let Some(page) = pages.next().await {
                let page = page.map_err(|error| {
                    anyhow::anyhow!(
                        "ListObjectsV2 {prefix} failed: {}",
                        display_sdk_error(&error)
                    )
                })?;
                for object in page.contents() {
                    let Some(key) = object.key() else {
                        continue;
                    };
                    objects.push(ShardRef {
                        key: key.to_string(),
                        etag: object.e_tag().unwrap_or_default().to_string(),
                        size: u64::try_from(object.size().unwrap_or_default()).unwrap_or(0),
                    });
                }
            }
            Ok(objects)
        })
    }

    fn open<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<ShardReader>> {
        Box::pin(async move {
            let object = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
                .map_err(|error| {
                    anyhow::anyhow!("GetObject {key} failed: {}", display_sdk_error(&error))
                })?;
            Ok(Box::pin(object.body.into_async_read()) as ShardReader)
        })
    }
}

fn display_sdk_error<E: std::error::Error + 'static, R: std::fmt::Debug>(
    error: &aws_sdk_s3::error::SdkError<E, R>,
) -> String {
    aws_sdk_s3::error::DisplayErrorContext(error).to_string()
}

/// Directory-backed store used by tests and offline validation. Keys map to
/// relative paths below `root`; ETags are the SHA-256 of the file content.
pub struct LocalShardSource {
    root: PathBuf,
}

impl LocalShardSource {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn resolve(&self, key: &str) -> PathBuf {
        let mut path = self.root.clone();
        for segment in key.split('/').filter(|segment| !segment.is_empty()) {
            path.push(segment);
        }
        path
    }

    fn collect_objects(&self, dir: &Path, prefix: &str, out: &mut Vec<ShardRef>) -> Result<()> {
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(error).with_context(|| format!("failed to read {}", dir.display()));
            }
        };
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let key = format!("{prefix}{name}");
            if entry.file_type()?.is_dir() {
                self.collect_objects(&entry.path(), &format!("{key}/"), out)?;
            } else {
                let bytes = std::fs::read(entry.path())?;
                out.push(ShardRef {
                    key,
                    etag: hex::encode(Sha256::digest(&bytes)),
                    size: bytes.len() as u64,
                });
            }
        }
        Ok(())
    }
}

impl ShardSource for LocalShardSource {
    fn list_prefixes<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<Vec<String>>> {
        Box::pin(async move {
            let dir = self.resolve(prefix);
            let entries = match std::fs::read_dir(&dir) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(Vec::new());
                }
                Err(error) => {
                    return Err(error).with_context(|| format!("failed to read {}", dir.display()));
                }
            };
            let mut prefixes = Vec::new();
            for entry in entries {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    prefixes.push(format!("{prefix}{}/", entry.file_name().to_string_lossy()));
                }
            }
            prefixes.sort();
            Ok(prefixes)
        })
    }

    fn list_objects<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<Vec<ShardRef>>> {
        Box::pin(async move {
            let mut objects = Vec::new();
            self.collect_objects(&self.resolve(prefix), prefix, &mut objects)?;
            objects.sort();
            Ok(objects)
        })
    }

    fn open<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<ShardReader>> {
        Box::pin(async move {
            let path = self.resolve(key);
            let file = tokio::fs::File::open(&path)
                .await
                .with_context(|| format!("failed to open shard {}", path.display()))?;
            Ok(Box::pin(file) as ShardReader)
        })
    }
}
