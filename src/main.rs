use minio::s3::types::NotificationRecords;
use rusoto_core::{HttpClient, Region};
use rusoto_credential::StaticProvider;
use rusoto_s3::{GetObjectRequest, PutObjectRequest, S3, S3Client};
use minio::s3::creds::StaticProvider as MinioStaticProvider;
use minio::s3::client::ClientBuilder;
use minio::s3::http::BaseUrl;
use tokio::sync::Mutex;
use std::sync::Arc;
use std::collections::HashMap;
use opendal::{Operator, Result};


use std::io::Cursor;
use log::{error, info};
use serde::Deserialize;
use apache_avro::{from_value, Reader};

#[derive(Debug, Deserialize)]
struct IcebergTableMetadata {
    #[serde(rename = "current-snapshot-id")]
    current_snapshot_id: Option<u64>,
    snapshots: Option<Vec<IcebergSnapshot>>,
}

#[derive(Debug, Deserialize)]
struct IcebergSnapshot {
    #[serde(rename = "snapshot-id")]
    snapshot_id: u64,
    #[serde(rename = "manifest-list")]
    manifest_list: Option<String>,
    // There are more fields in the snapshot, but we only show what we need.
}

pub async fn read_s3_file(s3_config: HashMap<String, String>, file_path: &str) -> Result<bytes::Bytes> {
    let mut builder = Operator::new("s3");

    if let Some(endpoint) = s3_config.get("endpoint") {
        builder = builder.endpoint(endpoint);
    }
    if let Some(access_key_id) = s3_config.get("access_key_id") {
        builder = builder.access_key_id(access_key_id);
    }
    if let Some(secret_access_key) = s3_config.get("secret_access_key") {
        builder = builder.secret_access_key(secret_access_key);
    }
    if let Some(region) = s3_config.get("region") {
        builder = builder.region(region);
    }

    let operator: Operator = builder.build()?;

    let bytes = operator.read(file_path).await?;

    Ok(bytes)
}

/// Helper function: Download an object from S3/MinIO as bytes
async fn download_s3_object(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
) -> Result<Vec<u8>> {
    let get_req = GetObjectRequest {
        bucket: bucket.to_string(),
        key: key.to_string(),
        ..Default::default()
    };

    let result = s3_client
        .get_object(get_req)
        .await
        .map_err(|e| anyhow::anyhow!("S3 get_object error: {}", e))?;
    
    let stream = result.body.ok_or_else(|| anyhow::anyhow!("Missing body"))?;
    
        
}

/// Helper function: Reads and parses the JSON metadata
async fn parse_iceberg_metadata(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
) -> Result<IcebergTableMetadata> {
    let metadata_bytes = download_s3_object(s3_client, bucket, key).await?;
    let metadata: IcebergTableMetadata = serde_json::from_slice(&metadata_bytes)?;
    Ok(metadata)
}

/// Helper function: Parse a snapshot Avro file to get a list of manifest files
async fn parse_snapshot_avro(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
) -> Result<Vec<String>> {
    let avro_bytes = download_s3_object(s3_client, bucket, key).await?;
    let reader = Reader::new(Cursor::new(avro_bytes))?;

    let mut manifest_paths = vec![];
    for value in reader {
        let record = value?;
        // The schema is dynamic, but typically you'll see fields like "manifest_path"
        // The example output for each record was something like:
        // Record([
        //   ("manifest_path", String("s3://warehouse/...")),
        //   ("content", Int(0)),
        //   ...
        // ])
        //
        // We can decode it into an Avro "GenericRecord" or try to map it into a custom struct.
        // For simplicity, let's do a partial parse using Avro's Value interface:

        if let apache_avro::types::Value::Record(fields) = record {
            for (field_name, field_value) in fields {
                if field_name == "manifest_path" {
                    if let apache_avro::types::Value::String(manifest_path) = field_value {
                        manifest_paths.push(manifest_path);
                    }
                }
            }
        }
    }
    Ok(manifest_paths)
}

/// Helper function: Parse a manifest Avro file to get data file paths
async fn parse_manifest_avro(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
) -> Result<Vec<(String, i32)>> {
    let avro_bytes = download_s3_object(s3_client, bucket, key).await?;
    let reader = Reader::new(Cursor::new(avro_bytes))?;

    let mut file_list = vec![];

    // The structure of the manifest Avro is different from the snapshot Avro. 
    // Typically, each record in a manifest Avro corresponds to a data file or a delete file.
    // The schema might look something like this for each record:
    //
    // Record([
    //   ("file_path", String("s3://...")),
    //   ("content", Int(0)), // or 1
    //   ...
    // ])
    //
    // But in practice, the schema is more complex. 
    // We'll do a simplified parse of the "file_path" and "content" fields.

    for value in reader {
        let record = value?;
        if let apache_avro::types::Value::Record(fields) = record {
            let mut filepath_opt: Option<String> = None;
            let mut content_opt: Option<i32> = None;
            for (field_name, field_value) in fields {
                match field_name.as_str() {
                    "file_path" => {
                        if let apache_avro::types::Value::String(fp) = field_value {
                            filepath_opt = Some(fp);
                        }
                    },
                    "content" => {
                        if let apache_avro::types::Value::Int(content_type) = field_value {
                            content_opt = Some(content_type);
                        }
                    },
                    _ => {}
                }
            }
            if let (Some(fp), Some(c)) = (filepath_opt, content_opt) {
                file_list.push((fp, c));
            }
        }
    }

    Ok(file_list)
}

fn update_checkpoint_filename(_s: &mut Arc<Mutex<String>>, _new_value: String) {
    // let mut locked_user = s.lock().unwrap();
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logger so `info!`, `error!` macros work
    env_logger::init();

    let minio_url = "http://localhost:9000"; // Replace with your MinIO URL
    let access_key = "admin";
    let secret_key = "password";
    let bucket_name = "warehouse";

    // Step 1: Set up S3 client
    let region = Region::Custom {
        name: "us-east-1".to_owned(),
        endpoint: minio_url.to_owned(),
    };

    let credentials = StaticProvider::new_minimal(access_key.to_owned(), secret_key.to_owned());
    let s3_client = S3Client::new_with(
        HttpClient::new().expect("failed to create HTTP client"),
        credentials,
        region,
    );

    let checkpoint = Arc::new(Mutex::new(String::from("initial") ));

    // Example: we spawn a task that listens for *-metadata.json (or however you want to detect that).
    // You already have a similar bucket notification listener in your code. 
    // We'll just demonstrate the logic after we detect a file.
    let listener_handle = {
        tokio::spawn(async move {
            info!("Starting bucket notification listener for *-metadata.json files...");

            // We set up the Minio client for notifications:
            let base_url = "http://localhost:9000"
                .parse::<BaseUrl>()
                .expect("Unable to parse base url");
            let miniocred = MinioStaticProvider::new(access_key, secret_key, None);
            let client = ClientBuilder::new(base_url)
                .provider(Some(Box::new(miniocred)))
                .build()
                .expect("Failed to create MinIO client");

            // For demonstration, let's reuse your code: we'll listen for ObjectCreated events
            // with suffix: "-metadata.json"
            let events = vec!["s3:ObjectCreated:*"];
            let args = minio::s3::args::ListenBucketNotificationArgs {
                extra_headers: None,
                extra_query_params: None,
                region: None,
                bucket: bucket_name,
                prefix: Some(""), // Listen to all objects in the bucket
                suffix: Some("metadata.json"), // Listen specifically for objects ending in -metadata.json
                events: Some(events.clone()),
                event_fn: &|records: NotificationRecords| {

                    let s3_client_clone = s3_client.clone();
                    let local_bucket = bucket_name.to_string();
                    let shared_string_task = Arc::clone(&checkpoint);

                    tokio::spawn(async move {
                        for record in records.records {

                            if let Some(s3_data) = record.s3 {
                            
                                if let Some(object) = s3_data.object {
                            
                                    if let Some(key) = object.key {
                                        info!("Detected new metadata JSON: {}", key);

                                        // Let’s parse the metadata JSON
                                        match parse_iceberg_metadata(&s3_client_clone, &local_bucket, &key).await {
                                            Ok(table_metadata) => {
                                                info!("Parsed metadata JSON: {:#?}", table_metadata);
                                                if let Some(current_snapshot_id) = table_metadata.current_snapshot_id {
                                                    info!("current-snapshot-id: {}", current_snapshot_id);

                                                    // Next, find the snapshot that matches current_snapshot_id through the snapshots list
                                                    if let Some(snapshots) = table_metadata.snapshots {
                                                        // Find the snapshot with that ID
                                                        if let Some(snapshot) = snapshots.iter()
                                                            .find(|snap| snap.snapshot_id == current_snapshot_id)
                                                        {
                                                            // That snapshot has a "manifest-list" Avro
                                                            if let Some(manifest_list_path) = &snapshot.manifest_list {
                                                                // The file location might be an S3 URI: 
                                                                //   "s3://warehouse/nyc/taxis_partitioned/metadata/snap-5432601199165646943-1-f83b8d73-90f0-426a-86bc-35db1f9dcfef.avro"
                                                                // but in your local MinIO scenario, 
                                                                // you might need to parse out the actual bucket/key from that URI. 
                                                                // For simplicity, let's assume it's all in the same "my-bucket" with a path.
                                                                // If you literally have an s3://warehouse... path, you'd need to parse that
                                                                // to figure out the bucket and key. We'll do a naive approach:

                                                                if let Some(key_in_bucket) = manifest_list_path.strip_prefix("s3://warehouse/") {
                                                                    match parse_snapshot_avro(
                                                                        &s3_client_clone,
                                                                        &local_bucket,
                                                                        key_in_bucket
                                                                    ).await {
                                                                        Ok(manifest_files) => {
                                                                            info!("Manifest files from snapshot Avro: {:#?}", manifest_files);

                                                                            // For each manifest Avro, parse it to get actual data files
                                                                            for manifest_avro_uri in manifest_files {
                                                                                // again, parse out bucket/key
                                                                                if let Some(key_in_bucket) = manifest_avro_uri.strip_prefix("s3://warehouse/") {
                                                                                    match parse_manifest_avro(
                                                                                        &s3_client_clone,
                                                                                        &local_bucket,
                                                                                        key_in_bucket
                                                                                    ).await {
                                                                                        Ok(file_paths) => {
                                                                                            for (fp, content_type) in file_paths {
                                                                                                info!("Found data file path: {}, content: {}", fp, content_type);
                                                                                            }
                                                                                        },
                                                                                        Err(e) => error!("Failed to parse manifest avro: {:?}", e),
                                                                                    }
                                                                                } else {
                                                                                    error!("Manifest Avro URI is not recognized: {}", manifest_avro_uri);
                                                                                }
                                                                            }
                                                                        },
                                                                        Err(e) => error!("Failed to parse snapshot Avro: {:?}", e),
                                                                    }
                                                                } else {
                                                                    error!("Manifest list path is not recognized or not in my-bucket: {}", manifest_list_path);
                                                                }
                                                            } else {
                                                                error!("No manifest-list found in snapshot!");
                                                            }
                                                        } else {
                                                            error!("Could not find snapshot with ID {}", current_snapshot_id);
                                                        }
                                                    } else {
                                                        error!("No snapshots found in metadata!");
                                                    }
                                                } else {
                                                    error!("No current-snapshot-id in metadata JSON!");
                                                }
                                            }
                                            Err(e) => {
                                                error!("Error parsing metadata JSON: {:?}", e);
                                            }
                                        }

                                        // Just as an example, update the checkpoint Arc
                                        let mut locked_str = shared_string_task.lock().await;
                                        *locked_str = key;
                                    }
                                }
                            }
                        }
                    });
                    true
                },
            };

            if let Err(e) = client.listen_bucket_notification(&args).await {
                error!("Failed to listen to bucket notifications: {}", e);
            }

            info!("Listening for *-metadata.json notifications on bucket: {}", bucket_name);
        })
    };

    // For demonstration, if you still have a parallel upload job, you can keep that code here...
    // (omitted for brevity)

    // Wait for the listener to finish (which it likely won't, unless there's an error).
    tokio::select! {
        _ = listener_handle => {
            info!("Listener task finished.");
        }
    }

    Ok(())
}
