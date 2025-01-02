use anyhow::{anyhow, Ok, Result};
use serde::Deserialize;
use serde_avro_fast::object_container_file_encoding::Reader;
use std::f32::consts::E;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, Mutex};
use tokio::task;
use rusoto_s3::{GetObjectRequest, S3Client, S3};
use rusoto_core::Region;
use rusoto_credential::StaticProvider;
use rusoto_core::HttpClient;
use rayon::prelude::*;


/// Utility to read S3 data into a `Vec<u8>`.
async fn read_s3_file(s3_client: &S3Client, file_path: &str) -> Result<Vec<u8>> {
    let get_obj_req = GetObjectRequest {
        bucket: "lakehouse".to_string(),
        key: file_path.to_string(),
        ..Default::default()
    };
    let result = s3_client.get_object(get_obj_req).await?;
    let stream = result
        .body
        .ok_or_else(|| anyhow!("Missing body in the S3 response"))?;

    let mut buffer = Vec::new();
    let mut body_stream = stream.into_async_read();
    body_stream.read_to_end(&mut buffer).await?;
    Ok(buffer)
}

#[derive(Debug, Deserialize)]
pub struct ManifestListRecord {
    #[serde(rename = "manifest_path")]
    manifest_list: Option<String>,
}

impl ManifestListRecord {

    async fn process_manifest_list_new(
        s3_client: &S3Client,
        input_rx: Arc<Mutex<mpsc::Receiver<String>>>,
        output_tx: mpsc::Sender<String>,
    ) -> Result<()> {
        while let Some(file_path) = {
            let mut guard = input_rx.lock().await;
            guard.recv().await
        } {
            match read_s3_file(s3_client, &file_path).await {
                Result::Ok(data) => {
                    match Reader::from_slice(&data) {
                        Result::Ok(mut reader) => {
                            let records: Vec<Result<ManifestListRecord, _>> = reader.deserialize().collect();

                            let results: Vec<String> = records
                                .par_iter() // Convert the iterator to a parallel iterator
                                .filter_map(|record_result| {
                                    // Attempt to unwrap each record; skip if there's an error
                                    if let Result::Ok(rec) = record_result {
                                        // Clone the `manifest_list` if it exists
                                        rec.manifest_list.clone()
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                                // eprint!("num of results: {}", results.len());
                            for manifest_path in results {
                                let stripped_path = manifest_path.strip_prefix("s3://lakehouse/").unwrap_or(&manifest_path);
                                if output_tx.send(stripped_path.to_string()).await.is_err() {
                                    eprintln!("Output channel closed");
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Error reading Avro data: {:?}", e);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error reading file from S3: {:?}", e);
                }
            }
        }
        Ok(())
    }

    //not used now 
    async fn process_manifest_list(s3_client: &S3Client, manifest_list_path: &str) -> Result<Vec<String>> {
        
        let data = read_s3_file(s3_client, manifest_list_path).await?;
        let mut reader = Reader::from_slice(&data)?;
        let records: Vec<Result<ManifestListRecord,_>> = reader.deserialize().collect();
        
        let results: Vec<String> = records
        .par_iter()  // Convert the iterator to a parallel iterator
        .filter_map(|record_result| {
            // Attempt to unwrap each record; skip if there's an error
            if let Result::Ok(rec) = record_result {
                // Clone the `manifest_list` if it exists
                rec.manifest_list.clone()
            } else {
                None
            }
        })
        .collect();

         
        Ok(results)
        
    }

    /// Spawns N workers that read Avro *manifest-list paths* from `input_rx`
    /// and produce Avro *manifest-file paths* into `output_tx`.
    pub fn spawn_workers(
        concurrency: usize,
        input_rx: Arc<Mutex<mpsc::Receiver<String>>>,
        output_tx: mpsc::Sender<String>,
        s3_client: S3Client,
    ) {
        for _ in 0..concurrency {
            let input_rx = Arc::clone(&input_rx);
            let output_tx = output_tx.clone();
            let client = s3_client.clone();

            task::spawn(async move {
                if let Err(e) = Self::process_manifest_list_new(&client, input_rx, output_tx).await {
                    eprintln!("Error in worker: {:?}", e);
                }

            });
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ManifestFileRecord {
    snapshot_id: Option<i64>,
}

impl ManifestFileRecord {

    async fn process_manifest_file_new(
        s3_client: &S3Client,
        input_rx: Arc<Mutex<mpsc::Receiver<String>>>,
        output_tx: mpsc::Sender<String>,

    ) -> Result<()> {

        while let Some(file_path) = {
            let mut guard = input_rx.lock().await;
            guard.recv().await
        } {
            match read_s3_file(s3_client, &file_path).await {
                Result::Ok(data) => {
                    
                    match Reader::from_slice(&data) {
                        Result::Ok(mut reader) => {
                            let records: Vec<Result<ManifestFileRecord, _>> = reader.deserialize().collect();
                            let results: Vec<String> = records
                            .par_iter()  // Convert the iterator to a parallel iterator
                            .filter_map(|record_result| {
                                // Attempt to unwrap each record; skip if there's an error
                                if let Result::Ok(rec) = record_result {
                                    // // Clone the `manifest_list` if it exists
                                    rec.snapshot_id.map(|id| id.to_string())
                                    // Some("Yo".to_string())
                                } else {
                                    None
                                }
                            })
                            .collect();

                            for sid in results {
                                if output_tx.send(sid).await.is_err() {
                                    eprintln!("ManifestFileRecord worker: output channel closed");
                                    break;
                                }
                            }


                        }
                        Err(e) => {
                            eprintln!("Error reading Avro data: {:?}", e);
                        }
                    
                    }
                    
                }

                    
                    Err(e) => {
                        eprintln!("Error reading Avro data: {:?}", e);
                    }
            }

        }

        Ok(())
    }

    //not used now
    async fn process_manifest_file(s3_client: &S3Client, manifest_file_path: &str) -> Result<Vec<String>> {
        let data = read_s3_file(s3_client, manifest_file_path).await?;
        let mut reader = Reader::from_slice(&data)?;
        
        // let mut results: Vec<String> = Vec::new();
        // for record in reader.deserialize() {
        //         let rec: ManifestFileRecord = record?;
        //         if let Some(id) = rec.snapshot_id {
        //                 results.push(id.to_string());
        //             }
        //         }
        let records: Vec<Result<ManifestFileRecord, _>> = reader.deserialize().collect();
        let results: Vec<String> = records
        .par_iter()  // Convert the iterator to a parallel iterator
        .filter_map(|record_result| {
            // Attempt to unwrap each record; skip if there's an error
            if let Result::Ok(rec) = record_result {
                // rec.snapshot_id.map(|id| id.to_string())
                Some("Yo".to_string())
            } else {
                None
            }
        })
        .collect();

        Ok(results)
    }

    /// Spawns N workers that read Avro *manifest-file paths* from `input_rx`
    /// and produce final snapshot IDs (strings) into `output_tx`.
    pub fn spawn_workers(
        concurrency: usize,
        input_rx: Arc<Mutex<mpsc::Receiver<String>>>,
        output_tx: mpsc::Sender<String>,
        s3_client: S3Client,
    ) {
        for _ in 0..concurrency {
            let input_rx = Arc::clone(&input_rx);
            let output_tx = output_tx.clone();
            let client = s3_client.clone();

            task::spawn(async move {
                if let Err(e) = Self::process_manifest_file_new(&client, input_rx, output_tx).await {
                    eprintln!("Error in worker: {:?}", e);
                }
            });
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {

    // S3 client
    let minio_url = "http://localhost:9000"; // Replace with your MinIO URL
    let access_key = "admin";
    let secret_key = "password";

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

    // Channels

    //  1) Avro file paths for "manifest list" stage:
    let (manifest_list_tx, manifest_list_rx) = mpsc::channel::<String>(4000);

    //  2) Avro file paths for "manifest file" stage:
    let (manifest_file_tx, manifest_file_rx) = mpsc::channel::<String>(8000);

    //  3) Final results (snapshot IDs):
    let (final_tx, mut final_rx) = mpsc::channel::<String>(1000);

    let manifest_list_rx = Arc::new(Mutex::new(manifest_list_rx));
    let manifest_file_rx = Arc::new(Mutex::new(manifest_file_rx));

    // Spawn the worker pools
    let manifest_list_worker_count = 10;
    let manifest_file_worker_count = 20;

    // Each ManifestFileRecord worker reads Avro "manifest files" and pushes to manifest_file_tx
    ManifestListRecord::spawn_workers(
        manifest_list_worker_count,
        Arc::clone(&manifest_list_rx),
        manifest_file_tx,
        s3_client.clone(),
    );

    ManifestFileRecord::spawn_workers(
        manifest_file_worker_count,
        Arc::clone(&manifest_file_rx),
        final_tx,
        s3_client.clone(),
    );

    // Use Instant to measure total processing time
    let start_time = std::time::Instant::now();


    let repeats = 4000;
    let manifest_list_path = "/nyc/taxis_partitioned/metadata/snap-7400282505158315953-1-d5c3b31a-81ac-47a4-b493-e32960f392e4.avro";
    for _ in 0..repeats {
        if manifest_list_tx.send(manifest_list_path.to_string()).await.is_err() {
            eprintln!("main: manifest_list_tx receiver dropped");
            break;
        }
    }
    // Close the channel so workers eventually stop
    drop(manifest_list_tx);

    // Gather final results from the last channel (snapshot IDs)
    let mut total_snapshots = 0_usize;
    while let Some(snapshot_id) = final_rx.recv().await {
        // You could store, log, or process these snapshot IDs
        total_snapshots += 1;
        // e.g., println!("Final Snapshot: {}", snapshot_id);
    }

    let duration = start_time.elapsed();
    //for testing each manifest list file points to 2 manifest files
    //so for 4000 manifest list files we should read 4000*2 = 8000 manifest files
    println!(
        "collected {} data file path and read {} avro files in  {:?} .",
        total_snapshots, repeats*3 ,duration
    );

    Ok(())
}
