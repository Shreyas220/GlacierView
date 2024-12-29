use reqwest::{Client, Error as ReqwestError};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio;
use url::Url;

#[derive(Error, Debug)]
pub enum CatalogError {
    #[error("HTTP request failed: {0}")]
    RequestError(#[from] ReqwestError),
    #[error("Invalid catalog URL: {0}")]
    InvalidUrl(String),
    #[error("Table not found: {0}")]
    TableNotFound(String),
    #[error("No snapshots available")]
    NoSnapshots,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub snapshot_id: i64,
    pub timestamp_ms: i64,
    pub manifest_list: String,
    #[serde(default)]
    pub summary: SnapshotSummary,
    pub schema_id: i32,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SnapshotSummary {
    pub operation: Option<String>,
    pub added_files_count: Option<i32>,
    pub deleted_files_count: Option<i32>,
    pub total_records_count: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableMetadata {
    pub table_uuid: String,
    pub location: String,
    pub current_snapshot_id: Option<i64>,
    pub snapshots: Vec<Snapshot>,
}

#[derive(Debug, Clone)]
pub struct RestCatalog {
    base_url: Url,
    client: Client,
    warehouse_location: String,
}

impl RestCatalog {
    /// Creates a new REST catalog client
    pub fn new(base_url: &str, warehouse_location: &str) -> Result<Self, CatalogError> {
        let base_url = Url::parse(base_url).map_err(|_| CatalogError::InvalidUrl(base_url.to_string()))?;
        
        Ok(Self {
            base_url,
            client: Client::new(),
            warehouse_location: warehouse_location.to_string(),
        })
    }

    /// Gets the latest snapshot for a table
    pub async fn get_latest_snapshot(
        &self,
        namespace: &str,
        table_name: &str,
    ) -> Result<Snapshot, CatalogError> {
        // Build the REST API URL for table metadata
        let url = self.base_url
            .join(&format!("v1/namespaces/{}/tables/{}", namespace, table_name))
            .map_err(|_| CatalogError::InvalidUrl(format!("{}/{}", namespace, table_name)))?;

        // Fetch table metadata
        let response = self.client
            .get(url)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(CatalogError::TableNotFound(format!("{}.{}", namespace, table_name)));
        }

        let metadata: TableMetadata = response.json().await?;
        
        // Find the current snapshot
        if let Some(current_id) = metadata.current_snapshot_id {
            metadata.snapshots
                .into_iter()
                .find(|s| s.snapshot_id == current_id)
                .ok_or(CatalogError::NoSnapshots)
        } else {
            // If no current snapshot ID, return the latest by timestamp
            metadata.snapshots
                .into_iter()
                .max_by_key(|s| s.timestamp_ms)
                .ok_or(CatalogError::NoSnapshots)
        }
    }

    /// Gets a specific snapshot by ID
    pub async fn get_snapshot(
        &self,
        namespace: &str,
        table_name: &str,
        snapshot_id: i64,
    ) -> Result<Snapshot, CatalogError> {
        let url = self.base_url
            .join(&format!("v2/namespaces/{}/tables/{}/snapshots/{}", 
                namespace, table_name, snapshot_id))
            .map_err(|_| CatalogError::InvalidUrl(format!("{}/{}", namespace, table_name)))?;

        let response = self.client
            .get(url)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(CatalogError::TableNotFound(format!("{}.{}", namespace, table_name)));
        }

        let snapshot: Snapshot = response.json().await?;
        Ok(snapshot)
    }

    /// Lists all snapshots for a table
    pub async fn list_snapshots(
        &self,
        namespace: &str,
        table_name: &str,
    ) -> Result<Vec<Snapshot>, CatalogError> {
        let url = self.base_url
            .join(&format!("v1/namespaces/{}/tables/{}/snapshots", 
                namespace, table_name))
            .map_err(|_| CatalogError::InvalidUrl(format!("{}/{}", namespace, table_name)))?;

        let response = self.client
            .get(url)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(CatalogError::TableNotFound(format!("{}.{}", namespace, table_name)));
        }

        let snapshots: Vec<Snapshot> = response.json().await?;
        Ok(snapshots)
    }
}

// Example usage
#[tokio::main]
async fn main() -> Result<(), CatalogError> {
    let catalog = RestCatalog::new(
        "http://localhost:8181",
        "s3://warehouse/"
    )?;

    // Get the latest snapshot
    let latest_snapshot = catalog
        .get_latest_snapshot("demo", "nyc.taxis")
        .await?;

    println!("Latest snapshot ID: {}", latest_snapshot.snapshot_id);
    println!("Manifest list location: {}", latest_snapshot.manifest_list);
    
    if let Some(added_files) = latest_snapshot.summary.added_files_count {
        println!("Added files: {}", added_files);
    }

    Ok(())
}