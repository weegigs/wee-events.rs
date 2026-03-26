use std::path::{Path, PathBuf};

use async_trait::async_trait;
use wee_events::{AggregateId, AggregateType};

use crate::Error;

use super::super::partitioning::SqlitePartitionCatalog;
use super::super::types::{
    SqliteDatabaseTarget, SqlitePartition, SqlitePartitioning, SqlitePartitioningStrategy,
};

#[derive(Debug)]
pub(crate) struct LocalPartitionCatalog {
    root: PathBuf,
    strategy: SqlitePartitioningStrategy,
}

impl LocalPartitionCatalog {
    pub(crate) fn new(root: PathBuf, partitioning: SqlitePartitioning) -> Result<Self, Error> {
        let strategy = partitioning.resolve(SqlitePartitioningStrategy::Aggregate);
        if !matches!(strategy, SqlitePartitioningStrategy::Global) {
            std::fs::create_dir_all(&root)?;
        }

        Ok(Self { root, strategy })
    }

    fn path_for_partition(&self, partition: &SqlitePartition) -> Result<PathBuf, Error> {
        match (self.strategy, partition) {
            (SqlitePartitioningStrategy::Global, SqlitePartition::Single) => Ok(self.root.clone()),
            (SqlitePartitioningStrategy::Type, SqlitePartition::AggregateType(aggregate_type)) => {
                Ok(self.root.join(format!("{}.db", aggregate_type.as_str())))
            }
            (SqlitePartitioningStrategy::Aggregate, SqlitePartition::Aggregate(aggregate_id)) => {
                Ok(self
                    .root
                    .join(aggregate_id.aggregate_type.as_str())
                    .join(format!("{}.db", aggregate_id.aggregate_key)))
            }
            _ => Err(Error::Configuration(format!(
                "partition {partition:?} does not match local partitioning strategy {:?}",
                self.strategy
            ))),
        }
    }

    fn db_file_stem(path: &Path) -> Option<String> {
        (path.extension().is_some_and(|extension| extension == "db"))
            .then(|| path.file_stem())
            .flatten()
            .map(|stem| stem.to_string_lossy().into_owned())
    }
}

#[async_trait]
impl SqlitePartitionCatalog for LocalPartitionCatalog {
    fn partition_for_aggregate(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<SqlitePartition, Error> {
        Ok(match self.strategy {
            SqlitePartitioningStrategy::Global => SqlitePartition::Single,
            SqlitePartitioningStrategy::Type => {
                SqlitePartition::AggregateType(aggregate_id.aggregate_type.clone())
            }
            SqlitePartitioningStrategy::Aggregate => {
                SqlitePartition::Aggregate(aggregate_id.clone())
            }
        })
    }

    async fn ensure_target_for_partition(
        &self,
        partition: &SqlitePartition,
    ) -> Result<SqliteDatabaseTarget, Error> {
        let path = self.path_for_partition(partition)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        Ok(SqliteDatabaseTarget::Local(path))
    }

    async fn target_for_existing_partition(
        &self,
        partition: &SqlitePartition,
    ) -> Result<Option<SqliteDatabaseTarget>, Error> {
        let path = self.path_for_partition(partition)?;
        Ok(path.exists().then_some(SqliteDatabaseTarget::Local(path)))
    }

    async fn partitions(&self) -> Result<Vec<SqlitePartition>, Error> {
        match self.strategy {
            SqlitePartitioningStrategy::Global => Ok(vec![SqlitePartition::Single]),
            SqlitePartitioningStrategy::Type => {
                let mut partitions = Vec::new();
                for entry in std::fs::read_dir(&self.root)? {
                    let entry = entry?;
                    let path = entry.path();
                    let Some(aggregate_type) = Self::db_file_stem(&path) else {
                        continue;
                    };
                    partitions.push(SqlitePartition::AggregateType(AggregateType::new(
                        aggregate_type,
                    )));
                }
                Ok(partitions)
            }
            SqlitePartitioningStrategy::Aggregate => {
                let mut partitions = Vec::new();
                for entry in std::fs::read_dir(&self.root)? {
                    let entry = entry?;
                    let aggregate_type = entry.file_name().to_string_lossy().into_owned();
                    if !entry.file_type()?.is_dir() {
                        continue;
                    }

                    for child in std::fs::read_dir(entry.path())? {
                        let child = child?;
                        let child_path = child.path();
                        let Some(aggregate_key) = Self::db_file_stem(&child_path) else {
                            continue;
                        };
                        partitions.push(SqlitePartition::Aggregate(AggregateId::new(
                            aggregate_type.clone(),
                            aggregate_key,
                        )));
                    }
                }
                Ok(partitions)
            }
        }
    }
}
