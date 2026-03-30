use std::path::Path;

use wee_events::{AggregateId, AggregateType};

use crate::Error;

use super::{
    NamedPartition, SqliteLocalPartitionLayout, SqliteLocalPartitionStrategy,
    SqlitePartitionNamingStrategy, SqlitePartitionRead, SqlitePartitionStrategy,
    SqliteSqldNamespacedPartitionStrategy,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AggregateStrategy;

pub type AggregatePartition = NamedPartition<AggregateId>;

impl SqlitePartitionStrategy for AggregateStrategy {
    type Partition = AggregatePartition;

    fn partition_for_aggregate(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Self::Partition, Error> {
        Ok(AggregatePartition::new(
            aggregate_id.to_string(),
            aggregate_id.clone(),
        ))
    }

    fn read_plan(&self, partition: &Self::Partition) -> SqlitePartitionRead {
        SqlitePartitionRead::Direct(partition.key().clone())
    }

    fn read_plan_by_type(
        &self,
        partition: &Self::Partition,
        aggregate_type: &AggregateType,
    ) -> SqlitePartitionRead {
        if &partition.key().aggregate_type == aggregate_type {
            SqlitePartitionRead::Direct(partition.key().clone())
        } else {
            SqlitePartitionRead::Skip
        }
    }
}

impl SqlitePartitionNamingStrategy for AggregateStrategy {
    fn partition_name<'a>(&self, partition: &'a Self::Partition) -> Option<&'a str> {
        Some(partition.name())
    }

    fn partition_from_name(&self, name: &str) -> Result<Self::Partition, Error> {
        let aggregate_id = name.parse::<AggregateId>().map_err(|error| {
            Error::Configuration(format!(
                "invalid aggregate partition name '{name}': {error}"
            ))
        })?;
        Ok(AggregatePartition::new(
            aggregate_id.to_string(),
            aggregate_id,
        ))
    }
}

impl SqliteLocalPartitionStrategy for AggregateStrategy {
    fn initialize_root(&self, root: &Path) -> Result<(), Error> {
        std::fs::create_dir_all(root)?;
        Ok(())
    }

    fn local_partition_layout(&self) -> SqliteLocalPartitionLayout {
        SqliteLocalPartitionLayout::NamedDatabases
    }
}

impl SqliteSqldNamespacedPartitionStrategy for AggregateStrategy {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_name_matches_aggregate_id() {
        let aggregate_id = AggregateId::new("campaign/run", "urn:uuid:abc/123");
        let partition = AggregateStrategy
            .partition_for_aggregate(&aggregate_id)
            .expect("routing should succeed");

        assert_eq!(partition.name(), "campaign/run:urn:uuid:abc/123");
        assert_eq!(
            partition.key(),
            &AggregateId::new("campaign/run", "urn:uuid:abc/123")
        );
    }

    #[test]
    fn partition_from_name_restores_aggregate_partition() {
        let partition = AggregateStrategy
            .partition_from_name("campaign/run:urn:uuid:abc/123")
            .expect("partition restore should succeed");

        assert_eq!(
            partition,
            AggregatePartition::new(
                "campaign/run:urn:uuid:abc/123",
                AggregateId::new("campaign/run", "urn:uuid:abc/123"),
            )
        );
    }
}
