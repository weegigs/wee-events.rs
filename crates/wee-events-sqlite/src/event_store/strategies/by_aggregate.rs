use std::path::Path;

use wee_events::{AggregateId, AggregateType};

use crate::Error;

use super::{
    LocalPartitionLayout, LocalPartitionStrategy, NamedPartition, PartitionNamingStrategy,
    PartitionRead, PartitionStrategy, SqldNamespacedPartitionStrategy,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AggregateStrategy;

pub type AggregatePartition = NamedPartition<AggregateId>;

impl PartitionStrategy for AggregateStrategy {
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

    fn read_plan(&self, partition: &Self::Partition) -> PartitionRead {
        PartitionRead::Direct(partition.key().clone())
    }

    fn read_plan_by_type(
        &self,
        partition: &Self::Partition,
        aggregate_type: &AggregateType,
    ) -> PartitionRead {
        if &partition.key().aggregate_type == aggregate_type {
            PartitionRead::Direct(partition.key().clone())
        } else {
            PartitionRead::Skip
        }
    }
}

impl PartitionNamingStrategy for AggregateStrategy {
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

impl LocalPartitionStrategy for AggregateStrategy {
    fn initialize_root(&self, root: &Path) -> Result<(), Error> {
        std::fs::create_dir_all(root)?;
        Ok(())
    }

    fn local_partition_layout(&self) -> LocalPartitionLayout {
        LocalPartitionLayout::NamedDatabases
    }
}

impl SqldNamespacedPartitionStrategy for AggregateStrategy {}

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
