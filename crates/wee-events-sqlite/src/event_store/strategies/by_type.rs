use std::path::Path;

use wee_events::{AggregateId, AggregateType};

use crate::Error;

use super::{
    NamedPartition, SqliteLocalPartitionLayout, SqliteLocalPartitionStrategy,
    SqlitePartitionNamingStrategy, SqlitePartitionRead, SqlitePartitionStrategy,
    SqliteSqldNamespacedPartitionStrategy,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TypeStrategy;

pub type TypePartition = NamedPartition<AggregateType>;

impl SqlitePartitionStrategy for TypeStrategy {
    type Partition = TypePartition;

    fn partition_for_aggregate(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Self::Partition, Error> {
        let aggregate_type = aggregate_id.aggregate_type.clone();
        Ok(TypePartition::new(
            aggregate_type.as_str().to_string(),
            aggregate_type,
        ))
    }

    fn read_plan(&self, partition: &Self::Partition) -> SqlitePartitionRead {
        SqlitePartitionRead::ScanType(partition.key().clone())
    }

    fn read_plan_by_type(
        &self,
        partition: &Self::Partition,
        aggregate_type: &AggregateType,
    ) -> SqlitePartitionRead {
        if partition.key() == aggregate_type {
            SqlitePartitionRead::ScanType(aggregate_type.clone())
        } else {
            SqlitePartitionRead::Skip
        }
    }
}

impl SqlitePartitionNamingStrategy for TypeStrategy {
    fn partition_name<'a>(&self, partition: &'a Self::Partition) -> Option<&'a str> {
        Some(partition.name())
    }

    fn partition_from_name(&self, name: &str) -> Result<Self::Partition, Error> {
        let aggregate_type = AggregateType::new(name);
        Ok(TypePartition::new(
            aggregate_type.as_str().to_string(),
            aggregate_type,
        ))
    }
}

impl SqliteLocalPartitionStrategy for TypeStrategy {
    fn initialize_root(&self, root: &Path) -> Result<(), Error> {
        std::fs::create_dir_all(root)?;
        Ok(())
    }

    fn local_partition_layout(&self) -> SqliteLocalPartitionLayout {
        SqliteLocalPartitionLayout::NamedDatabases
    }
}

impl SqliteSqldNamespacedPartitionStrategy for TypeStrategy {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_name_matches_aggregate_type() {
        let partition = TypeStrategy
            .partition_for_aggregate(&AggregateId::new("a/b:c", "123"))
            .expect("routing should succeed");

        assert_eq!(partition.name(), "a/b:c");
        assert_eq!(partition.key(), &AggregateType::new("a/b:c"));
    }

    #[test]
    fn partition_from_name_restores_type_partition() {
        let partition = TypeStrategy
            .partition_from_name("a/b:c")
            .expect("partition restore should succeed");

        assert_eq!(
            partition,
            TypePartition::new("a/b:c", AggregateType::new("a/b:c"))
        );
    }
}
