use std::path::Path;

use wee_events::{AggregateId, AggregateType};

use crate::Error;

use super::{
    NamedPartition, SqliteLocalPartitionLayout, SqliteLocalPartitionStrategy,
    SqlitePartitionNamingStrategy, SqlitePartitionRead, SqlitePartitionStrategy,
    SqliteSqldNamespacedPartitionStrategy,
};

#[derive(Clone)]
pub struct PartitionByStrategy<F> {
    partitioner: F,
}

impl<F> PartitionByStrategy<F> {
    pub fn new(partitioner: F) -> Self {
        Self { partitioner }
    }
}

impl<F> std::fmt::Debug for PartitionByStrategy<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PartitionByStrategy(..)")
    }
}

impl<F> SqlitePartitionStrategy for PartitionByStrategy<F>
where
    F: Fn(&AggregateId) -> String + Clone + Send + Sync + 'static,
{
    type Partition = NamedPartition<String>;

    fn partition_for_aggregate(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Self::Partition, Error> {
        let name = (self.partitioner)(aggregate_id);
        Ok(NamedPartition::new(name.clone(), name))
    }

    fn read_plan(&self, _partition: &Self::Partition) -> SqlitePartitionRead {
        SqlitePartitionRead::ScanAll
    }

    fn read_plan_by_type(
        &self,
        _partition: &Self::Partition,
        aggregate_type: &AggregateType,
    ) -> SqlitePartitionRead {
        SqlitePartitionRead::ScanType(aggregate_type.clone())
    }
}

impl<F> SqlitePartitionNamingStrategy for PartitionByStrategy<F>
where
    F: Fn(&AggregateId) -> String + Clone + Send + Sync + 'static,
{
    fn partition_name<'a>(&self, partition: &'a Self::Partition) -> Option<&'a str> {
        Some(partition.name())
    }

    fn partition_from_name(&self, name: &str) -> Result<Self::Partition, Error> {
        Ok(NamedPartition::new(name.to_string(), name.to_string()))
    }
}

impl<F> SqliteLocalPartitionStrategy for PartitionByStrategy<F>
where
    F: Fn(&AggregateId) -> String + Clone + Send + Sync + 'static,
{
    fn initialize_root(&self, root: &Path) -> Result<(), Error> {
        std::fs::create_dir_all(root)?;
        Ok(())
    }

    fn local_partition_layout(&self) -> SqliteLocalPartitionLayout {
        SqliteLocalPartitionLayout::NamedDatabases
    }
}

impl<F> SqliteSqldNamespacedPartitionStrategy for PartitionByStrategy<F> where
    F: Fn(&AggregateId) -> String + Clone + Send + Sync + 'static
{
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partition_by_user(aggregate_id: &AggregateId) -> String {
        aggregate_id
            .aggregate_key
            .split(':')
            .next()
            .expect("split always yields at least one segment")
            .to_string()
    }

    #[test]
    fn partition_by_strategy_routes_with_partitioner() {
        let strategy = PartitionByStrategy::new(partition_by_user);
        let order = AggregateId::new("order", "kevin:123");
        let payment = AggregateId::new("payment", "kevin:123");

        assert_eq!(
            strategy
                .partition_for_aggregate(&order)
                .expect("routing should succeed"),
            NamedPartition::new("kevin", "kevin".to_string())
        );
        assert_eq!(
            strategy
                .partition_for_aggregate(&payment)
                .expect("routing should succeed"),
            NamedPartition::new("kevin", "kevin".to_string())
        );
    }

    #[test]
    fn partition_from_name_restores_named_partition() {
        let strategy = PartitionByStrategy::new(partition_by_user);
        let partition = strategy
            .partition_from_name("kevin/team:1")
            .expect("partition restore should succeed");

        assert_eq!(
            partition,
            NamedPartition::new("kevin/team:1", "kevin/team:1".to_string())
        );
    }
}
