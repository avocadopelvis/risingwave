// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::catalog::TableId;
use risingwave_common::util::sort_util::OrderPair;

use super::*;
use crate::executor::AppendOnlyTopNExecutor;

pub struct AppendOnlyTopNExecutorBuilder;

impl ExecutorBuilder for AppendOnlyTopNExecutorBuilder {
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &StreamNode,
        store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> Result<BoxedExecutor> {
        let node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::AppendOnlyTopN)?;
        let [input]: [_; 1] = params.input.try_into().unwrap();

        let order_pairs: Vec<_> = node
            .get_column_orders()
            .iter()
            .map(OrderPair::from_prost)
            .collect();
        let limit = if node.limit == 0 {
            None
        } else {
            Some(node.limit as usize)
        };
        let cache_size = Some(1024);
        let total_count = (0, 0);
        // TODO: refactor to one state table likes the non-append-only version
        let table_id_l = TableId::new(node.table_id_l);
        let table_id_h = TableId::new(node.table_id_h);
        let key_indices = node
            .get_distribution_key()
            .iter()
            .map(|key| *key as usize)
            .collect::<Vec<_>>();

        Ok(AppendOnlyTopNExecutor::new(
            input,
            order_pairs,
            (node.offset as usize, limit),
            params.pk_indices,
            store,
            table_id_l,
            table_id_h,
            cache_size,
            total_count,
            params.executor_id,
            key_indices,
        )?
        .boxed())
    }
}
