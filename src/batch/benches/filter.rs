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

use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use futures::StreamExt;
use risingwave_batch::executor::test_utils::{gen_data, MockExecutor};
use risingwave_batch::executor::{BoxedExecutor, FilterExecutor};
use risingwave_common::catalog::schema_test_utils::field_n;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_expr::expr::build_from_prost;
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::expr::expr_node::RexNode;
use risingwave_pb::expr::expr_node::Type::{
    ConstantValue as TConstValue, Equal, InputRef, Modulus,
};
use risingwave_pb::expr::{ConstantValue, ExprNode, FunctionCall, InputRefExpr};
use tikv_jemallocator::Jemalloc;
use tokio::runtime::Runtime;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn create_filter_executor(chunk_size: usize, chunk_num: usize) -> BoxedExecutor {
    let input_data = gen_data(chunk_size, chunk_num, &[DataType::Int64]);

    let mut mock_executor = MockExecutor::new(field_n::<1>(DataType::Int64));
    input_data.into_iter().for_each(|c| mock_executor.add(c));

    // Expression: $1 % 2 == 0
    let expr = {
        let input_ref1 = ExprNode {
            expr_type: InputRef as i32,
            return_type: Some(risingwave_pb::data::DataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::InputRef(InputRefExpr { column_idx: 0 })),
        };

        let literal2 = ExprNode {
            expr_type: TConstValue as i32,
            return_type: Some(risingwave_pb::data::DataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::Constant(ConstantValue {
                body: ScalarImpl::Int64(2).to_protobuf(),
            })),
        };

        // $1 % 2
        let mod2 = ExprNode {
            expr_type: Modulus as i32,
            return_type: Some(risingwave_pb::data::DataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![input_ref1, literal2],
            })),
        };

        let literal0 = ExprNode {
            expr_type: TConstValue as i32,
            return_type: Some(risingwave_pb::data::DataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::Constant(ConstantValue {
                body: ScalarImpl::Int64(0).to_protobuf(),
            })),
        };

        // $1 % 2 == 0
        ExprNode {
            expr_type: Equal as i32,
            return_type: Some(risingwave_pb::data::DataType {
                type_name: TypeName::Boolean as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![mod2, literal0],
            })),
        }
    };

    Box::new(FilterExecutor::new(
        build_from_prost(&expr).unwrap(),
        Box::new(mock_executor),
        "FilterBenchmark".to_string(),
    ))
}

async fn execute_filter_executor(executor: BoxedExecutor) {
    let mut stream = executor.execute();
    while let Some(ret) = stream.next().await {
        black_box(ret.unwrap());
    }
}

fn bench_filter(c: &mut Criterion) {
    const TOTAL_SIZE: usize = 1024 * 1024usize;
    let rt = Runtime::new().unwrap();
    for chunk_size in &[32usize, 128, 512, 1024, 2048, 4096] {
        c.bench_with_input(
            BenchmarkId::new("FilterExecutor", chunk_size),
            chunk_size,
            |b, &chunk_size| {
                let chunk_num = TOTAL_SIZE / chunk_size;
                b.to_async(&rt).iter_batched(
                    || create_filter_executor(chunk_size, chunk_num),
                    |e| execute_filter_executor(e),
                    BatchSize::SmallInput,
                );
            },
        );
    }
}

criterion_group!(benches, bench_filter);
criterion_main!(benches);
