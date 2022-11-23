// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "aggregation_sink_operator.h"

#include "vec/exec/vaggregation_node.h"

namespace doris::pipeline {

AggSinkOperator::AggSinkOperator(AggSinkOperatorTemplate* operator_template,
                                 vectorized::AggregationNode* agg_node)
        : Operator(operator_template), _agg_node(agg_node) {}

Status AggSinkOperator::init(ExecNode* exec_node, RuntimeState* state) {
    RETURN_IF_ERROR(Operator::init(exec_node, state));
    return Status::OK();
}

Status AggSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _agg_node->_child_return_rows =
            std::bind<int64_t>(&AggSinkOperator::get_child_return_rows, this);
    return Status::OK();
}

Status AggSinkOperator::open(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::open(state));
    return Status::OK();
}

bool AggSinkOperator::can_write() {
    return true;
}

Status AggSinkOperator::sink(RuntimeState* state, vectorized::Block* in_block, bool eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    if (!_agg_node->is_streaming_preagg()) {
        RETURN_IF_ERROR(_agg_node->sink(state, in_block, eos));
    }
    // TODO: remove it after we split the stream agg and normal agg
    _agg_node->_executor.update_memusage();
    return Status::OK();
}

Status AggSinkOperator::close(RuntimeState* state) {
    return Status::OK();
}

///////////////////////////////  operator template  ////////////////////////////////

AggSinkOperatorTemplate::AggSinkOperatorTemplate(int32_t id, const std::string& name,
                                                 vectorized::AggregationNode* exec_node)
        : OperatorTemplate(id, name, exec_node),
          _agg_node(exec_node) {}

OperatorPtr AggSinkOperatorTemplate::build_operator() {
    return std::make_shared<AggSinkOperator>(this, _agg_node);
}

// use final aggregation source operator
bool AggSinkOperatorTemplate::is_sink() const {
    return true;
}

bool AggSinkOperatorTemplate::is_source() const {
    return false;
}
} // namespace doris::pipeline