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

#include "pipeline_task.h"

#include "pipeline/pipeline_fragment_context.h"

namespace doris::pipeline {

void PipelineTask::_init_profile() {
    std::stringstream ss;
    ss << "PipelineTask"
       << " (index=" << _index << ")";
    auto* task_profile = new RuntimeProfile(ss.str());
    _parent_profile->add_child(task_profile, true, nullptr);
    _task_profile.reset(task_profile);
    _sink_timer = ADD_TIMER(_task_profile, "SinkTime");
    _get_block_timer = ADD_TIMER(_task_profile, "GetBlockTime");
}

Status PipelineTask::prepare(RuntimeState* state) {
    DCHECK(_sink);
    _init_profile();
    RETURN_IF_ERROR(_sink->prepare(state));
    for (auto& o : _operators) {
        RETURN_IF_ERROR(o->prepare(state));
    }
    _task_profile->add_child(_sink->runtime_profile(), true, nullptr);
    RETURN_IF_ERROR(_root->link_profile(_task_profile.get()));
    _block.reset(new doris::vectorized::Block());
    _prepared = true;
    return Status::OK();
}

bool PipelineTask::has_dependency() {
    if (_dependency_finish) {
        return false;
    }
    if (_fragment_context->is_canceled()) {
        _dependency_finish = true;
        return false;
    }
    if (_pipeline->has_dependency()) {
        return true;
    }
    // FE do not call execute
    if (!_state->get_query_fragments_ctx()
                 ->is_ready_to_execute()) { // TODO pipeline config::s_ready_to_execute
        return true;
    }

    // runtime filter is a dependency
    _dependency_finish = true;
    return false;
}

Status PipelineTask::open() {
    if (_sink) {
        RETURN_IF_ERROR(_sink->open(_state));
    }
    for (auto& o : _operators) {
        RETURN_IF_ERROR(o->open(_state));
    }
    _opened = true;
    return Status::OK();
}

Status PipelineTask::execute(bool* eos) {
    SCOPED_ATTACH_TASK(runtime_state());
    SCOPED_TIMER(_task_profile->total_time_counter());
    int64_t time_spent = 0;
    // The status must be runnable
    *eos = false;
    if (!_opened) {
        SCOPED_RAW_TIMER(&time_spent);
        RETURN_IF_ERROR(open());
    }
    while (_source->can_read() && _sink->can_write() && !_fragment_context->is_canceled()) {
        if (time_spent > THREAD_TIME_SLICE) {
            break;
        }
        SCOPED_RAW_TIMER(&time_spent);
        _block->clear_column_data(_root->row_desc().num_materialized_slots());
        auto* block = _block.get();

        // Pull block from operator chain
        {
            SCOPED_TIMER(_get_block_timer);
            RETURN_IF_ERROR(_root->get_block(_state, block, eos));
        }
        if (_block->rows() != 0 || *eos) {
            SCOPED_TIMER(_sink_timer);
            RETURN_IF_ERROR(_sink->sink(_state, block, *eos));
            if (*eos) { // just return, the scheduler will do finish work
                break;
            }
        }
    }

    if (!*eos && (!_source->can_read() || !_sink->can_write())) {
        set_state(BLOCKED);
    }

    return Status::OK();
}

Status PipelineTask::finalize() {
    return _sink->finalize(_state);
}

Status PipelineTask::close() {
    auto s = _sink->close(_state);
    for (auto& op : _operators) {
        auto tem = op->close(_state);
        if (!tem.ok() && s.ok()) {
            s = tem;
        }
    }
    _pipeline->close(_state);
    return s;
}

QueryFragmentsCtx* PipelineTask::query_fragments_context() {
    return _fragment_context->get_query_context();
}
} // namespace doris::pipeline