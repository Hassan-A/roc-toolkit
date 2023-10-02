/*
 * Copyright (c) 2020 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include <benchmark/benchmark.h>

#include "roc_core/fast_random.h"
#include "roc_ctl/control_task_executor.h"
#include "roc_ctl/control_task_queue.h"

namespace roc {
namespace ctl {
namespace {

const core::nanoseconds_t MaxDelay = 100 * core::Millisecond;

enum {
    NumScheduleIterations = 2000000,
    NumScheduleAfterIterations = 20000,
    NumThreads = 2,
    BatchSize = 1000
};

class NoopExecutor : public ControlTaskExecutor<NoopExecutor> {
public:
    class Task : public ControlTask {
    public:
        Task()
            : ControlTask(&NoopExecutor::do_task_) {
        }
    };

private:
    ControlTaskResult do_task_(ControlTask&) {
        return ControlTaskSuccess;
    }
};

class NoopCompleter : public IControlTaskCompleter {
public:
    virtual void control_task_completed(ControlTask&) {
    }
};

struct BM_QueueSequential : benchmark::Fixture {
    ControlTaskQueue queue;
    NoopExecutor executor;
    NoopCompleter completer;
};

BENCHMARK_DEFINE_F(BM_QueueSequential, Schedule)(benchmark::State& state) {
    NoopExecutor::Task* tasks = new NoopExecutor::Task[NumScheduleIterations];
    size_t n_task = 0;

    while (state.KeepRunningBatch(BatchSize)) {
        for (int n = 0; n < BatchSize; n++) {
            queue.schedule(tasks[n_task++], executor, &completer);
        }
    }

    for (int n = 0; n < NumScheduleIterations; n++) {
        queue.wait(tasks[n]);
    }

    delete[] tasks;
}

BENCHMARK_REGISTER_F(BM_QueueSequential, Schedule)
    ->ThreadRange(1, NumThreads)
    ->Iterations(NumScheduleIterations)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_DEFINE_F(BM_QueueSequential, ScheduleAt)(benchmark::State& state) {
    NoopExecutor::Task* tasks = new NoopExecutor::Task[NumScheduleIterations];
    core::nanoseconds_t* delays = new core::nanoseconds_t[NumScheduleIterations];
    size_t n_task = 0;

    for (int n = 0; n < NumScheduleIterations; n++) {
        delays[n] = core::fast_random(0, MaxDelay);
    }

    while (state.KeepRunningBatch(BatchSize)) {
        for (int n = 0; n < BatchSize; n++) {
            queue.schedule_at(tasks[n_task],
                              core::timestamp(core::ClockMonotonic) + delays[n_task],
                              executor, &completer);
            n_task++;
        }
    }

    for (int n = 0; n < NumScheduleIterations; n++) {
        queue.wait(tasks[n]);
    }

    delete[] tasks;
    delete[] delays;
}

BENCHMARK_REGISTER_F(BM_QueueSequential, ScheduleAt)
    ->ThreadRange(1, NumThreads)
    ->Iterations(NumScheduleIterations)
    ->Unit(benchmark::kMicrosecond);

} // namespace
} // namespace ctl
} // namespace roc
