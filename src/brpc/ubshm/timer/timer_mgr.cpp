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

#include <atomic>
#include <new>
#include "bthread/unstable.h"                    // bthread_timer_add/del
#include "butil/time.h"
#include "brpc/ubshm/timer/timer_mgr.h"

namespace brpc {
namespace ubring {

namespace {

std::atomic<uint32_t> g_total_timer_num(0);

}  // namespace

// Ownership: one "owner" reference held on behalf of the slot published by
// UbrTimerStart, plus one "schedule" reference for each pending/running
// bthread timer schedule. A schedule reference is consumed either by the
// firing callback or by the deleter whose bthread_timer_del returns 0
// (cancelled before run) -- exactly one side wins, so the task is freed
// precisely when both references are gone and never while a callback may
// still touch it.
//
// `stopped', `id' and `ref' all use seq_cst so that the re-arm vs delete
// race below is closed regardless of interleaving: the re-arm claims a
// reference before re-reading `stopped', and the deleter stores `stopped'
// before cancelling, so either the re-arm gives the claim up, or the
// deleter (or the re-arm's own re-check) successfully cancels the freshly
// scheduled timer and consumes the claim. The task therefore cannot be
// freed while a schedule or a running callback still refers to it (no UAF).
struct UbrTimerTask {
    std::atomic<bthread_timer_t> id;
    void* (*cb)(void*);
    void* arg;
    UbrTimerBackoffFn backoff;
    uint64_t interval_us;                        // timer thread only
    bool periodic;
    std::atomic<bool> stopped;
    std::atomic<int> ref;
};

namespace {

void ReleaseRef(UbrTimerTask* task) {
    if (task->ref.fetch_sub(1) == 1) {
        g_total_timer_num.fetch_sub(1);
        delete task;
    }
}

void UbrTimerOnFire(void* p) {
    UbrTimerTask* task = (UbrTimerTask*)p;
    if (!task->stopped.load()) {
        task->cb(task->arg);
    }

    if (task->periodic) {
        // Claim the next schedule's reference BEFORE re-reading `stopped':
        // a racing UbrTimerDel either sees our claim (it will not free the
        // task) or we see its stop flag (we will not re-arm).
        task->ref.fetch_add(1);
        if (task->stopped.load()) {
            ReleaseRef(task);                    // give up re-arming
        } else {
            uint64_t interval = task->interval_us;
            if (task->backoff != nullptr) {
                interval = task->backoff(task->arg, interval);
                task->interval_us = interval;
            }
            bthread_timer_t id = 0;
            int rc = bthread_timer_add(
                &id, butil::microseconds_from_now((int64_t)interval),
                UbrTimerOnFire, task);
            if (rc == 0) {
                task->id.store(id);
                if (task->stopped.load() &&
                    bthread_timer_del(task->id.load()) == 0) {
                    ReleaseRef(task);            // cancelled what we scheduled
                }
            } else {
                LOG(ERROR) << "Fail to re-arm ubring timer, rc=" << rc;
                ReleaseRef(task);                // stop the periodic chain
            }
        }
    }
    ReleaseRef(task);                            // consume current schedule
}

}  // namespace

void UbrTimerStart(UbrTimerId* slot, uint64_t delay_us, uint64_t interval_us,
                   void* (*cb)(void*), void* arg, UbrTimerBackoffFn backoff) {
    if (UNLIKELY(slot == nullptr || cb == nullptr)) {
        LOG(ERROR) << "UbrTimerStart invalid argument, slot=" << slot;
        return;
    }

    UbrTimerTask* task = new (std::nothrow) UbrTimerTask();
    if (UNLIKELY(task == nullptr)) {
        LOG(ERROR) << "Fail to malloc ubring timer task.";
        return;
    }
    task->id.store(0);
    task->cb = cb;
    task->arg = arg;
    task->backoff = backoff;
    task->interval_us = interval_us;
    task->periodic = (interval_us > 0);
    task->stopped.store(false);
    task->ref.store(2);                          // owner + first schedule

    bthread_timer_t id = 0;
    int rc = bthread_timer_add(
        &id, butil::microseconds_from_now((int64_t)delay_us),
        UbrTimerOnFire, task);
    if (UNLIKELY(rc != 0)) {
        LOG(ERROR) << "Fail to add ubring timer, rc=" << rc;
        delete task;
        return;
    }
    task->id.store(id);
    g_total_timer_num.fetch_add(1);

    // If the timer fires before this store, the re-arm path may race with
    // it on `id'; the losing store only costs one self-healing skip fire.
    *slot = task;
}

void UbrTimerDel(UbrTimerId* slot) {
    if (slot == nullptr) {
        return;
    }
    UbrTimerTask* task =
        __atomic_exchange_n(slot, (UbrTimerId) nullptr, __ATOMIC_SEQ_CST);
    if (task == nullptr) {
        return;
    }

    task->stopped.store(true);
    // Never wait for a running callback: self-delete from inside the
    // callback lands here (bthread_timer_del reports 1/EINVAL) and the
    // schedule reference is consumed when the callback returns.
    int rc = bthread_timer_del(task->id.load());
    if (rc == 0) {
        ReleaseRef(task);                        // cancelled before run
    }
    ReleaseRef(task);                            // owner reference
}

uint32_t GetActiveTimerNum(void) {
    return g_total_timer_num.load();
}

}  // namespace ubring
}  // namespace brpc
