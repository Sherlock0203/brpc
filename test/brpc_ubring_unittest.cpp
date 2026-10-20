// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to You under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <functional>
#include <limits>
#include <sys/mman.h>
#include <unistd.h>
#include <gflags/gflags.h>
#include <string>
#include <thread>
#include <vector>
#include "butil/macros.h"
#include "butil/atomicops.h"
#include "butil/sys_byteorder.h"
#include "brpc/socket.h"

#if BRPC_WITH_UBRING
#include "brpc/ubshm/common/common.h"
#include "brpc/ubshm/ub_cleanup_worker.h"
#include "brpc/ubshm/ub_endpoint.h"
#include "brpc/ubshm/shm/shm_def.h"
#include "brpc/ubshm/shm/shm_mgr.h"
#include "brpc/ubshm/ub_ring_manager.h"
#include "brpc/ubshm/ub_ring.h"
#include "brpc/ubshm/timer/timer_mgr.h"
#include "brpc/ubshm/ubr_msg.h"
#include "brpc/ubshm/ubr_msg_v2.h"
#include "brpc/ubshm_transport.h"
#include "brpc/server.h"

namespace brpc {
namespace ubring {
DECLARE_int32(ub_disconnect_timeout_s);
DECLARE_int32(ub_connect_timeout_s);
DECLARE_int32(ub_hb_timer_interval_s);
DECLARE_int32(ub_event_queue_timer_interval_us);
DECLARE_int32(ub_event_queue_timer_interval_max_us);
DECLARE_int32(ub_flying_io_timeout_s);
DECLARE_int32(ubr_max_managed_num);
DECLARE_int32(tail_update_after_read);
DECLARE_int32(ub_shm_type);
DECLARE_bool(ub_poller_yield);

extern bool g_skip_ub_init;
extern butil::atomic<int> g_ub_test_worker_start_rc;
}  // namespace ubring
}  // namespace brpc

namespace {
TEST(IpcV2LayoutTest, fixed_layout) {
    using brpc::ubring::IpcV2Slot;

    EXPECT_EQ(4096u, sizeof(IpcV2Slot));
    EXPECT_EQ(64u, alignof(IpcV2Slot));
    EXPECT_EQ(16u, offsetof(IpcV2Slot, payload));
    EXPECT_EQ(4080u, sizeof(((IpcV2Slot*)nullptr)->payload));
    EXPECT_EQ(0, brpc::ubring::UBR_DATA_FORMAT_NONE);
    EXPECT_EQ(1, brpc::ubring::UBR_DATA_FORMAT_LEGACY_64);
    EXPECT_EQ(2, brpc::ubring::UBR_DATA_FORMAT_IPC_V2);
}

TEST(IpcV2RingTest, fixed_writev_readv_boundaries_and_wrap) {
    using namespace brpc::ubring;
    IpcV2Slot slots[4] = {};
    uint32_t tail = 0;
    uint32_t write_pos = 0;
    uint32_t read_pos = 0;
    uint32_t read_offset = 0;
    IpcV2TxView tx(slots, &tail, 4, &write_pos);
    IpcV2RxView rx(slots, &tail, 4, &read_pos, &read_offset);
    EXPECT_EQ(IPC_V2_SLOT_UNINITIALIZED,
              __atomic_load_n(&slots[0].header.state, __ATOMIC_ACQUIRE));
    ASSERT_TRUE(IpcV2RxView::InitializeShared(slots, &tail, 4));
    EXPECT_EQ(3u, __atomic_load_n(&tail, __ATOMIC_ACQUIRE));

    std::string source(IpcV2Slot::PAYLOAD_SIZE + 19, '\0');
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<char>((i * 29 + 7) & 0xff);
    }
    struct iovec write_iov[4] = {
        {nullptr, 0},
        {&source[0], 7},
        {&source[7], IpcV2Slot::PAYLOAD_SIZE - 3},
        {&source[IpcV2Slot::PAYLOAD_SIZE + 4], 15},
    };
    size_t written = 99;
    ASSERT_EQ(IPC_V2_RING_OK, tx.TryWritev(write_iov, 4, written));
    EXPECT_EQ(source.size(), written);
    EXPECT_EQ(2u, write_pos);
    EXPECT_EQ(IpcV2Slot::PAYLOAD_SIZE, slots[0].header.payload_len);
    EXPECT_EQ(0u, slots[0].header.flags);
    EXPECT_EQ(19u, slots[1].header.payload_len);
    EXPECT_EQ(static_cast<uint32_t>(IPC_V2_SLOT_EOF), slots[1].header.flags);

    std::string output(source.size(), '?');
    struct iovec first_read = {&output[0], 31};
    size_t read = 99;
    bool batch_end = true;
    ASSERT_EQ(IPC_V2_RING_OK, rx.TryReadv(&first_read, 1, read, batch_end));
    EXPECT_EQ(31u, read);
    EXPECT_FALSE(batch_end);
    EXPECT_EQ(0u, read_pos);
    EXPECT_EQ(31u, read_offset);
    EXPECT_EQ(3u, __atomic_load_n(&tail, __ATOMIC_ACQUIRE));
    struct iovec remaining_iov[4] = {
        {nullptr, 0},
        {&output[31], 5},
        {&output[36], IpcV2Slot::PAYLOAD_SIZE - 20},
        {&output[IpcV2Slot::PAYLOAD_SIZE + 16], 3},
    };
    ASSERT_EQ(IPC_V2_RING_OK, rx.TryReadv(remaining_iov, 4, read, batch_end));
    EXPECT_EQ(source.size() - 31, read);
    EXPECT_TRUE(batch_end);
    EXPECT_EQ(source, output);
    EXPECT_EQ(2u, read_pos);
    EXPECT_EQ(0u, read_offset);
    EXPECT_EQ(1u, __atomic_load_n(&tail, __ATOMIC_ACQUIRE));

    std::string oversized(IpcV2Slot::PAYLOAD_SIZE * 4, 'x');
    struct iovec oversized_iov = {&oversized[0], oversized.size()};
    EXPECT_EQ(IPC_V2_RING_TOO_LARGE,
              tx.TryWritev(&oversized_iov, 1, written));
    EXPECT_EQ(0u, written);

    char values[4] = {'a', 'b', 'c', 'd'};
    for (int i = 0; i < 3; ++i) {
        struct iovec item = {&values[i], 1};
        ASSERT_EQ(IPC_V2_RING_OK, tx.TryWritev(&item, 1, written));
    }
    struct iovec fourth = {&values[3], 1};
    EXPECT_EQ(IPC_V2_RING_RETRY, tx.TryWritev(&fourth, 1, written));
    EXPECT_EQ(0u, written);
    char result[4] = {};
    struct iovec one = {&result[0], 1};
    ASSERT_EQ(IPC_V2_RING_OK, rx.TryReadv(&one, 1, read, batch_end));
    ASSERT_TRUE(batch_end);
    ASSERT_EQ(IPC_V2_RING_OK, tx.TryWritev(&fourth, 1, written));
    for (int i = 1; i < 4; ++i) {
        struct iovec destination = {&result[i], 1};
        ASSERT_EQ(IPC_V2_RING_OK,
                  rx.TryReadv(&destination, 1, read, batch_end));
        ASSERT_TRUE(batch_end);
    }
    EXPECT_EQ(0, memcmp(values, result, sizeof(values)));
}

TEST(IpcV2RingTest, rejects_invalid_iovec_without_publication) {
    using namespace brpc::ubring;
    IpcV2Slot slots[2] = {};
    uint32_t tail = 0;
    uint32_t write_pos = 0;
    uint32_t read_pos = 0;
    uint32_t read_offset = 0;
    IpcV2TxView tx(slots, &tail, 2, &write_pos);
    IpcV2RxView rx(slots, &tail, 2, &read_pos, &read_offset);
    size_t bytes = 99;
    bool batch_end = true;
    char value = 'x';
    struct iovec one_byte = {&value, 1};
    EXPECT_EQ(IPC_V2_RING_INVALID, tx.TryWritev(&one_byte, 1, bytes));
    EXPECT_EQ(IPC_V2_RING_INVALID,
              rx.TryReadv(&one_byte, 1, bytes, batch_end));
    ASSERT_TRUE(IpcV2RxView::InitializeShared(slots, &tail, 2));
    EXPECT_EQ(IPC_V2_RING_INVALID, tx.TryWritev(nullptr, 1, bytes));
    EXPECT_EQ(IPC_V2_RING_INVALID, tx.TryWritev(nullptr, -1, bytes));
    EXPECT_EQ(IPC_V2_RING_INVALID,
              rx.TryReadv(nullptr, 1, bytes, batch_end));
    struct iovec null_data = {nullptr, 1};
    EXPECT_EQ(IPC_V2_RING_INVALID, tx.TryWritev(&null_data, 1, bytes));
    EXPECT_EQ(IPC_V2_RING_OK, tx.TryWritev(nullptr, 0, bytes));
    EXPECT_EQ(0u, bytes);
    EXPECT_EQ(IPC_V2_RING_OK, rx.TryReadv(nullptr, 0, bytes, batch_end));
    EXPECT_EQ(0u, bytes);
    EXPECT_FALSE(batch_end);
    EXPECT_EQ(IPC_V2_SLOT_EMPTY,
              __atomic_load_n(&slots[0].header.state, __ATOMIC_ACQUIRE));
}

TEST(IpcV2RingTest, concurrent_writev_readv) {
    using namespace brpc::ubring;
    IpcV2Slot slots[8] = {};
    uint32_t tail = 0;
    uint32_t write_pos = 0;
    uint32_t read_pos = 0;
    uint32_t read_offset = 0;
    ASSERT_TRUE(IpcV2RxView::InitializeShared(slots, &tail, 8));
    IpcV2TxView tx(slots, &tail, 8, &write_pos);
    IpcV2RxView rx(slots, &tail, 8, &read_pos, &read_offset);
    const uint32_t rounds = 200;
    const auto deadline = std::chrono::steady_clock::now() +
        std::chrono::seconds(10);
    std::atomic<bool> stop(false);
    std::string producer_error;
    std::string consumer_error;
    const auto message_size = [](uint32_t sequence) {
        return 1 + (sequence * 53) % (IpcV2Slot::PAYLOAD_SIZE * 2);
    };
    const auto fill = [](uint32_t sequence, char* data, size_t length) {
        for (size_t i = 0; i < length; ++i) {
            data[i] = static_cast<char>((sequence * 11 + i * 37) & 0xff);
        }
    };
    std::thread producer([&] {
        std::string input(IpcV2Slot::PAYLOAD_SIZE * 2, '\0');
        for (uint32_t sequence = 0; sequence < rounds; ++sequence) {
            const size_t length = message_size(sequence);
            fill(sequence, &input[0], length);
            const size_t first = length < 3 ? length : 3;
            struct iovec parts[3] = {
                {&input[0], first},
                {nullptr, 0},
                {&input[first], length - first},
            };
            for (;;) {
                size_t written = 0;
                const IpcV2RingResult result = tx.TryWritev(parts, 3, written);
                if (result == IPC_V2_RING_OK && written == length) {
                    break;
                }
                if (result != IPC_V2_RING_RETRY ||
                    stop.load(std::memory_order_relaxed) ||
                    std::chrono::steady_clock::now() >= deadline) {
                    producer_error = "valid writev failed or timed out";
                    stop.store(true, std::memory_order_relaxed);
                    return;
                }
                std::this_thread::yield();
            }
        }
    });

    std::string output(IpcV2Slot::PAYLOAD_SIZE * 2, '\0');
    std::string expected(IpcV2Slot::PAYLOAD_SIZE * 2, '\0');
    for (uint32_t sequence = 0; sequence < rounds; ++sequence) {
        const size_t length = message_size(sequence);
        fill(sequence, &expected[0], length);
        size_t received = 0;
        bool ended = false;
        while (!ended && !stop.load(std::memory_order_relaxed)) {
            const size_t chunk = std::min(size_t(113), length - received);
            struct iovec destination = {&output[received], chunk};
            size_t read = 0;
            const IpcV2RingResult result =
                rx.TryReadv(&destination, 1, read, ended);
            if (result == IPC_V2_RING_RETRY) {
                if (std::chrono::steady_clock::now() >= deadline) {
                    consumer_error = "consumer timed out";
                    break;
                }
                std::this_thread::yield();
                continue;
            }
            if (result != IPC_V2_RING_OK || read == 0 || read > chunk) {
                consumer_error = "valid readv failed";
                break;
            }
            received += read;
        }
        if (!consumer_error.empty() || stop.load(std::memory_order_relaxed)) {
            break;
        }
        if (received != length ||
            memcmp(output.data(), expected.data(), length) != 0) {
            consumer_error = "batch length, order or content mismatch";
            break;
        }
    }
    stop.store(true, std::memory_order_relaxed);
    producer.join();
    EXPECT_TRUE(producer_error.empty()) << producer_error;
    EXPECT_TRUE(consumer_error.empty()) << consumer_error;
}

TEST(IpcV2QueueMappingTest, reuses_existing_tail_and_preserves_neighbors) {
    using namespace brpc::ubring;
    struct alignas(64) QueueStorage {
        IpcV2Slot slots[2];
        uint8_t trailing[31];
    } storage;
    memset(&storage, 0xa5, sizeof(storage));
    UbrDataStatusQMsg status = {};
    status.tail = 99;
    status.timeout = 12345;
    status.heart_beat = 7;

    IpcV2QueueView queue;
    ASSERT_TRUE(InitializeIpcV2ReceiveQueue(
        reinterpret_cast<uint8_t*>(storage.slots), sizeof(storage),
        &status.tail, queue));
    EXPECT_EQ(storage.slots, queue.slots);
    EXPECT_EQ(&status.tail, queue.tail);
    EXPECT_EQ(2u, queue.capacity);
    EXPECT_EQ(1u, __atomic_load_n(&status.tail, __ATOMIC_ACQUIRE));
    EXPECT_EQ(12345u, status.timeout);
    EXPECT_EQ(7u, status.heart_beat);
    EXPECT_EQ(IPC_V2_SLOT_EMPTY,
              __atomic_load_n(&storage.slots[0].header.state,
                              __ATOMIC_ACQUIRE));
    EXPECT_EQ(IPC_V2_SLOT_EMPTY,
              __atomic_load_n(&storage.slots[1].header.state,
                              __ATOMIC_ACQUIRE));
    const uint8_t* trailing = reinterpret_cast<const uint8_t*>(&storage) +
        sizeof(storage.slots);
    for (size_t i = 0; i < sizeof(storage) - sizeof(storage.slots); ++i) {
        EXPECT_EQ(0xa5, trailing[i]);
    }
}

TEST(IpcV2QueueMappingTest, rejects_invalid_layout_without_modification) {
    using namespace brpc::ubring;
    IpcV2Slot slots[2] = {};
    uint32_t tail = 77;
    IpcV2QueueView queue;
    EXPECT_FALSE(MapIpcV2Queue(nullptr, sizeof(slots), &tail, queue));
    EXPECT_FALSE(MapIpcV2Queue(
        reinterpret_cast<uint8_t*>(slots), sizeof(IpcV2Slot), &tail, queue));
    EXPECT_FALSE(MapIpcV2Queue(
        reinterpret_cast<uint8_t*>(slots) + 1, sizeof(slots) - 1,
        &tail, queue));
    alignas(uint32_t) uint8_t tail_storage[sizeof(uint32_t) + 1] = {};
    EXPECT_FALSE(MapIpcV2Queue(
        reinterpret_cast<uint8_t*>(slots), sizeof(slots),
        reinterpret_cast<uint32_t*>(tail_storage + 1), queue));
    EXPECT_EQ(nullptr, queue.slots);
    EXPECT_EQ(nullptr, queue.tail);
    EXPECT_EQ(0u, queue.capacity);
    EXPECT_EQ(77u, tail);
}

TEST(IpcV2FormatPreparationTest, prepares_layout_without_activation) {
    using namespace brpc::ubring;
    IpcV2Slot local_slots[2] = {};
    IpcV2Slot remote_slots[3] = {};
    UbrDataStatusQMsg local_status = {};
    UbrDataStatusQMsg remote_status = {};
    local_status.tail = 91;
    local_status.timeout = 111;
    local_status.heart_beat = 3;
    remote_status.tail = 92;
    remote_status.timeout = 222;
    remote_status.heart_beat = 4;
    UbrTrx trx = {};
    trx.ubr_tx.remote_data_q.addr =
        reinterpret_cast<uint8_t*>(remote_slots);
    trx.ubr_tx.remote_data_q.len = sizeof(remote_slots);
    trx.ubr_tx.local_data_status_q.addr =
        reinterpret_cast<uint8_t*>(&local_status);
    trx.ubr_rx.local_data_q.addr = reinterpret_cast<uint8_t*>(local_slots);
    trx.ubr_rx.local_data_q.len = sizeof(local_slots);
    trx.ubr_rx.remote_data_status_q.addr =
        reinterpret_cast<uint8_t*>(&remote_status);

    UBRing ring;
    ring.SetTrxForTest(&trx);
    ASSERT_EQ(0, ring.UbrPrepareIpcV2Format());
    EXPECT_EQ(UBR_DATA_FORMAT_IPC_V2, ring.data_format());
    EXPECT_EQ(3u, trx.ubr_tx.capacity);
    EXPECT_EQ(0u, trx.ubr_tx.write_pos);
    EXPECT_EQ(2u, trx.ubr_rx.capacity);
    EXPECT_EQ(0u, trx.ubr_rx.read_pos);
    EXPECT_EQ(0u, trx.ubr_rx.ipc_v2_read_offset);
    EXPECT_EQ(UBR_STATE_NONE, trx.ubr_tx.trx_state);
    EXPECT_EQ(UBR_STATE_NONE, trx.ubr_rx.trx_state);
    // The close/heartbeat timers must stay unarmed (the pre-facade fields
    // timer_fd/hb_timer_fd became the close_timer/hb_timer slots).
    EXPECT_EQ(nullptr, trx.close_timer.load());
    EXPECT_EQ(nullptr, trx.hb_timer.load());
    EXPECT_EQ(91u, local_status.tail);
    EXPECT_EQ(1u, __atomic_load_n(&remote_status.tail, __ATOMIC_ACQUIRE));
    EXPECT_EQ(111u, local_status.timeout);
    EXPECT_EQ(3u, local_status.heart_beat);
    EXPECT_EQ(222u, remote_status.timeout);
    EXPECT_EQ(4u, remote_status.heart_beat);
    EXPECT_EQ(IPC_V2_SLOT_EMPTY,
              __atomic_load_n(&local_slots[0].header.state,
                              __ATOMIC_ACQUIRE));
    EXPECT_EQ(IPC_V2_SLOT_UNINITIALIZED,
              __atomic_load_n(&remote_slots[0].header.state,
                              __ATOMIC_ACQUIRE));
    EXPECT_EQ(-1, ring.UbrPrepareIpcV2Format());
}

TEST(IpcV2FormatPreparationTest, failure_keeps_connection_unselected) {
    using namespace brpc::ubring;
    alignas(64) uint8_t local_storage[sizeof(IpcV2Slot) * 2 + 1] = {};
    IpcV2Slot remote_slots[2] = {};
    UbrDataStatusQMsg local_status = {};
    UbrDataStatusQMsg remote_status = {};
    local_status.tail = 71;
    remote_status.tail = 72;
    UbrTrx trx = {};
    trx.ubr_tx.remote_data_q.addr =
        reinterpret_cast<uint8_t*>(remote_slots);
    trx.ubr_tx.remote_data_q.len = sizeof(remote_slots);
    trx.ubr_tx.local_data_status_q.addr =
        reinterpret_cast<uint8_t*>(&local_status);
    trx.ubr_rx.local_data_q.addr = local_storage + 1;
    trx.ubr_rx.local_data_q.len = sizeof(local_storage) - 1;
    trx.ubr_rx.remote_data_status_q.addr =
        reinterpret_cast<uint8_t*>(&remote_status);

    UBRing ring;
    ring.SetTrxForTest(&trx);
    EXPECT_EQ(-1, ring.UbrPrepareIpcV2Format());
    EXPECT_EQ(UBR_DATA_FORMAT_NONE, ring.data_format());
    EXPECT_EQ(0u, trx.ubr_tx.capacity);
    EXPECT_EQ(0u, trx.ubr_rx.capacity);
    EXPECT_EQ(71u, local_status.tail);
    EXPECT_EQ(72u, remote_status.tail);
}

struct IpcV2UbrTestContext {
    IpcV2UbrTestContext() {
        memset(local_slots, 0, sizeof(local_slots));
        memset(remote_slots, 0, sizeof(remote_slots));
        memset(&local_status, 0, sizeof(local_status));
        memset(&remote_status, 0, sizeof(remote_status));
        memset(&local_rx_event, 0, sizeof(local_rx_event));
        memset(&local_tx_event, 0, sizeof(local_tx_event));
        memset(&remote_rx_event, 0, sizeof(remote_rx_event));
        trx.local_shm.addr = reinterpret_cast<uint8_t*>(local_slots);
        trx.ubr_tx.remote_data_q.addr =
            reinterpret_cast<uint8_t*>(remote_slots);
        trx.ubr_tx.remote_data_q.len = sizeof(remote_slots);
        trx.ubr_tx.local_data_status_q.addr =
            reinterpret_cast<uint8_t*>(&local_status);
        trx.ubr_tx.local_tx_event_q.addr =
            reinterpret_cast<uint8_t*>(&local_tx_event);
        trx.ubr_tx.remote_rx_event_q.addr =
            reinterpret_cast<uint8_t*>(&remote_rx_event);
        trx.ubr_rx.local_data_q.addr =
            reinterpret_cast<uint8_t*>(local_slots);
        trx.ubr_rx.local_data_q.len = sizeof(local_slots);
        trx.ubr_rx.remote_data_status_q.addr =
            reinterpret_cast<uint8_t*>(&remote_status);
        trx.ubr_rx.local_rx_event_q.addr =
            reinterpret_cast<uint8_t*>(&local_rx_event);
        ring.SetTrxForTest(&trx);
    }

    bool PrepareAndConnect() {
        if (ring.UbrPrepareIpcV2Format() != 0 ||
            !brpc::ubring::IpcV2RxView::InitializeShared(
                remote_slots, &local_status.tail, CAPACITY)) {
            return false;
        }
        trx.ubr_tx.trx_state = brpc::ubring::UBR_STATE_CONNECTED;
        trx.ubr_rx.trx_state = brpc::ubring::UBR_STATE_CONNECTED;
        return true;
    }

    enum { CAPACITY = 4 };
    brpc::ubring::IpcV2Slot local_slots[CAPACITY];
    brpc::ubring::IpcV2Slot remote_slots[CAPACITY];
    brpc::ubring::UbrDataStatusQMsg local_status;
    brpc::ubring::UbrDataStatusQMsg remote_status;
    brpc::ubring::UbrEventQMsg local_rx_event;
    brpc::ubring::UbrEventQMsg local_tx_event;
    brpc::ubring::UbrEventQMsg remote_rx_event;
    brpc::ubring::UbrTrx trx{};
    brpc::ubring::UBRing ring;
};

TEST(IpcV2UbrDispatchTest, writev_readv_and_notifications) {
    using namespace brpc::ubring;
    IpcV2UbrTestContext context;
    ASSERT_TRUE(context.PrepareAndConnect());
    EXPECT_EQ(UBRING_OK, context.ring.IsUbrTrxWriteable(0));

    std::string source(IpcV2Slot::PAYLOAD_SIZE + 19, '\0');
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<char>((i * 13 + 5) & 0xff);
    }
    struct iovec source_iov[2] = {
        {&source[0], 17},
        {&source[17], source.size() - 17},
    };
    ASSERT_EQ(static_cast<ssize_t>(source.size()),
              context.ring.UbrTrxWritev(source_iov, 2));
    EXPECT_EQ(1u, context.trx.ubr_tx.out_io_id);
    EXPECT_EQ(1u, context.remote_rx_event.io_id);

    uint32_t peer_read_pos = 0;
    uint32_t peer_read_offset = 0;
    IpcV2RxView peer_rx(
        context.remote_slots, &context.local_status.tail,
        IpcV2UbrTestContext::CAPACITY, &peer_read_pos, &peer_read_offset);
    std::string peer_output(source.size(), '?');
    struct iovec peer_destination = {&peer_output[0], peer_output.size()};
    size_t peer_read = 0;
    bool peer_batch_end = false;
    ASSERT_EQ(IPC_V2_RING_OK,
              peer_rx.TryReadv(&peer_destination, 1, peer_read,
                               peer_batch_end));
    EXPECT_TRUE(peer_batch_end);
    EXPECT_EQ(source, peer_output);

    char empty_value = 0;
    struct iovec empty_destination = {&empty_value, 1};
    errno = 0;
    EXPECT_EQ(-1, context.ring.UbrTrxReadv(&empty_destination, 1));
    EXPECT_EQ(EAGAIN, errno);
    EXPECT_EQ(MPA_MUXER_NOT_READY, context.ring.IsUbrTrxReadable(0));

    uint32_t peer_write_pos = 0;
    IpcV2TxView peer_tx(
        context.local_slots, &context.remote_status.tail,
        IpcV2UbrTestContext::CAPACITY, &peer_write_pos);
    size_t peer_written = 0;
    ASSERT_EQ(IPC_V2_RING_OK,
              peer_tx.TryWritev(source_iov, 2, peer_written));
    context.local_rx_event.io_id = 1;
    EXPECT_EQ(UBRING_OK, context.ring.IsUbrTrxReadable(0));

    std::string output(source.size(), '?');
    struct iovec first = {&output[0], 31};
    ASSERT_EQ(31, context.ring.UbrTrxReadv(&first, 1));
    EXPECT_EQ(0u, context.trx.ubr_rx.read_pos);
    EXPECT_EQ(31u, context.trx.ubr_rx.ipc_v2_read_offset);
    struct iovec remaining = {&output[31], output.size() - 31};
    ASSERT_EQ(static_cast<ssize_t>(output.size() - 31),
              context.ring.UbrTrxReadv(&remaining, 1));
    EXPECT_EQ(2u, context.trx.ubr_rx.read_pos);
    EXPECT_EQ(0u, context.trx.ubr_rx.ipc_v2_read_offset);
    EXPECT_EQ(source, output);
}

TEST(IpcV2UbrDispatchTest, backpressure_and_error_mapping) {
    using namespace brpc::ubring;
    IpcV2UbrTestContext context;
    ASSERT_TRUE(context.PrepareAndConnect());
    char values[4] = {'a', 'b', 'c', 'd'};
    for (int i = 0; i < 3; ++i) {
        struct iovec source = {&values[i], 1};
        ASSERT_EQ(1, context.ring.UbrTrxWritev(&source, 1));
    }
    const uint64_t notified_io_id = context.remote_rx_event.io_id;
    struct iovec fourth = {&values[3], 1};
    EXPECT_EQ(UBRING_RETRY, context.ring.UbrTrxWritev(&fourth, 1));
    EXPECT_EQ(notified_io_id, context.remote_rx_event.io_id);
    EXPECT_EQ(MPA_MUXER_NOT_READY,
              context.ring.IsUbrTrxWriteable(0));

    std::string oversized(
        IpcV2Slot::PAYLOAD_SIZE * IpcV2UbrTestContext::CAPACITY, 'x');
    struct iovec too_large = {&oversized[0], oversized.size()};
    errno = 0;
    EXPECT_EQ(-1, context.ring.UbrTrxWritev(&too_large, 1));
    EXPECT_EQ(EMSGSIZE, errno);
    EXPECT_EQ(notified_io_id, context.remote_rx_event.io_id);

    context.local_slots[0].header.payload_len =
        IpcV2Slot::PAYLOAD_SIZE + 1;
    context.local_slots[0].header.flags = IPC_V2_SLOT_EOF;
    context.local_slots[0].header.reserved = 0;
    __atomic_store_n(&context.local_slots[0].header.state,
                     IPC_V2_SLOT_READY, __ATOMIC_RELEASE);
    char destination = 0;
    struct iovec invalid = {&destination, 1};
    EXPECT_EQ(UBRING_OK, context.ring.IsUbrTrxReadable(0));
    errno = 0;
    EXPECT_EQ(-1, context.ring.UbrTrxReadv(&invalid, 1));
    EXPECT_EQ(EBADMSG, errno);

    context.trx.data_format = UBR_DATA_FORMAT_NONE;
    errno = 0;
    EXPECT_EQ(UBRING_ERR, context.ring.UbrTrxWritev(&fourth, 1));
    EXPECT_EQ(EPROTONOSUPPORT, errno);
}

TEST(IpcV2UbrDispatchTest, rejects_uninitialized_remote_ring) {
    using namespace brpc::ubring;
    IpcV2UbrTestContext context;
    ASSERT_EQ(0, context.ring.UbrPrepareIpcV2Format());
    context.trx.ubr_tx.trx_state = UBR_STATE_CONNECTED;
    context.trx.ubr_rx.trx_state = UBR_STATE_CONNECTED;

    EXPECT_EQ(UBRING_ERR, context.ring.IsUbrTrxWriteable(0));
    char value = 'x';
    struct iovec source = {&value, 1};
    errno = 0;
    EXPECT_EQ(-1, context.ring.UbrTrxWritev(&source, 1));
    EXPECT_EQ(EBADMSG, errno);
    EXPECT_EQ(0u, context.trx.ubr_tx.out_io_id);
    EXPECT_EQ(0u, context.remote_rx_event.io_id);
}

struct __attribute__((packed)) HelloMessageLayout {
    uint16_t msg_len;
    uint16_t hello_ver;
    uint16_t impl_ver;
    uint64_t len;
    char shm_name[SHM_MAX_NAME_BUFF_LEN];
};
}

class HelloMessageTest : public ::testing::Test {
protected:
    void SetUp() override {
        memset(&msg, 0, sizeof(msg));
        buffer.resize(256, 0);
    }

    brpc::ubring::HelloMessage msg;
    std::string buffer;
};

TEST(HelloFormatExtensionTest, serialize_deserialize_roundtrip) {
    brpc::ubring::HelloFormatExtension extension = {
        brpc::ubring::HelloFormatExtension::WIRE_SIZE,
        brpc::ubring::UBR_DATA_FORMAT_LEGACY_64};
    char buffer[brpc::ubring::HelloFormatExtension::WIRE_SIZE] = {};

    extension.Serialize(buffer);

    brpc::ubring::HelloFormatExtension decoded = {};
    decoded.Deserialize(buffer);
    EXPECT_EQ(extension.extension_len, decoded.extension_len);
    EXPECT_EQ(extension.format_id, decoded.format_id);

    extension.format_id = brpc::ubring::UBR_DATA_FORMAT_IPC_V2;
    extension.Serialize(buffer);
    decoded.Deserialize(buffer);
    EXPECT_EQ(extension.extension_len, decoded.extension_len);
    EXPECT_EQ(extension.format_id, decoded.format_id);
}

TEST(HelloFormatExtensionTest, serialize_uses_network_byte_order) {
    brpc::ubring::HelloFormatExtension extension = {0x0102, 0x0304};
    char buffer[brpc::ubring::HelloFormatExtension::WIRE_SIZE] = {};
    const unsigned char expected[] = {0x01, 0x02, 0x03, 0x04};

    extension.Serialize(buffer);

    EXPECT_EQ(0, memcmp(expected, buffer, sizeof(expected)));
}

TEST(HelloFormatExtensionTest, deserialize_none_format) {
    const unsigned char buffer[] = {0x00, 0x04, 0x00, 0x00};
    brpc::ubring::HelloFormatExtension extension = {};

    extension.Deserialize(buffer);

    EXPECT_EQ(4, extension.extension_len);
    EXPECT_EQ(brpc::ubring::UBR_DATA_FORMAT_NONE, extension.format_id);
}

TEST(HelloFormatExtensionTest, deserialize_unknown_format) {
    const unsigned char buffer[] = {0x00, 0x04, 0x12, 0x34};
    brpc::ubring::HelloFormatExtension extension = {};

    extension.Deserialize(buffer);

    EXPECT_EQ(4, extension.extension_len);
    EXPECT_EQ(0x1234, extension.format_id);
}

TEST(HelloFormatExtensionTest, selects_format_for_local_backend) {
    using namespace brpc::ubring;
    EXPECT_EQ(UBR_DATA_FORMAT_IPC_V2,
              PreferredDataFormatForShmType(SHM_TYPE_IPC));
    EXPECT_EQ(UBR_DATA_FORMAT_LEGACY_64,
              PreferredDataFormatForShmType(SHM_TYPE_UBS));
    EXPECT_EQ(UBR_DATA_FORMAT_NONE,
              PreferredDataFormatForShmType(SHM_TYPE_UB));
    EXPECT_EQ(UBR_DATA_FORMAT_NONE,
              PreferredDataFormatForShmType(SHM_TYPE_UNSUPPORT));
}

TEST(HelloFormatExtensionTest, selects_only_matching_supported_format) {
    using namespace brpc::ubring;
    EXPECT_EQ(UBR_DATA_FORMAT_IPC_V2,
              SelectDataFormat(UBR_DATA_FORMAT_IPC_V2,
                               UBR_DATA_FORMAT_IPC_V2));
    EXPECT_EQ(UBR_DATA_FORMAT_LEGACY_64,
              SelectDataFormat(UBR_DATA_FORMAT_LEGACY_64,
                               UBR_DATA_FORMAT_LEGACY_64));
    EXPECT_EQ(UBR_DATA_FORMAT_NONE,
              SelectDataFormat(UBR_DATA_FORMAT_IPC_V2,
                               UBR_DATA_FORMAT_LEGACY_64));
    EXPECT_EQ(UBR_DATA_FORMAT_NONE,
              SelectDataFormat(UBR_DATA_FORMAT_LEGACY_64,
                               UBR_DATA_FORMAT_IPC_V2));
    EXPECT_EQ(UBR_DATA_FORMAT_NONE,
              SelectDataFormat(UBR_DATA_FORMAT_IPC_V2, 0x1234));
    EXPECT_EQ(UBR_DATA_FORMAT_NONE,
              SelectDataFormat(UBR_DATA_FORMAT_NONE,
                               UBR_DATA_FORMAT_NONE));
    EXPECT_EQ(UBR_DATA_FORMAT_NONE,
              SelectDataFormat(static_cast<UbrDataFormat>(0x1234), 0x1234));
}

TEST_F(HelloMessageTest, serialize_deserialize_roundtrip) {
    msg.msg_len = 64;
    msg.hello_ver = 2;
    msg.impl_ver = 1;
    msg.len = 4 * 1024 * 1024;
    memcpy(msg.shm_name, "UBRING_test_C", 14);

    msg.Serialize(&buffer[0]);

    brpc::ubring::HelloMessage decoded;
    memset(&decoded, 0, sizeof(decoded));
    decoded.Deserialize(&buffer[0]);

    EXPECT_EQ(msg.msg_len, decoded.msg_len);
    EXPECT_EQ(msg.hello_ver, decoded.hello_ver);
    EXPECT_EQ(msg.impl_ver, decoded.impl_ver);
    EXPECT_EQ(msg.len, decoded.len);
    EXPECT_EQ(0, memcmp(msg.shm_name, decoded.shm_name, SHM_MAX_NAME_BUFF_LEN));
}

TEST_F(HelloMessageTest, serialize_uses_network_byte_order) {
    msg.msg_len = 0x0102;
    msg.hello_ver = 0x0304;
    msg.impl_ver = 0x0506;
    msg.len = 0x0102030405060708ULL;
    memset(msg.shm_name, 0, SHM_MAX_NAME_BUFF_LEN);

    msg.Serialize(&buffer[0]);

    HelloMessageLayout* raw = reinterpret_cast<HelloMessageLayout*>(&buffer[0]);
    EXPECT_EQ(butil::HostToNet16(0x0102), raw->msg_len);
    EXPECT_EQ(butil::HostToNet16(0x0304), raw->hello_ver);
    EXPECT_EQ(butil::HostToNet16(0x0506), raw->impl_ver);
    EXPECT_EQ(butil::HostToNet64(0x0102030405060708ULL), raw->len);
}

TEST_F(HelloMessageTest, large_len_value) {
    msg.msg_len = 64;
    msg.hello_ver = 2;
    msg.impl_ver = 1;
    msg.len = 0xFFFFFFFFFFFFFFFFULL;
    memset(msg.shm_name, 0, SHM_MAX_NAME_BUFF_LEN);

    msg.Serialize(&buffer[0]);

    brpc::ubring::HelloMessage decoded;
    memset(&decoded, 0, sizeof(decoded));
    decoded.Deserialize(&buffer[0]);

    EXPECT_EQ(0xFFFFFFFFFFFFFFFFULL, decoded.len);
}

TEST_F(HelloMessageTest, full_shm_name) {
    // A peer that fills every byte of shm_name leaves no terminator on the
    // wire. Deserialize must still hand back a valid C-string, because the
    // handshake feeds the decoded name straight into strncpy/strrchr/
    // snprintf("%s") and the shm allocate/map calls; without a terminator
    // those scan past shm_name.
    memset(msg.shm_name, 'A', SHM_MAX_NAME_BUFF_LEN);
    msg.msg_len = 64;
    msg.hello_ver = 2;
    msg.impl_ver = 1;
    msg.len = 0;
    msg.Serialize(&buffer[0]);

    brpc::ubring::HelloMessage decoded;
    memset(&decoded, 0, sizeof(decoded));
    decoded.Deserialize(&buffer[0]);

    EXPECT_EQ('\0', decoded.shm_name[SHM_MAX_NAME_BUFF_LEN - 1]);
    EXPECT_EQ(static_cast<size_t>(SHM_MAX_NAME_BUFF_LEN - 1),
              strnlen(decoded.shm_name, SHM_MAX_NAME_BUFF_LEN));
    EXPECT_EQ(0, memcmp(msg.shm_name, decoded.shm_name,
                        SHM_MAX_NAME_BUFF_LEN - 1));
}

TEST_F(HelloMessageTest, toString_contains_fields) {
    msg.msg_len = 64;
    msg.hello_ver = 2;
    msg.impl_ver = 1;
    msg.len = 4194304;
    memcpy(msg.shm_name, "UBRING_test", 12);

    std::string s = msg.toString();
    EXPECT_NE(std::string::npos, s.find("msg_len=64"));
    EXPECT_NE(std::string::npos, s.find("hello_ver=2"));
    EXPECT_NE(std::string::npos, s.find("impl_ver=1"));
    EXPECT_NE(std::string::npos, s.find("UBRING_test"));
}

TEST(UBRingConfigurationTest, time_flags_include_units_and_expected_defaults) {
    struct TimeFlagExpectation {
        const char* name;
        const char* suffix;
        const char* unit;
        const char* default_value;
    };
    const TimeFlagExpectation expected_flags[] = {
        {"ub_disconnect_timeout_s", "_s", "seconds", "5"},
        {"ub_connect_timeout_s", "_s", "seconds", "1"},
        {"ub_hb_timer_interval_s", "_s", "seconds", "5"},
        {"ub_event_queue_timer_interval_us", "_us", "microseconds", "100"},
        {"ub_event_queue_timer_interval_max_us", "_us", "microseconds", "10000"},
        {"ub_flying_io_timeout_s", "_s", "seconds", "5"},
    };

    for (const auto& expected : expected_flags) {
        GFLAGS_NAMESPACE::CommandLineFlagInfo info;
        ASSERT_TRUE(GFLAGS_NAMESPACE::GetCommandLineFlagInfo(
            expected.name, &info)) << expected.name;
        const std::string flag_name(expected.name);
        const std::string suffix(expected.suffix);
        ASSERT_GE(flag_name.size(), suffix.size());
        EXPECT_EQ(flag_name.size() - suffix.size(), flag_name.rfind(suffix));
        EXPECT_NE(std::string::npos, info.description.find(expected.unit));
        EXPECT_EQ(std::string(expected.default_value), info.default_value);
    }

    EXPECT_EQ(5, brpc::ubring::FLAGS_ub_disconnect_timeout_s);
    EXPECT_EQ(1, brpc::ubring::FLAGS_ub_connect_timeout_s);
    EXPECT_EQ(5, brpc::ubring::FLAGS_ub_hb_timer_interval_s);
    EXPECT_EQ(100, brpc::ubring::FLAGS_ub_event_queue_timer_interval_us);
    EXPECT_EQ(10000, brpc::ubring::FLAGS_ub_event_queue_timer_interval_max_us);
    EXPECT_EQ(5, brpc::ubring::FLAGS_ub_flying_io_timeout_s);
    EXPECT_EQ(100U * USEC_TO_NSEC,
              static_cast<uint32_t>(
                  brpc::ubring::FLAGS_ub_event_queue_timer_interval_us) *
                  USEC_TO_NSEC);
}

// ---------------------------------------------------------------------------
// UbrTimer: the bthread based timer facade that backs every ubring timer.
//
// The facade is asynchronous (callbacks run on the process-wide bthread timer
// thread), so every test waits for an observed state instead of sleeping for a
// fixed period, and keeps its probe alive until the callback has returned.

namespace {

using UbrTimerId = brpc::ubring::UbrTimerId;

bool WaitUbrTimerCondition(const std::function<bool()>& predicate,
                           int timeout_ms = 5000) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeout_ms);
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

struct UbrTimerProbe {
    UbrTimerProbe() : slot(nullptr), fires(0), finished(0), gen(0) {}

    butil::atomic<UbrTimerId> slot;
    std::atomic<int> fires;
    std::atomic<int> finished;
    std::atomic<uint64_t> gen;
};

void* CountUbrTimerFire(void* arg, uint64_t gen) {
    auto* probe = static_cast<UbrTimerProbe*>(arg);
    probe->gen.store(gen);
    probe->fires.fetch_add(1);
    probe->finished.fetch_add(1);
    return nullptr;
}

struct UbrTimerBackoffProbe {
    UbrTimerBackoffProbe() : calls(0) {
        for (int i = 0; i < 4; ++i) {
            seen[i].store(0);
        }
    }

    UbrTimerProbe timer;
    std::atomic<int> calls;
    std::atomic<uint64_t> seen[4];
};

uint64_t RecordUbrTimerBackoff(void* arg, uint64_t cur_interval_us) {
    auto* probe = static_cast<UbrTimerBackoffProbe*>(arg);
    const int call = probe->calls.fetch_add(1);
    if (call < 4) {
        probe->seen[call].store(cur_interval_us);
    }
    if (call == 0) {
        return cur_interval_us * 2;
    }
    if (call == 1) {
        // A broken back-off must not turn the global timer thread into a spin
        // loop: the facade keeps the previous interval instead of honouring 0.
        return 0;
    }
    if (call == 2) {
        return cur_interval_us + 1000;
    }
    return cur_interval_us;
}

struct UbrTimerBlockingProbe {
    UbrTimerBlockingProbe() : entered(0), release(0), finished(0) {}

    UbrTimerProbe timer;
    std::atomic<int> entered;
    std::atomic<int> release;
    std::atomic<int> finished;
};

void* BlockingUbrTimerFire(void* arg, uint64_t) {
    auto* probe = static_cast<UbrTimerBlockingProbe*>(arg);
    probe->entered.fetch_add(1);
    while (!probe->release.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    probe->finished.fetch_add(1);
    return nullptr;
}

struct UbrTimerSelfDeleteProbe {
    UbrTimerSelfDeleteProbe() : fires(0), del_rc(1), returned(0) {}

    UbrTimerProbe timer;
    std::atomic<int> fires;
    std::atomic<int> del_rc;
    std::atomic<int> returned;
};

void* SelfDeletingUbrTimerFire(void* arg, uint64_t) {
    auto* probe = static_cast<UbrTimerSelfDeleteProbe*>(arg);
    probe->fires.fetch_add(1);
    probe->del_rc.store(brpc::ubring::UbrTimerDel(&probe->timer.slot));
    probe->returned.fetch_add(1);
    return nullptr;
}

struct UbrTimerSelfWaitProbe {
    UbrTimerSelfWaitProbe() : fires(0), returned(0) {}

    UbrTimerProbe timer;
    std::atomic<int> fires;
    std::atomic<int> returned;
};

void* SelfWaitingUbrTimerFire(void* arg, uint64_t) {
    auto* probe = static_cast<UbrTimerSelfWaitProbe*>(arg);
    probe->fires.fetch_add(1);
    // Joining itself would deadlock the global timer thread. The facade must
    // recognize the callback's own timer and degrade to a non-blocking delete.
    brpc::ubring::UbrTimerDelAndWait(&probe->timer.slot);
    probe->returned.fetch_add(1);
    return nullptr;
}

}  // namespace

TEST(UbrTimerTest, start_rejects_invalid_arguments) {
    using namespace brpc::ubring;
    butil::atomic<UbrTimerId> slot(nullptr);

    EXPECT_EQ(UBRING_ERR,
              UbrTimerStart(nullptr, 0, CountUbrTimerFire, nullptr, 0));
    EXPECT_EQ(UBRING_ERR, UbrTimerStart(&slot, 0, nullptr, nullptr, 0));
    EXPECT_EQ(UBRING_ERR,
              UbrTimerStartPeriodic(&slot, 0, 0, CountUbrTimerFire, nullptr,
                                    0));
    EXPECT_EQ(nullptr, slot.load());
    EXPECT_EQ(1, UbrTimerDel(nullptr));
    UbrTimerDelAndWait(nullptr);
}

TEST(UbrTimerTest, one_shot_fires_once_and_retires_slot) {
    using namespace brpc::ubring;
    UbrTimerProbe probe;

    ASSERT_EQ(UBRING_OK,
              UbrTimerStart(&probe.slot, 0, CountUbrTimerFire, &probe, 42));
    ASSERT_TRUE(WaitUbrTimerCondition(
        [&probe] { return probe.finished.load() == 1; }));
    EXPECT_EQ(1, probe.fires.load());
    EXPECT_EQ(42u, probe.gen.load());
    EXPECT_EQ(nullptr, probe.slot.load());
    // The slot was already retired by the firing callback.
    EXPECT_EQ(1, UbrTimerDel(&probe.slot));
}

TEST(UbrTimerTest, one_shot_cancel_suppresses_callback) {
    using namespace brpc::ubring;
    UbrTimerProbe probe;

    ASSERT_EQ(UBRING_OK,
              UbrTimerStart(&probe.slot, 5000000, CountUbrTimerFire, &probe, 1));
    EXPECT_EQ(0, UbrTimerDel(&probe.slot));
    EXPECT_EQ(nullptr, probe.slot.load());
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_EQ(0, probe.fires.load());
}

TEST(UbrTimerTest, start_refuses_occupied_slot) {
    using namespace brpc::ubring;
    UbrTimerProbe first;
    UbrTimerProbe second;

    ASSERT_EQ(UBRING_OK,
              UbrTimerStart(&first.slot, 5000000, CountUbrTimerFire, &first, 1));
    EXPECT_EQ(UBRING_ERR,
              UbrTimerStart(&first.slot, 0, CountUbrTimerFire, &second, 2));
    EXPECT_EQ(0, second.fires.load());
    EXPECT_EQ(0, UbrTimerDel(&first.slot));
    EXPECT_EQ(nullptr, first.slot.load());
}

TEST(UbrTimerTest, periodic_rearms_until_deleted) {
    using namespace brpc::ubring;
    UbrTimerProbe probe;

    ASSERT_EQ(UBRING_OK,
              UbrTimerStartPeriodic(&probe.slot, 0, 2000, CountUbrTimerFire,
                                    &probe, 7));
    ASSERT_TRUE(WaitUbrTimerCondition(
        [&probe] { return probe.fires.load() >= 3; }));
    UbrTimerDelAndWait(&probe.slot);
    const int fired = probe.fires.load();
    EXPECT_GE(fired, 3);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_EQ(fired, probe.fires.load());
    EXPECT_EQ(nullptr, probe.slot.load());
}

TEST(UbrTimerTest, periodic_backoff_zero_keeps_previous_interval) {
    using namespace brpc::ubring;
    UbrTimerBackoffProbe probe;

    ASSERT_EQ(UBRING_OK,
              UbrTimerStartPeriodic(&probe.timer.slot, 0, 1000,
                                    CountUbrTimerFire, &probe.timer, 0,
                                    RecordUbrTimerBackoff));
    ASSERT_TRUE(WaitUbrTimerCondition(
        [&probe] { return probe.calls.load() >= 4; }));
    UbrTimerDelAndWait(&probe.timer.slot);

    EXPECT_EQ(1000u, probe.seen[0].load());
    EXPECT_EQ(2000u, probe.seen[1].load());
    // The ignored 0 keeps the 2000us interval instead of re-arming for now.
    EXPECT_EQ(2000u, probe.seen[2].load());
    EXPECT_EQ(3000u, probe.seen[3].load());
}

TEST(UbrTimerTest, periodic_callback_can_delete_itself) {
    using namespace brpc::ubring;
    UbrTimerSelfDeleteProbe probe;

    ASSERT_EQ(UBRING_OK,
              UbrTimerStartPeriodic(&probe.timer.slot, 0, 2000,
                                    SelfDeletingUbrTimerFire, &probe, 3));
    ASSERT_TRUE(WaitUbrTimerCondition(
        [&probe] { return probe.returned.load() == 1; }));
    EXPECT_EQ(0, probe.del_rc.load());
    ASSERT_TRUE(WaitUbrTimerCondition(
        [&probe] { return probe.timer.slot.load() == nullptr; }));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    // The self-delete only marks the task stopped; the callback that issued it
    // still completes, and no further fire may be scheduled.
    EXPECT_EQ(1, probe.fires.load());
}

TEST(UbrTimerTest, del_and_wait_waits_for_running_periodic_callback) {
    using namespace brpc::ubring;
    UbrTimerBlockingProbe probe;

    ASSERT_EQ(UBRING_OK,
              UbrTimerStartPeriodic(&probe.timer.slot, 0, 1000000,
                                    BlockingUbrTimerFire, &probe, 4));
    if (!WaitUbrTimerCondition(
            [&probe] { return probe.entered.load() == 1; })) {
        probe.release.store(1);
        FAIL() << "the periodic callback never started";
    }

    std::atomic<bool> done(false);
    std::thread waiter([&probe, &done] {
        brpc::ubring::UbrTimerDelAndWait(&probe.timer.slot);
        done.store(true);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    // A periodic timer stays anchored while its callback runs, so teardown can
    // wait for it before reclaiming the callback's resources.
    EXPECT_FALSE(done.load());
    probe.release.store(1);
    waiter.join();
    EXPECT_TRUE(done.load());
    EXPECT_EQ(1, probe.finished.load());
}

TEST(UbrTimerTest, del_and_wait_inside_own_callback_does_not_join_itself) {
    using namespace brpc::ubring;
    UbrTimerSelfWaitProbe probe;

    ASSERT_EQ(UBRING_OK,
              UbrTimerStartPeriodic(&probe.timer.slot, 0, 2000,
                                    SelfWaitingUbrTimerFire, &probe, 5));
    ASSERT_TRUE(WaitUbrTimerCondition(
        [&probe] { return probe.returned.load() == 1; }));
    ASSERT_TRUE(WaitUbrTimerCondition(
        [&probe] { return probe.timer.slot.load() == nullptr; }));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_EQ(1, probe.fires.load());
}

TEST(UbrTimerTest, start_and_delete_race_leaves_no_orphan_callback) {
    using namespace brpc::ubring;
    for (int i = 0; i < 100; ++i) {
        UbrTimerProbe probe;
        ASSERT_EQ(UBRING_OK,
                  UbrTimerStart(&probe.slot, 0, CountUbrTimerFire, &probe,
                                static_cast<uint64_t>(i)));
        const int del_rc = UbrTimerDel(&probe.slot);
        EXPECT_EQ(nullptr, probe.slot.load());
        if (del_rc == 0) {
            // This call won the slot: the callback must never run.
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            EXPECT_EQ(0, probe.fires.load());
        } else {
            // The firing wrapper already owns the slot and will run the
            // callback; keep the probe alive until it returns.
            ASSERT_TRUE(WaitUbrTimerCondition(
                [&probe] { return probe.finished.load() == 1; }));
        }
    }
}

// ---------------------------------------------------------------------------
// UBRingManager: pool lifetime, generation checks and teardown barriers.

namespace brpc {
namespace ubring {
namespace {

std::atomic<int> g_ub_cleanup_timer_fires(0);

void* CountCleanupTimerFire(void*, uint64_t) {
    g_ub_cleanup_timer_fires.fetch_add(1);
    return nullptr;
}

void* NoopTimerCallback(void*, uint64_t) {
    return nullptr;
}

class UBRingManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        _saved_cap = FLAGS_ubr_max_managed_num;
        // A small pool keeps the full/empty and index-reuse cases cheap.
        FLAGS_ubr_max_managed_num = 4;
        ASSERT_EQ(UBRING_OK, UBRingManager::UbrMgrInit());
        _fini_done = false;
    }

    void TearDown() override {
        if (!_fini_done) {
            UBRingManager::UbrMgrFini();
        }
        FLAGS_ubr_max_managed_num = _saved_cap;
    }

    // UbrMgrFini is terminal until the next UbrMgrInit, so tests that need to
    // observe its aftermath call it explicitly and tell TearDown not to.
    void FiniNow() {
        if (!_fini_done) {
            UBRingManager::UbrMgrFini();
            _fini_done = true;
        }
    }

    UbrTrx* Acquire() {
        UbrTrx* trx = nullptr;
        EXPECT_EQ(UBRING_OK, UBRingManager::AcquireUbrTrxFromMgr(&trx));
        return trx;
    }

    int _saved_cap = 0;
    bool _fini_done = false;
};

}  // namespace

TEST(UBRingManagerConfigTest, get_deal_msg_max_cnt_validates_arguments) {
    uint32_t count = 0;
    EXPECT_EQ(UBRING_ERR, UBRingManager::GetUbrDealMsgMaxCnt(64, nullptr));

    const int saved = FLAGS_tail_update_after_read;
    FLAGS_tail_update_after_read = 0;
    EXPECT_EQ(UBRING_ERR, UBRingManager::GetUbrDealMsgMaxCnt(64, &count));

    FLAGS_tail_update_after_read = 8;
    EXPECT_EQ(UBRING_OK, UBRingManager::GetUbrDealMsgMaxCnt(64, &count));
    EXPECT_EQ(8u, count);

    FLAGS_tail_update_after_read = 3;
    EXPECT_EQ(UBRING_OK, UBRingManager::GetUbrDealMsgMaxCnt(10, &count));
    EXPECT_EQ(3u, count);
    FLAGS_tail_update_after_read = saved;
}

// A manager that was never initialized (or already finished) must reject
// every entry point instead of walking null pool arrays.
TEST(UBRingManagerStateTest, operations_fail_while_shut_down) {
    UBRingManager::UbrMgrFini();
    UbrTrx* trx = nullptr;
    EXPECT_EQ(UBRING_ERR, UBRingManager::AcquireUbrTrxFromMgr(&trx));
    EXPECT_EQ(UBRING_ERR, UBRingManager::AcquireUbrTrxFromMgr(nullptr));
    EXPECT_EQ(UBRING_ERR, UBRingManager::ReleaseUbrTrxFromMgr(nullptr, 0));
    EXPECT_FALSE(UBRingManager::IsUbrTrxSlotUsed(0, 0));
    EXPECT_FALSE(UBRingManager::BeginPoolAccess());
    UBRingManager::FinishPoolAccess();
    bool armed = false;
    EXPECT_FALSE(UBRingManager::ArmTimersExclusive([&armed] {
        armed = true;
        return true;
    }));
    EXPECT_FALSE(armed);

    UbrCleanupCtl* ctl = nullptr;
    EXPECT_EQ(UBR_CLEANUP_CLAIM_NOT_OURS,
              UBRingManager::ClaimTrxCleanupForced(0, 0, &ctl));
    EXPECT_EQ(nullptr, ctl);
    EXPECT_FALSE(UBRingManager::TryPublishUnitCleanupCtl(0, 0, nullptr));
    EXPECT_FALSE(UBRingManager::DetachUnitCleanupCtl(0, nullptr));
}

TEST_F(UBRingManagerTest, acquire_release_and_generation_reuse) {
    UbrTrx* first = Acquire();
    ASSERT_NE(nullptr, first);
    const uint32_t idx = first->trx_mgr_index;
    const uint64_t first_id = first->ubr_id.load();
    EXPECT_TRUE(UBRingManager::IsUbrTrxSlotUsed(idx, first_id));
    ASSERT_EQ(UBRING_OK,
              UBRingManager::ReleaseUbrTrxFromMgr(first, first_id));
    EXPECT_FALSE(UBRingManager::IsUbrTrxSlotUsed(idx, first_id));

    UbrTrx* second = Acquire();
    ASSERT_NE(nullptr, second);
    const uint64_t second_id = second->ubr_id.load();
    EXPECT_EQ(idx, second->trx_mgr_index);
    EXPECT_GT(second_id, first_id);

    // A stale release carrying the previous generation's id must leave the
    // new occupant of the reused slot alone.
    EXPECT_EQ(UBRING_OK,
              UBRingManager::ReleaseUbrTrxFromMgr(first, first_id));
    EXPECT_TRUE(UBRingManager::IsUbrTrxSlotUsed(idx, second_id));
    EXPECT_EQ(UBRING_OK,
              UBRingManager::ReleaseUbrTrxFromMgr(second, second_id));
    EXPECT_FALSE(UBRingManager::IsUbrTrxSlotUsed(idx, second_id));
}

TEST_F(UBRingManagerTest, release_is_idempotent) {
    UbrTrx* trx = Acquire();
    ASSERT_NE(nullptr, trx);
    const uint64_t id = trx->ubr_id.load();
    ASSERT_EQ(UBRING_OK, UBRingManager::ReleaseUbrTrxFromMgr(trx, id));
    // The second release sees a FREE slot and succeeds without double
    // decrementing the pool count.
    EXPECT_EQ(UBRING_OK, UBRingManager::ReleaseUbrTrxFromMgr(trx, id));
}

TEST_F(UBRingManagerTest, pool_rejects_acquire_when_full) {
    UbrTrx* slots[4] = {};
    for (int i = 0; i < 4; ++i) {
        slots[i] = Acquire();
        ASSERT_NE(nullptr, slots[i]);
    }
    UbrTrx* extra = nullptr;
    EXPECT_EQ(UBRING_ERR, UBRingManager::AcquireUbrTrxFromMgr(&extra));
    EXPECT_EQ(nullptr, extra);

    ASSERT_EQ(UBRING_OK,
              UBRingManager::ReleaseUbrTrxFromMgr(
                  slots[0], slots[0]->ubr_id.load()));
    slots[0] = Acquire();
    ASSERT_NE(nullptr, slots[0]);
    for (int i = 0; i < 4; ++i) {
        ASSERT_EQ(UBRING_OK,
                  UBRingManager::ReleaseUbrTrxFromMgr(
                      slots[i], slots[i]->ubr_id.load()));
    }
}

TEST_F(UBRingManagerTest, try_claim_close_checks_generation_and_queue) {
    UbrTrx* trx = Acquire();
    ASSERT_NE(nullptr, trx);
    const uint32_t idx = trx->trx_mgr_index;
    const uint64_t id = trx->ubr_id.load();

    // Without a mapped tx event queue the close cannot be claimed, and the
    // rejected attempt must leave the counter intact.
    EXPECT_EQ(UBRING_ERR, UBRingManager::TryClaimTrxClose(idx, id));
    EXPECT_EQ(MAX_CLOSE_COUNT, trx->close_cnt.load());

    UbrEventQMsg event = {};
    trx->ubr_tx.local_tx_event_q.addr =
        reinterpret_cast<uint8_t*>(&event);
    EXPECT_EQ(UBRING_OK, UBRingManager::TryClaimTrxClose(idx, id));
    EXPECT_EQ(MAX_CLOSE_COUNT - 1, trx->close_cnt.load());
    // The same generation is already closing.
    EXPECT_EQ(UBRING_REENTRY, UBRingManager::TryClaimTrxClose(idx, id));
    // Out-of-range index and a stale generation are both refused.
    EXPECT_EQ(UBRING_ERR, UBRingManager::TryClaimTrxClose(idx + 1, id));
    EXPECT_EQ(UBRING_ERR, UBRingManager::TryClaimTrxClose(idx, id + 1));
    ASSERT_EQ(UBRING_OK, UBRingManager::ReleaseUbrTrxFromMgr(trx, id));
}

TEST_F(UBRingManagerTest, cleanup_ctl_publish_claim_and_detach) {
    UbrTrx* trx = Acquire();
    ASSERT_NE(nullptr, trx);
    const uint32_t idx = trx->trx_mgr_index;
    const uint64_t id = trx->ubr_id.load();

    UbrCleanupCtl* ctl = new UbrCleanupCtl();
    ctl->trx = trx;
    ctl->ubr_id = id;
    ctl->state.store(UBR_CLEANUP_PENDING);
    ctl->timer = nullptr;
    ctl->ref.store(1);

    // A wrong generation must not publish or anchor the control object.
    EXPECT_FALSE(UBRingManager::TryPublishUnitCleanupCtl(idx, id + 1, ctl));
    EXPECT_EQ(1, ctl->ref.load());
    ASSERT_TRUE(UBRingManager::TryPublishUnitCleanupCtl(idx, id, ctl));
    EXPECT_EQ(2, ctl->ref.load());          // manager anchor
    EXPECT_EQ(ctl, trx->cleanup_ctl.load());
    // A second publication for the same generation is refused.
    UbrCleanupCtl* other = new UbrCleanupCtl();
    other->trx = trx;
    other->ubr_id = id;
    other->state.store(UBR_CLEANUP_PENDING);
    other->timer = nullptr;
    other->ref.store(1);
    EXPECT_FALSE(UBRingManager::TryPublishUnitCleanupCtl(idx, id, other));
    EXPECT_EQ(1, other->ref.load());
    other->ReleaseRef();

    UbrCleanupCtl* snapshot = nullptr;
    EXPECT_EQ(UBR_CLEANUP_CLAIM_HAS_CTL,
              UBRingManager::ClaimTrxCleanupForced(idx, id, &snapshot));
    EXPECT_EQ(ctl, snapshot);
    EXPECT_EQ(3, ctl->ref.load());          // snapshot reference
    snapshot->ReleaseRef();
    EXPECT_EQ(2, ctl->ref.load());

    EXPECT_TRUE(UBRingManager::DetachUnitCleanupCtl(idx, ctl));
    EXPECT_EQ(1, ctl->ref.load());          // manager anchor released
    EXPECT_EQ(nullptr, trx->cleanup_ctl.load());
    // Detaching twice is a no-op and must not release another reference.
    EXPECT_FALSE(UBRingManager::DetachUnitCleanupCtl(idx, ctl));
    EXPECT_EQ(1, ctl->ref.load());
    ctl->ReleaseRef();
    ASSERT_EQ(UBRING_OK, UBRingManager::ReleaseUbrTrxFromMgr(trx, id));
}

TEST_F(UBRingManagerTest, forced_claim_owns_cleanup_without_ctl) {
    UbrTrx* trx = Acquire();
    ASSERT_NE(nullptr, trx);
    const uint32_t idx = trx->trx_mgr_index;
    const uint64_t id = trx->ubr_id.load();

    UbrCleanupCtl* snapshot = nullptr;
    EXPECT_EQ(UBR_CLEANUP_CLAIM_OWNED_NULL,
              UBRingManager::ClaimTrxCleanupForced(idx, id, &snapshot));
    EXPECT_EQ(nullptr, snapshot);
    EXPECT_TRUE(trx->cleanup_forced.load());

    // The force close owns the cleanup now: a later delayed cleanup must be
    // refused instead of running the same teardown a second time.
    UbrCleanupCtl* ctl = new UbrCleanupCtl();
    ctl->trx = trx;
    ctl->ubr_id = id;
    ctl->state.store(UBR_CLEANUP_PENDING);
    ctl->timer = nullptr;
    ctl->ref.store(1);
    EXPECT_FALSE(UBRingManager::TryPublishUnitCleanupCtl(idx, id, ctl));
    EXPECT_EQ(1, ctl->ref.load());
    ctl->ReleaseRef();

    // A stale index/generation is never claimed.
    EXPECT_EQ(UBR_CLEANUP_CLAIM_NOT_OURS,
              UBRingManager::ClaimTrxCleanupForced(idx + 1, id, &snapshot));
    ASSERT_EQ(UBRING_OK, UBRingManager::ReleaseUbrTrxFromMgr(trx, id));
    EXPECT_EQ(UBR_CLEANUP_CLAIM_NOT_OURS,
              UBRingManager::ClaimTrxCleanupForced(idx, id, &snapshot));
    EXPECT_EQ(nullptr, snapshot);
}

TEST_F(UBRingManagerTest, begin_and_finish_pool_access) {
    EXPECT_TRUE(UBRingManager::BeginPoolAccess());
    EXPECT_TRUE(UBRingManager::BeginPoolAccess());
    UBRingManager::FinishPoolAccess();
    UBRingManager::FinishPoolAccess();
    // Unbalanced finishes must not underflow the counter and make a later
    // UbrMgrFini return before the accepted access did.
    UBRingManager::FinishPoolAccess();
    bool armed = false;
    EXPECT_TRUE(UBRingManager::ArmTimersExclusive([&armed] {
        armed = true;
        return true;
    }));
    EXPECT_TRUE(armed);
}

TEST_F(UBRingManagerTest, fini_waits_for_accepted_pool_access) {
    ASSERT_TRUE(UBRingManager::BeginPoolAccess());
    std::atomic<bool> fini_done(false);
    std::thread fini([&fini_done] {
        UBRingManager::UbrMgrFini();
        fini_done.store(true);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(fini_done.load());
    UBRingManager::FinishPoolAccess();
    fini.join();
    EXPECT_TRUE(fini_done.load());
    _fini_done = true;
    // Once the shutdown flag is set, no new callback-driven access is
    // accepted, and the pool is gone.
    EXPECT_FALSE(UBRingManager::BeginPoolAccess());
    UBRingManager::FinishPoolAccess();
    EXPECT_EQ(nullptr, UBRingManager::g_ubr_mgr.trx_mgr);
    EXPECT_EQ(0u, UBRingManager::g_ubr_mgr.trx_cap);
}

TEST_F(UBRingManagerTest, fini_cancels_per_trx_timers) {
    UbrTrx* trx = Acquire();
    ASSERT_NE(nullptr, trx);
    const uint64_t id = trx->ubr_id.load();
    ASSERT_EQ(UBRING_OK, UbrTimerStartPeriodic(&trx->close_timer, 0,
                                               1000000, NoopTimerCallback,
                                               trx, id));
    ASSERT_EQ(UBRING_OK, UbrTimerStartPeriodic(&trx->hb_timer, 0, 1000000,
                                               NoopTimerCallback, trx, id));

    // UbrMgrFini promises to stop the timers it snapshotted before freeing
    // the pool they point into, so no callback may fire after it returns.
    FiniNow();
    EXPECT_EQ(nullptr, UBRingManager::g_ubr_mgr.trx_mgr);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_EQ(nullptr, UBRingManager::g_ubr_mgr.trx_mgr_unit_status);
}

TEST_F(UBRingManagerTest, fini_cancels_published_cleanup) {
    UbrTrx* trx = Acquire();
    ASSERT_NE(nullptr, trx);
    const uint32_t idx = trx->trx_mgr_index;
    const uint64_t id = trx->ubr_id.load();

    g_ub_cleanup_timer_fires.store(0);
    UbrCleanupCtl* ctl = new UbrCleanupCtl();
    ctl->trx = trx;
    ctl->ubr_id = id;
    ctl->state.store(UBR_CLEANUP_PENDING);
    ctl->timer = nullptr;
    // Timer/callback + starter, mirroring UbrScheduleClearTimer before it
    // hands the starter reference back.
    ctl->ref.store(2);
    ASSERT_TRUE(UBRingManager::TryPublishUnitCleanupCtl(idx, id, ctl));
    ASSERT_EQ(UBRING_OK, UbrTimerStart(&ctl->timer, 5000000,
                                       CountCleanupTimerFire, nullptr, 0));
    ctl->ReleaseRef();                       // starter reference

    FiniNow();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_EQ(0, g_ub_cleanup_timer_fires.load());
    EXPECT_EQ(nullptr, UBRingManager::g_ubr_mgr.trx_mgr_unit_ctl);
}

TEST_F(UBRingManagerTest, link_info_acquire_and_release) {
    UbrTrx* trx = Acquire();
    ASSERT_NE(nullptr, trx);
    strncpy(trx->local_shm.name, "UBRING_link_ut",
            SHM_MAX_NAME_BUFF_LEN - 1);
    ASSERT_EQ(0u, UBRingManager::g_link_info_mgr.link_num);

    UBRingManager::AcquireLinkInfoToMgr(nullptr, trx);
    UBRingManager::AcquireLinkInfoToMgr("listener", nullptr);
    EXPECT_EQ(0u, UBRingManager::g_link_info_mgr.link_num);
    UBRingManager::AcquireLinkInfoToMgr("listener", trx);
    EXPECT_EQ(1u, UBRingManager::g_link_info_mgr.link_num);
    // A used slot is not overwritten by another listener.
    UBRingManager::AcquireLinkInfoToMgr("listener2", trx);
    EXPECT_EQ(1u, UBRingManager::g_link_info_mgr.link_num);

    UBRingManager::ReleaseLinkInfoFromMgr(nullptr);
    UBRingManager::ReleaseLinkInfoFromMgr(trx);
    EXPECT_EQ(0u, UBRingManager::g_link_info_mgr.link_num);
    // Releasing an already free link must not decrement below zero.
    UBRingManager::ReleaseLinkInfoFromMgr(trx);
    EXPECT_EQ(0u, UBRingManager::g_link_info_mgr.link_num);
    ASSERT_EQ(UBRING_OK, UBRingManager::ReleaseUbrTrxFromMgr(
                             trx, trx->ubr_id.load()));
}

TEST_F(UBRingManagerTest, ub_event_callback_closes_matching_generation) {
    UbrTrx* trx = Acquire();
    ASSERT_NE(nullptr, trx);
    const uint64_t id = trx->ubr_id.load();
    strncpy(trx->local_shm.name, "UBRING_event_ut",
            SHM_MAX_NAME_BUFF_LEN - 1);
    UbrEventQMsg local_tx_event = {};
    trx->ubr_tx.local_tx_event_q.addr =
        reinterpret_cast<uint8_t*>(&local_tx_event);
    // No local_shm.addr: the passive clear claims the close but schedules no
    // cleanup job, keeping this test independent of the cleanup worker.

    EXPECT_EQ(UBRING_ERR, UBRingManager::UbEventCallback(nullptr));
    EXPECT_EQ(UBRING_ERR,
              UBRingManager::UbEventCallback("UBRING_missing_ut"));
    EXPECT_EQ(UBRING_OK,
              UBRingManager::UbEventCallback("UBRING_event_ut"));
    EXPECT_EQ(MAX_CLOSE_COUNT - 1, trx->close_cnt.load());
    EXPECT_EQ(UBR_STATE_CLOSED, trx->ubr_tx.trx_state);
    EXPECT_EQ(UBR_STATE_CLOSED, trx->ubr_rx.trx_state);
    ASSERT_EQ(UBRING_OK, UBRingManager::ReleaseUbrTrxFromMgr(trx, id));
}

}  // namespace ubring
}  // namespace brpc

// ---------------------------------------------------------------------------
// SHM manager and the IPC backend: these exercise the real /dev/shm objects,
// so every name carries the pid and a sequence and is unlinked in TearDown.

namespace brpc {
namespace ubring {
namespace {

class ShmIpcTest : public ::testing::Test {
protected:
    void SetUp() override {
        _saved_type = FLAGS_ub_shm_type;
        FLAGS_ub_shm_type = SHM_TYPE_IPC;
        ASSERT_EQ(UBRING_OK, ShmMgrInit());
    }

    void TearDown() override {
        for (const auto& name : _names) {
            shm_unlink(name.c_str());
        }
        ShmMgrFini();
        FLAGS_ub_shm_type = _saved_type;
        SetShmType(SHM_TYPE_IPC);
    }

    static std::string Name(const char* tag) {
        static std::atomic<int> sequence(0);
        return std::string("UBRING_ut_") + tag + "_" +
               std::to_string(getpid()) + "_" +
               std::to_string(sequence.fetch_add(1));
    }

    // `len` must be a multiple of the 4MB allocation unit. `unlink_first`
    // removes a leftover object of the same name; pass false to look up an
    // object a previous call created.
    SHM MakeShm(const std::string& name,
                size_t len = SHM_ALLOC_UNIT_SIZE,
                bool unlink_first = true) {
        SHM shm = {};
        shm.len = len;
        snprintf(shm.name, sizeof(shm.name), "%s", name.c_str());
        if (unlink_first) {
            shm_unlink(name.c_str());
        }
        _names.push_back(name);
        return shm;
    }

    static void ExpectInvalidInput(SHM* shm) {
        EXPECT_EQ(SHM_ERR_INPUT_INVALID, ShmLocalMalloc(shm));
        EXPECT_EQ(SHM_ERR_INPUT_INVALID, ShmLocalCalloc(shm));
        EXPECT_EQ(SHM_ERR_INPUT_INVALID, ShmLocalFree(shm));
        EXPECT_EQ(SHM_ERR_INPUT_INVALID, ShmRemoteMalloc(shm));
        EXPECT_EQ(SHM_ERR_INPUT_INVALID, ShmRemoteFree(shm));
        EXPECT_EQ(SHM_ERR_INPUT_INVALID, ShmLocalMmap(shm, PROT_READ));
        EXPECT_EQ(SHM_ERR_INPUT_INVALID, ShmMunmap(shm));
        EXPECT_EQ(SHM_ERR_INPUT_INVALID, ShmFree(shm));
    }

    int _saved_type = SHM_TYPE_IPC;
    std::vector<std::string> _names;
};

}  // namespace

TEST_F(ShmIpcTest, init_validates_shm_type) {
    const int saved = FLAGS_ub_shm_type;

    FLAGS_ub_shm_type = SHM_TYPE_UB;
    EXPECT_EQ(UBRING_ERR, ShmMgrInit());
    EXPECT_EQ(SHM_TYPE_IPC, GetShmType());   // unchanged on rejection
    FLAGS_ub_shm_type = SHM_TYPE_UNSUPPORT;
    EXPECT_EQ(UBRING_ERR, ShmMgrInit());

    FLAGS_ub_shm_type = SHM_TYPE_IPC;
    EXPECT_EQ(UBRING_OK, ShmMgrInit());
    EXPECT_EQ(SHM_TYPE_IPC, GetShmType());
    FLAGS_ub_shm_type = saved;
}

TEST_F(ShmIpcTest, rejects_invalid_parameters) {
    ExpectInvalidInput(nullptr);

    SHM empty_name = {};
    empty_name.len = SHM_ALLOC_UNIT_SIZE;
    ExpectInvalidInput(&empty_name);

    SHM bad_len = MakeShm(Name("bad_len"));
    bad_len.len = 0;
    ExpectInvalidInput(&bad_len);
    bad_len.len = SHM_ALLOC_UNIT_SIZE - 1;
    ExpectInvalidInput(&bad_len);
    bad_len.len = SHM_ALLOC_UNIT_SIZE + 1;
    ExpectInvalidInput(&bad_len);
    bad_len.len = SHM_ALLOC_UNIT_SIZE * 3 / 2;
    ExpectInvalidInput(&bad_len);
}

TEST_F(ShmIpcTest, local_malloc_is_exclusive_and_free_unlinks) {
    const std::string name = Name("local");
    SHM shm = MakeShm(name);
    ASSERT_EQ(UBRING_OK, ShmLocalMalloc(&shm));
    ASSERT_NE(nullptr, shm.addr);
    memset(shm.addr, 0x5a, 64);

    // O_EXCL: a second owner of the same name is refused and leaves the
    // untouched object in place.
    SHM duplicate = MakeShm(name, SHM_ALLOC_UNIT_SIZE, false);
    EXPECT_EQ(SHM_ERR_EXIST, ShmLocalMalloc(&duplicate));
    EXPECT_EQ(nullptr, duplicate.addr);

    EXPECT_EQ(UBRING_OK, ShmLocalFree(&shm));
    EXPECT_EQ(nullptr, shm.addr);
    EXPECT_EQ(SHM_ERR_NOT_FOUND, ShmLocalFree(&shm));
}

TEST_F(ShmIpcTest, local_calloc_zeroes_reused_name) {
    const std::string name = Name("calloc");
    SHM first = MakeShm(name);
    ASSERT_EQ(UBRING_OK, ShmLocalMalloc(&first));
    ASSERT_NE(nullptr, first.addr);
    memset(first.addr, 0xff, 128);
    ASSERT_EQ(UBRING_OK, ShmLocalFree(&first));

    SHM second = MakeShm(name, SHM_ALLOC_UNIT_SIZE, false);
    ASSERT_EQ(UBRING_OK, ShmLocalCalloc(&second));
    ASSERT_NE(nullptr, second.addr);
    for (int i = 0; i < 128; ++i) {
        ASSERT_EQ(0, second.addr[i]) << "byte " << i;
    }
    EXPECT_EQ(UBRING_OK, ShmLocalFree(&second));
}

TEST_F(ShmIpcTest, remote_mapping_shares_local_pages) {
    const std::string name = Name("remote");
    SHM local = MakeShm(name);
    ASSERT_EQ(UBRING_OK, ShmLocalMalloc(&local));
    ASSERT_NE(nullptr, local.addr);
    strcpy(reinterpret_cast<char*>(local.addr), "shared");

    SHM remote = MakeShm(name, SHM_ALLOC_UNIT_SIZE, false);
    ASSERT_EQ(UBRING_OK, ShmRemoteMalloc(&remote));
    ASSERT_NE(nullptr, remote.addr);
    EXPECT_STREQ("shared", reinterpret_cast<char*>(remote.addr));

    // MAP_SHARED: writes through the remote mapping are visible locally.
    strcpy(reinterpret_cast<char*>(remote.addr) + 8, "back");
    EXPECT_STREQ("back", reinterpret_cast<char*>(local.addr) + 8);
    EXPECT_EQ(UBRING_OK, ShmRemoteFree(&remote));
    EXPECT_EQ(nullptr, remote.addr);
    EXPECT_EQ(UBRING_OK, ShmRemoteFree(&remote));   // already unmapped
    EXPECT_EQ(UBRING_OK, ShmLocalFree(&local));
}

TEST_F(ShmIpcTest, remote_malloc_rejects_missing_and_short_shm) {
    SHM missing = MakeShm(Name("missing"));
    EXPECT_EQ(SHM_ERR, ShmRemoteMalloc(&missing));
    EXPECT_EQ(nullptr, missing.addr);

    const std::string name = Name("short");
    SHM local = MakeShm(name);
    ASSERT_EQ(UBRING_OK, ShmLocalMalloc(&local));

    // The object is 4MB but the peer asks for 8MB: refuse before mapping.
    SHM too_large = MakeShm(name, SHM_ALLOC_UNIT_SIZE * 2, false);
    EXPECT_EQ(SHM_ERR, ShmRemoteMalloc(&too_large));
    EXPECT_EQ(nullptr, too_large.addr);
    EXPECT_EQ(UBRING_OK, ShmLocalFree(&local));
}

TEST_F(ShmIpcTest, local_mmap_read_only_and_munmap_twice) {
    const std::string name = Name("mmap");
    SHM writer = MakeShm(name);
    ASSERT_EQ(UBRING_OK, ShmLocalMalloc(&writer));
    strcpy(reinterpret_cast<char*>(writer.addr), "readonly");

    SHM reader = MakeShm(name, SHM_ALLOC_UNIT_SIZE, false);
    ASSERT_EQ(UBRING_OK, ShmLocalMmap(&reader, PROT_READ));
    ASSERT_NE(nullptr, reader.addr);
    EXPECT_STREQ("readonly", reinterpret_cast<char*>(reader.addr));
    EXPECT_EQ(UBRING_OK, ShmMunmap(&reader));
    EXPECT_EQ(nullptr, reader.addr);
    EXPECT_EQ(UBRING_OK, ShmMunmap(&reader));

    SHM missing = MakeShm(Name("mmap_missing"));
    EXPECT_EQ(SHM_ERR, ShmLocalMmap(&missing, PROT_READ));
    EXPECT_EQ(nullptr, missing.addr);
    EXPECT_EQ(UBRING_OK, ShmLocalFree(&writer));
}

TEST_F(ShmIpcTest, free_unlinks_the_object_and_reports_missing) {
    const std::string name = Name("free");
    SHM shm = MakeShm(name);
    ASSERT_EQ(UBRING_OK, ShmLocalMalloc(&shm));
    ASSERT_EQ(UBRING_OK, ShmFree(&shm));
    EXPECT_EQ(UBRING_OK, ShmMunmap(&shm));
    // The object is gone, but the SHM is still structurally valid.
    EXPECT_EQ(SHM_ERR_NOT_FOUND, ShmFree(&shm));
    EXPECT_EQ(nullptr, shm.addr);
}

TEST_F(ShmIpcTest, unsupported_type_is_rejected_by_every_entry_point) {
    SetShmType(SHM_TYPE_UB);
    SHM shm = MakeShm(Name("unsupported"));
    EXPECT_EQ(SHM_ERR, ShmLocalMalloc(&shm));
    EXPECT_EQ(SHM_ERR, ShmLocalCalloc(&shm));
    EXPECT_EQ(SHM_ERR, ShmLocalFree(&shm));
    EXPECT_EQ(SHM_ERR, ShmRemoteMalloc(&shm));
    EXPECT_EQ(SHM_ERR, ShmRemoteFree(&shm));
    EXPECT_EQ(SHM_ERR, ShmLocalMmap(&shm, PROT_READ));
    EXPECT_EQ(SHM_ERR, ShmMunmap(&shm));
    EXPECT_EQ(SHM_ERR, ShmFree(&shm));
    SetShmType(SHM_TYPE_IPC);
}

}  // namespace ubring
}  // namespace brpc

// ---------------------------------------------------------------------------
// UBRing: the pure ring logic, driven from fabricated shared-memory buffers.
// These tests never need a UB device: the data and event queues are ordinary
// heap buffers mapped through UbrTrxMapShm, exactly like the IPC backend maps
// /dev/shm, so the legacy data path and every guard can be exercised.

namespace brpc {
namespace ubring {
namespace {

class UBRingTest : public ::testing::Test {
protected:
    static constexpr size_t kBufferSize = 64 * 1024;

    UBRingTest() : _local(kBufferSize, 0), _remote(kBufferSize, 0) {
        _ring.SetTrxForTest(&_trx);
    }

    void MapShm() {
        SHM local = {_local.data(), _local.size(), 0, "local", 0};
        SHM remote = {_remote.data(), _remote.size(), 0, "remote", 0};
        ASSERT_EQ(UBRING_OK, _ring.UbrTrxMapShm(&local, &remote));
    }

    // Initialize the legacy layout and mark both ends connected, which is the
    // state the send/receive paths expect.
    void InitLegacy() {
        MapShm();
        ASSERT_EQ(UBRING_OK, _ring.InitializeServerLegacyFormat());
        _trx.data_format = UBR_DATA_FORMAT_LEGACY_64;
        _trx.ubr_tx.trx_state = UBR_STATE_CONNECTED;
        _trx.ubr_rx.trx_state = UBR_STATE_CONNECTED;
    }

    UbrMsgFormat* LocalChunk(uint32_t pos) {
        return reinterpret_cast<UbrMsgFormat*>(
                   _trx.ubr_rx.local_data_q.addr) + pos;
    }

    UbrMsgFormat* RemoteChunk(uint32_t pos) {
        return reinterpret_cast<UbrMsgFormat*>(
                   _trx.ubr_tx.remote_data_q.addr) + pos;
    }

    static void MakeChunk(UbrMsgFormat* chunk, UbrMsgHdrFlag flag,
                          uint8_t len, uint8_t cur_index, char value) {
        chunk->header[UBR_MSG_FLAG_INDEX] = static_cast<uint8_t>(flag);
        chunk->header[UBR_MSG_LEN_INDEX] = len;
        chunk->header[UBR_MSG_CUR_INDEX] = cur_index;
        memset(chunk->payload.inner, value, sizeof(chunk->payload.inner));
    }

    std::vector<uint8_t> _local;
    std::vector<uint8_t> _remote;
    UbrTrx _trx{};
    UBRing _ring;
};

}  // namespace

TEST_F(UBRingTest, guards_reject_null_and_unconnected_trx) {
    using namespace brpc::ubring;

    EXPECT_EQ(UBRING_ERR, UBRing::CheckTrxConnectParam(nullptr, "local"));
    EXPECT_EQ(UBRING_ERR, UBRing::CheckTrxConnectParam("listener", nullptr));
    EXPECT_EQ(UBRING_OK, UBRing::CheckTrxConnectParam("listener", "local"));

    EXPECT_EQ(UBRING_ERR, UBRing::CheckTrxSendPreCheck(nullptr));
    EXPECT_EQ(UBRING_ERR, UBRing::CheckTrxRecvPreCheck(&_trx));
    _trx.ubr_tx.trx_state = UBR_STATE_CONNECTED;
    EXPECT_EQ(UBRING_OK, UBRing::CheckTrxSendPreCheck(&_trx));
    _trx.ubr_tx.trx_state = UBR_STATE_CLOSING;
    EXPECT_EQ(UBRING_ERR, UBRing::CheckTrxSendPreCheck(&_trx));
    _trx.ubr_tx.trx_state = UBR_STATE_NONE;

    char data = 0;
    EXPECT_EQ(UBRING_ERR, UBRing::CheckTrxRecvParam(nullptr, &data, 1));
    EXPECT_EQ(UBRING_ERR,
              UBRing::CheckTrxRecvParam(&_trx, nullptr, 1));
    EXPECT_EQ(UBRING_ERR, UBRing::CheckTrxRecvParam(&_trx, &data, 0));
    // No local event queue mapped yet.
    EXPECT_EQ(UBRING_ERR, UBRing::CheckTrxRecvParam(&_trx, &data, 1));
    _trx.ubr_rx.local_rx_event_q.addr =
        reinterpret_cast<uint8_t*>(&_trx.ubr_rx.local_rx_event_q);
    EXPECT_EQ(UBR_NOT_CONNECTED,
              UBRing::CheckTrxRecvParam(&_trx, &data, 1));
    _trx.ubr_rx.trx_state = UBR_STATE_CONNECTED;
    EXPECT_EQ(UBRING_ERR, UBRing::CheckTrxRecvParam(&_trx, nullptr, 1));
    EXPECT_EQ(UBRING_ERR, UBRing::CheckTrxRecvParam(&_trx, &data, 0));
    EXPECT_EQ(UBRING_OK, UBRing::CheckTrxRecvParam(&_trx, &data, 1));

    EXPECT_EQ(UBRING_ERR, UBRing::UbrTrxCallbackCheck(nullptr));
    UbrTrx empty{};
    EXPECT_EQ(UBRING_ERR, UBRing::UbrTrxCallbackCheck(&empty));
    char dummy = 0;
    empty.local_shm.addr = reinterpret_cast<uint8_t*>(&dummy);
    EXPECT_EQ(UBRING_ERR, UBRing::UbrTrxCallbackCheck(&empty));
    empty.ubr_rx.local_rx_event_q.addr =
        reinterpret_cast<uint8_t*>(&dummy);
    EXPECT_EQ(UBRING_ERR, UBRing::UbrTrxCallbackCheck(&empty));
    empty.ubr_tx.local_tx_event_q.addr =
        reinterpret_cast<uint8_t*>(&dummy);
    EXPECT_EQ(UBRING_OK, UBRing::UbrTrxCallbackCheck(&empty));
}

TEST_F(UBRingTest, empty_ring_rejects_data_operations) {
    using namespace brpc::ubring;
    // A ring that was never bound to a transaction.
    UBRing null_ring;
    char data = 0;
    errno = 0;
    EXPECT_EQ(UBRING_ERR, null_ring.UbrTrxSend(&data, 1));
    EXPECT_EQ(EPROTONOSUPPORT, errno);
    errno = 0;
    EXPECT_EQ(UBRING_ERR, null_ring.UbrTrxRecv(&data, 1));
    EXPECT_EQ(EPROTONOSUPPORT, errno);
    EXPECT_EQ(UBRING_ERR, null_ring.UbrTrxWritev(nullptr, 0));
    EXPECT_EQ(UBRING_ERR, null_ring.UbrTrxReadv(nullptr, 0));
    EXPECT_EQ(UBRING_ERR, null_ring.UbrTrxClose());
    EXPECT_EQ(UBRING_ERR, null_ring.UbrUnlinkLocalShm());
    EXPECT_EQ(UBRING_ERR, UBRing::UbrTrxFreeShm(nullptr));
    EXPECT_EQ(UBRING_ERR, null_ring.UbrAddTimer());
    EXPECT_EQ(UBRING_ERR, null_ring.UbrAddCloseTimer());
    EXPECT_EQ(UBRING_ERR, null_ring.UbrAddHBTimer());
    EXPECT_EQ(-1, null_ring.UbrPrepareIpcV2Format());
    EXPECT_EQ(UBR_DATA_FORMAT_NONE, null_ring.data_format());

    // A bound transaction without a mapped shared memory has nothing to
    // unlink, and cannot prepare a queue either.
    EXPECT_EQ(SHM_ERR_INPUT_INVALID, _ring.UbrUnlinkLocalShm());
    EXPECT_EQ(-1, _ring.UbrPrepareIpcV2Format());
}

TEST_F(UBRingTest, map_shm_builds_aligned_queues) {
    MapShm();

    EXPECT_EQ(_local.data(), _trx.local_shm.addr);
    EXPECT_EQ(_remote.data(), _trx.remote_shm.addr);
    EXPECT_EQ(0u, reinterpret_cast<uintptr_t>(
                      _trx.ubr_rx.local_data_q.addr) % 64);
    EXPECT_EQ(0u, reinterpret_cast<uintptr_t>(
                      _trx.ubr_tx.remote_data_q.addr) % 64);
    EXPECT_EQ(_local.data() + TX_EVENTQ_ADDR_OFFSET,
              _trx.ubr_tx.local_tx_event_q.addr);
    EXPECT_EQ(_local.data() + RX_EVENTQ_ADDR_OFFSET,
              _trx.ubr_rx.local_rx_event_q.addr);
    EXPECT_EQ(_local.data() + DATASTATUSQ_ADDR_OFFSET,
              _trx.ubr_tx.local_data_status_q.addr);
    EXPECT_EQ(_remote.data() + TX_EVENTQ_ADDR_OFFSET,
              _trx.ubr_rx.remote_tx_event_q.addr);
    EXPECT_EQ(_remote.data() + DATASTATUSQ_ADDR_OFFSET,
              _trx.ubr_rx.remote_data_status_q.addr);
    EXPECT_GT(_trx.ubr_rx.local_data_q.len, 0u);
    EXPECT_GT(_trx.ubr_tx.remote_data_q.len, 0u);
}

TEST_F(UBRingTest, allocate_and_map_reject_invalid_shm) {
    SHM bad = {};
    snprintf(bad.name, sizeof(bad.name), "UBRING_ut_bad");
    bad.len = 3 * 1024 * 1024;   // not a 4MB multiple

    EXPECT_EQ(-1, _ring.UbrAllocateServerShm(
                      &bad, &bad, UBR_DATA_FORMAT_LEGACY_64));
    // The manager is not initialized in this fixture, so acquiring a slot
    // fails before any shared memory is created.
    EXPECT_EQ(-1, _ring.UbrAllocateLocalShm(&bad, "UBRING_ut_bad"));
    EXPECT_EQ(SHM_ERR_INPUT_INVALID,
              _ring.UbrMapRemoteShmAddTimer(
                  &bad, "UBRING_ut_bad", std::numeric_limits<uint64_t>::max(),
                  UBR_DATA_FORMAT_LEGACY_64));
    EXPECT_EQ(SHM_ERR, _ring.UbrMapRemoteShmAddTimer(
                           &bad, "UBRING_ut_missing", SHM_ALLOC_UNIT_SIZE,
                           UBR_DATA_FORMAT_LEGACY_64));
}

TEST_F(UBRingTest, map_shm_rejects_invalid_arguments) {
    SHM good_local = {_local.data(), _local.size(), 0, "local", 0};
    SHM good_remote = {_remote.data(), _remote.size(), 0, "remote", 0};
    SHM null_addr = {nullptr, _local.size(), 0, "null", 0};

    EXPECT_EQ(UBRING_ERR, _ring.UbrTrxMapShm(nullptr, &good_remote));
    EXPECT_EQ(UBRING_ERR, _ring.UbrTrxMapShm(&good_local, nullptr));
    EXPECT_EQ(UBRING_ERR, _ring.UbrTrxMapShm(&null_addr, &good_remote));
    EXPECT_EQ(UBRING_ERR, _ring.UbrTrxMapShm(&good_local, &null_addr));
}

TEST_F(UBRingTest, initialization_rejects_invalid_format) {
    MapShm();
    // The queues are mapped, but the transaction has not selected a format.
    EXPECT_EQ(UBRING_ERR, _ring.UbrServerTrxInit(
                               &_trx.local_shm, &_trx.remote_shm,
                               UBR_DATA_FORMAT_NONE));
    // A second IPC_V2 preparation is refused once the format is selected.
    ASSERT_EQ(0, _ring.UbrPrepareIpcV2Format());
    EXPECT_EQ(UBR_DATA_FORMAT_IPC_V2, _trx.data_format);
    EXPECT_EQ(-1, _ring.UbrPrepareIpcV2Format());
}

TEST_F(UBRingTest, ipc_v2_preparation_needs_mapped_queues) {
    // Nothing mapped at all.
    EXPECT_EQ(-1, _ring.UbrPrepareIpcV2Format());
    // Queues mapped: the RX queue is published and the cursors reset.
    MapShm();
    _trx.ubr_rx.read_pos = 7;
    _trx.ubr_rx.ipc_v2_read_offset = 3;
    ASSERT_EQ(0, _ring.UbrPrepareIpcV2Format());
    EXPECT_EQ(0u, _trx.ubr_tx.write_pos);
    EXPECT_EQ(0u, _trx.ubr_rx.read_pos);
    EXPECT_EQ(0u, _trx.ubr_rx.ipc_v2_read_offset);
    EXPECT_EQ(0u, _trx.ubr_tx.ep_last_cap);
    EXPECT_GE(_trx.ubr_rx.capacity, 2u);

    // A queue that is not mapped is rejected before any state is published.
    UBRing ring;
    UbrTrx trx{};
    ring.SetTrxForTest(&trx);
    trx.ubr_rx.local_data_q.addr = _local.data();
    trx.ubr_rx.local_data_q.len = _local.size();
    EXPECT_EQ(-1, ring.UbrPrepareIpcV2Format());
}

TEST_F(UBRingTest, server_legacy_layout_sets_capacities_and_tails) {
    MapShm();
    ASSERT_EQ(UBRING_OK, _ring.InitializeServerLegacyFormat());
    const uint32_t local_cap = _trx.ubr_rx.capacity;
    const uint32_t remote_cap = _trx.ubr_tx.capacity;
    EXPECT_EQ(_trx.ubr_rx.local_data_q.len / UBR_MSG_LEN, local_cap);
    EXPECT_EQ(_trx.ubr_tx.remote_data_q.len / UBR_MSG_LEN, remote_cap);
    EXPECT_EQ(local_cap / 8, _trx.ubr_rx.deal_msg_max_cnt);

    UbrDataStatusQMsg* local_status =
        reinterpret_cast<UbrDataStatusQMsg*>(
            _trx.ubr_tx.local_data_status_q.addr);
    UbrDataStatusQMsg* remote_status =
        reinterpret_cast<UbrDataStatusQMsg*>(
            _trx.ubr_rx.remote_data_status_q.addr);
    EXPECT_EQ(remote_cap - 1, local_status->tail);
    EXPECT_EQ(local_cap - 1, remote_status->tail);
}

TEST_F(UBRingTest, client_legacy_layout_prewrites_and_activates) {
    MapShm();
    _ring.InitializeClientRemoteLegacyFormat();
    EXPECT_EQ(_trx.ubr_tx.remote_data_q.len / UBR_MSG_LEN,
              _trx.ubr_tx.capacity);
    ASSERT_EQ(UBRING_OK, _ring.InitializeClientLocalLegacyFormat());
    EXPECT_EQ(_trx.ubr_rx.local_data_q.len / UBR_MSG_LEN,
              _trx.ubr_rx.capacity);
    EXPECT_EQ(_trx.ubr_rx.capacity / 8, _trx.ubr_rx.deal_msg_max_cnt);

    memset(_trx.ubr_rx.local_data_q.addr, 0xff,
           _trx.ubr_rx.capacity * sizeof(UbrMsgFormat));
    memset(_trx.ubr_tx.remote_data_q.addr, 0xff,
           _trx.ubr_tx.capacity * sizeof(UbrMsgFormat));
    _ring.InitializeClientLegacyFormat();
    EXPECT_EQ(0, _trx.ubr_rx.local_data_q.addr[0]);
    EXPECT_EQ(0, _trx.ubr_rx.local_data_q.addr[
                     _trx.ubr_rx.capacity * sizeof(UbrMsgFormat) - 1]);
    EXPECT_EQ(0, _trx.ubr_tx.remote_data_q.addr[0]);
    EXPECT_EQ(0, _trx.ubr_tx.remote_data_q.addr[
                     _trx.ubr_tx.capacity * sizeof(UbrMsgFormat) - 1]);

    _ring.ActivateClientFormat();
    EXPECT_EQ(UBR_STATE_CONNECTED,
              reinterpret_cast<UbrEventQMsg*>(
                  _trx.ubr_rx.local_rx_event_q.addr)->flag);
    EXPECT_EQ(UBR_STATE_CONNECTED,
              reinterpret_cast<UbrEventQMsg*>(
                  _trx.ubr_rx.remote_tx_event_q.addr)->flag);
    EXPECT_EQ(UBR_STATE_CONNECTED, _trx.ubr_tx.trx_state);
    EXPECT_EQ(UBR_STATE_CONNECTED, _trx.ubr_rx.trx_state);
}

TEST_F(UBRingTest, legacy_send_splits_chunks_and_reports_errors) {
    InitLegacy();
    UbrDataStatusQMsg* local_status =
        reinterpret_cast<UbrDataStatusQMsg*>(
            _trx.ubr_tx.local_data_status_q.addr);
    UbrEventQMsg* remote_rx_event = reinterpret_cast<UbrEventQMsg*>(
        _trx.ubr_tx.remote_rx_event_q.addr);

    const char short_payload[] = "hello";
    ASSERT_EQ(5, _ring.UbrTrxSend(short_payload, 5));
    EXPECT_EQ(1u, _trx.ubr_tx.write_pos);
    EXPECT_EQ(UBR_MSG_CHUNK_EOF,
              RemoteChunk(0)->header[UBR_MSG_FLAG_INDEX]);
    EXPECT_EQ(5, RemoteChunk(0)->header[UBR_MSG_LEN_INDEX]);
    EXPECT_EQ(0, memcmp(RemoteChunk(0)->payload.inner, short_payload, 5));
    EXPECT_EQ(1u, remote_rx_event->io_id);

    std::string long_payload(100, '\0');
    for (size_t i = 0; i < long_payload.size(); ++i) {
        long_payload[i] = static_cast<char>('a' + i % 26);
    }
    ASSERT_EQ(100, _ring.UbrTrxSend(long_payload.data(), 100));
    EXPECT_EQ(3u, _trx.ubr_tx.write_pos);
    EXPECT_EQ(UBR_MSG_CHUNK_EXIST,
              RemoteChunk(1)->header[UBR_MSG_FLAG_INDEX]);
    EXPECT_EQ(UBR_MSG_PAYLOAD_LEN,
              RemoteChunk(1)->header[UBR_MSG_LEN_INDEX]);
    EXPECT_EQ(UBR_MSG_CHUNK_EOF,
              RemoteChunk(2)->header[UBR_MSG_FLAG_INDEX]);
    EXPECT_EQ(40, RemoteChunk(2)->header[UBR_MSG_LEN_INDEX]);
    EXPECT_EQ(0, memcmp(RemoteChunk(1)->payload.inner, long_payload.data(),
                        UBR_MSG_PAYLOAD_LEN));
    EXPECT_EQ(0, memcmp(RemoteChunk(2)->payload.inner,
                        long_payload.data() + UBR_MSG_PAYLOAD_LEN, 40));

    // No room between write_pos and the peer's tail.
    local_status->tail = _trx.ubr_tx.write_pos;
    EXPECT_EQ(UBRING_RETRY, _ring.UbrTrxSend(short_payload, 5));

    // A message that needs the whole ring capacity is rejected.
    local_status->tail =
        (_trx.ubr_tx.write_pos + _trx.ubr_tx.capacity - 1) %
        _trx.ubr_tx.capacity;
    std::vector<char> too_large(_trx.ubr_tx.capacity * UBR_MSG_PAYLOAD_LEN,
                                'x');
    errno = 0;
    EXPECT_EQ(UBRING_ERR,
              _ring.UbrTrxSend(too_large.data(), too_large.size()));
    EXPECT_EQ(EMSGSIZE, errno);
}

TEST_F(UBRingTest, legacy_recv_assembles_and_tracks_cursor) {
    InitLegacy();
    UbrDataStatusQMsg* remote_status =
        reinterpret_cast<UbrDataStatusQMsg*>(
            _trx.ubr_rx.remote_data_status_q.addr);

    MakeChunk(LocalChunk(0), UBR_MSG_CHUNK_EXIST, UBR_MSG_PAYLOAD_LEN, 0,
              'a');
    MakeChunk(LocalChunk(1), UBR_MSG_CHUNK_EOF, 10, 0, 'b');
    std::string output(70, '?');
    ASSERT_EQ(70, _ring.UbrTrxRecv(&output[0], output.size()));
    EXPECT_EQ(std::string(60, 'a') + std::string(10, 'b'), output);
    EXPECT_EQ(2u, _trx.ubr_rx.read_pos);
    EXPECT_EQ(UBR_MSG_CHUNK_NONE, LocalChunk(0)->header[UBR_MSG_FLAG_INDEX]);
    EXPECT_EQ(UBR_MSG_CHUNK_NONE, LocalChunk(1)->header[UBR_MSG_FLAG_INDEX]);
    // The tail is published before the read cursor advances, so it names
    // the last chunk that was fully consumed.
    EXPECT_EQ(1u, remote_status->tail);
    EXPECT_EQ(2u, _trx.ubr_rx.ep_eof_pos);

    // Drained ring: the non-blocking receive reports a retry, while the
    // blocking receive reports EAGAIN.
    EXPECT_EQ(UBRING_RETRY, _ring.UbrTrxRecv(&output[0], output.size()));
    errno = 0;
    EXPECT_EQ(-1, _ring.UbrTrxRecvBlockMode(
                      reinterpret_cast<uint8_t*>(&output[0]), 1));
    EXPECT_EQ(EAGAIN, errno);
    EXPECT_EQ(UBRING_ERR, _ring.UbrTrxReadv(nullptr, 0));
}

TEST_F(UBRingTest, legacy_recv_honors_partial_buffer_and_chunk_cursor) {
    InitLegacy();
    MakeChunk(LocalChunk(0), UBR_MSG_CHUNK_EXIST, UBR_MSG_PAYLOAD_LEN, 0,
              'a');
    MakeChunk(LocalChunk(1), UBR_MSG_CHUNK_EOF, 10, 0, 'b');

    char small[10] = {};
    ASSERT_EQ(10, _ring.UbrTrxRecv(small, sizeof(small)));
    EXPECT_EQ(std::string(10, 'a'), std::string(small, sizeof(small)));
    EXPECT_EQ(0u, _trx.ubr_rx.read_pos);
    EXPECT_EQ(10, LocalChunk(0)->header[UBR_MSG_CUR_INDEX]);

    // Continue with a buffer larger than the rest of the message: the cursor
    // spans the chunk boundary and stops at EOF.
    std::string rest(60, '?');
    ASSERT_EQ(60, _ring.UbrTrxRecv(&rest[0], rest.size()));
    EXPECT_EQ(std::string(50, 'a') + std::string(10, 'b'), rest);
    EXPECT_EQ(2u, _trx.ubr_rx.read_pos);
    // A fully consumed chunk keeps its cursor and is marked NONE.
    EXPECT_EQ(UBR_MSG_PAYLOAD_LEN,
              LocalChunk(0)->header[UBR_MSG_CUR_INDEX]);
}

TEST_F(UBRingTest, legacy_recv_rejects_crafted_chunk_header) {
    InitLegacy();
    MakeChunk(LocalChunk(0), UBR_MSG_CHUNK_EOF, 255, 0, 'x');
    char output[8] = {};
    errno = 0;
    EXPECT_EQ(UBRING_ERR, _ring.UbrTrxRecv(output, sizeof(output)));
    EXPECT_EQ(EBADMSG, errno);
    // The chunk is left untouched (and unconsumed) after the rejection.
    EXPECT_EQ(255, LocalChunk(0)->header[UBR_MSG_LEN_INDEX]);

    MakeChunk(LocalChunk(0), UBR_MSG_CHUNK_EOF, 10, 20, 'x');
    errno = 0;
    EXPECT_EQ(UBRING_ERR, _ring.UbrTrxRecv(output, sizeof(output)));
    EXPECT_EQ(EBADMSG, errno);
}

TEST_F(UBRingTest, legacy_readv_spans_iovecs_and_reports_eagain) {
    InitLegacy();
    char empty = 0;
    struct iovec none = {&empty, 1};
    errno = 0;
    EXPECT_EQ(-1, _ring.UbrTrxReadv(&none, 1));
    EXPECT_EQ(EAGAIN, errno);
    errno = 0;
    EXPECT_EQ(-1, _ring.UbrTrxReadvBlockMode(&none, 1));
    EXPECT_EQ(EAGAIN, errno);

    MakeChunk(LocalChunk(0), UBR_MSG_CHUNK_EXIST, UBR_MSG_PAYLOAD_LEN, 0,
              'a');
    MakeChunk(LocalChunk(1), UBR_MSG_CHUNK_EOF, 10, 0, 'b');
    std::string output(70, '?');
    struct iovec iov[3] = {
        {nullptr, 0},
        {&output[0], 20},
        {&output[20], 50},
    };
    ASSERT_EQ(70, _ring.UbrTrxReadv(iov, 3));
    EXPECT_EQ(std::string(60, 'a') + std::string(10, 'b'), output);
    EXPECT_EQ(2u, _trx.ubr_rx.read_pos);
}

TEST_F(UBRingTest, legacy_readable_and_writeable_state) {
    InitLegacy();
    UbrEventQMsg* local_rx_event = reinterpret_cast<UbrEventQMsg*>(
        _trx.ubr_rx.local_rx_event_q.addr);
    UbrDataStatusQMsg* local_status =
        reinterpret_cast<UbrDataStatusQMsg*>(
            _trx.ubr_tx.local_data_status_q.addr);

    // Nothing to read: both edge and level trigger say "not ready", and the
    // edge trigger also records the io id.
    local_rx_event->io_id = 5;
    EXPECT_EQ(MPA_MUXER_NOT_READY,
              _ring.IsUbrTrxReadable(EPOLLET));
    EXPECT_EQ(MPA_MUXER_NOT_READY, _ring.IsUbrTrxReadable(0));
    // A not-ready edge poll returns before publishing the observed io id.
    EXPECT_EQ(0u, _trx.ubr_rx.in_io_id.load());

    MakeChunk(LocalChunk(0), UBR_MSG_CHUNK_EOF, 1, 0, 'a');
    EXPECT_EQ(UBRING_OK, _ring.IsUbrTrxReadable(0));
    local_rx_event->io_id = 6;
    EXPECT_EQ(UBRING_OK, _ring.IsUbrTrxReadable(EPOLLET));
    // The second edge-triggered poll with the same io id is not ready.
    EXPECT_EQ(MPA_MUXER_NOT_READY,
              _ring.IsUbrTrxReadable(EPOLLET));

    // Writable: the peer tail leaves room. The first edge-triggered poll
    // succeeds and records the room it reported; a second one with the same
    // room is suppressed until the window grows again.
    EXPECT_EQ(UBRING_OK, _ring.IsUbrTrxWriteable(EPOLLET));
    EXPECT_EQ(UBRING_OK, _ring.IsUbrTrxWriteable(0));
    EXPECT_EQ(MPA_MUXER_NOT_READY,
              _ring.IsUbrTrxWriteable(EPOLLET));
    local_status->tail = _trx.ubr_tx.write_pos;
    EXPECT_EQ(MPA_MUXER_NOT_READY,
              _ring.IsUbrTrxWriteable(0));

    // The guards reject a null transaction and unconnected state.
    _trx.ubr_tx.trx_state = UBR_STATE_CLOSING;
    EXPECT_EQ(UBRING_ERR, _ring.IsUbrTrxReadable(0));
    EXPECT_EQ(UBRING_ERR, _ring.IsUbrTrxWriteable(0));
    _trx.ubr_tx.trx_state = UBR_STATE_CONNECTED;
    _trx.data_format = UBR_DATA_FORMAT_NONE;
    EXPECT_EQ(UBRING_ERR, _ring.IsUbrTrxReadable(0));
    EXPECT_EQ(UBRING_ERR, _ring.IsUbrTrxWriteable(0));
}

TEST_F(UBRingTest, timeout_and_prewrite_helpers_are_bounds_checked) {
    using namespace brpc::ubring;
    EXPECT_EQ(UBRING_ERR,
              _ring.UbrSetTimeout(UBR_TASK_STEP_NUM, 1));
    EXPECT_EQ(UBRING_ERR, _ring.UbrSetTimeout(UBR_TASK_CLOSE, -1));
    const uint32_t saved = g_sleep_time[UBR_TASK_CLOSE];
    EXPECT_EQ(UBRING_OK, _ring.UbrSetTimeout(UBR_TASK_CLOSE, 7));
    EXPECT_EQ(7u, g_sleep_time[UBR_TASK_CLOSE]);
    _ring.UbrSetSleepTask(static_cast<UbrTaskStep>(-1));
    _ring.UbrSetSleepTask(UBR_TASK_STEP_NUM);
    g_sleep_time[UBR_TASK_CLOSE] = saved;

    // Every width of the unaligned pre-write path, plus the tail bytes.
    for (size_t len = 0; len <= 24; ++len) {
        uint8_t buffer[32];
        memset(buffer, 0xff, sizeof(buffer));
        _ring.PreWriteAddr(buffer, len);
        for (size_t i = 0; i < len; ++i) {
            ASSERT_EQ(0, buffer[i]) << "len=" << len << " i=" << i;
        }
        for (size_t i = len; i < sizeof(buffer); ++i) {
            ASSERT_EQ(0xff, buffer[i]) << "len=" << len << " i=" << i;
        }
    }
    _ring.PreWriteAddr(nullptr, 8);
    _ring.PrewriteUbrTx(nullptr);
    _ring.PrewriteUbrRx(nullptr);

    UbrTx tx = {};
    tx.remote_data_q.addr = _remote.data();
    tx.capacity = 2;
    memset(_remote.data(), 0xff, 3 * sizeof(UbrMsgFormat));
    _ring.PrewriteUbrTx(&tx);
    EXPECT_EQ(0, _remote[0]);
    EXPECT_EQ(0, _remote[2 * sizeof(UbrMsgFormat) - 1]);
    EXPECT_EQ(0xff, _remote[2 * sizeof(UbrMsgFormat)]);
}

TEST_F(UBRingTest, close_claim_and_clear_paths_cover_guards) {
    using namespace brpc::ubring;
    EXPECT_EQ(UBRING_ERR, UBRing::UbrTrxCloseCheck(nullptr, 0));
    EXPECT_EQ(UBRING_ERR, UBRing::UbrClearResourceCheck(nullptr));
    EXPECT_EQ(UBRING_ERR,
              UBRing::UbrAddAsynClearTimer(nullptr, 0));
    EXPECT_EQ(UBRING_ERR, UBRing::UbrPassiveClearTrx(nullptr, 0));

    UbrTrx trx{};
    EXPECT_EQ(UBRING_ERR, UBRing::UbrClearResourceCheck(&trx));
    EXPECT_EQ(UBRING_ERR, UBRing::UbrTrxCloseCheck(&trx, 0));

    UbrEventQMsg event = {};
    event.flag = UBR_STATE_CONNECTED;
    trx.ubr_tx.local_tx_event_q.addr =
        reinterpret_cast<uint8_t*>(&event);
    EXPECT_EQ(UBRING_OK, UBRing::UbrClearResourceCheck(&trx));
    EXPECT_EQ(UBR_STATE_CLOSED, event.flag);
    EXPECT_EQ(UBR_STATE_CLOSED, trx.ubr_tx.trx_state);
    // Without a mapped local shm the delayed clear is a no-op (a stale event
    // for a released transaction), not an error.
    EXPECT_EQ(UBRING_OK, UBRing::UbrAddAsynClearTimer(&trx, 0));
    EXPECT_EQ(UBRING_OK, UBRing::UbrAddAsynClearTimer(&trx, 1));

    EXPECT_EQ(nullptr, UBRing::UbrAsynClearCallback(nullptr, 0));
    EXPECT_EQ(nullptr, UBRing::UbrPassiveClearCallback(nullptr, 0));
}

TEST_F(UBRingTest, close_and_heartbeat_callbacks_reject_stale_generations) {
    using namespace brpc::ubring;
    UbrTrx trx{};
    char storage[256] = {};
    trx.local_shm.addr = reinterpret_cast<uint8_t*>(storage);
    trx.ubr_rx.local_rx_event_q.addr = reinterpret_cast<uint8_t*>(&trx);
    trx.ubr_tx.local_tx_event_q.addr = reinterpret_cast<uint8_t*>(&trx);
    trx.ubr_id.store(9);

    EXPECT_EQ(nullptr, UBRing::UbrTrxCloseCallback(&trx, 8));
    EXPECT_EQ(nullptr, UBRing::UbrTrxCloseCallback(nullptr, 0));

    UbrEventQMsg rx_event = {};
    UbrEventQMsg tx_event = {};
    UbrDataStatusQMsg local_status = {};
    UbrDataStatusQMsg remote_status = {};
    trx.ubr_rx.local_rx_event_q.addr =
        reinterpret_cast<uint8_t*>(&rx_event);
    trx.ubr_tx.local_tx_event_q.addr =
        reinterpret_cast<uint8_t*>(&tx_event);
    trx.ubr_tx.local_data_status_q.addr =
        reinterpret_cast<uint8_t*>(&local_status);
    trx.ubr_rx.remote_data_status_q.addr =
        reinterpret_cast<uint8_t*>(&remote_status);

    // Close is skipped while the peer has not closed.
    rx_event.flag = UBR_STATE_CONNECTED;
    tx_event.flag = UBR_STATE_CONNECTED;
    EXPECT_EQ(nullptr, UBRing::UbrTrxCloseCallback(&trx, 9));
    // ... and once this side already closed.
    rx_event.flag = UBR_STATE_CLOSED;
    tx_event.flag = UBR_STATE_CLOSED;
    EXPECT_EQ(nullptr, UBRing::UbrTrxCloseCallback(&trx, 9));
    // A matching close with no close rounds left returns immediately.
    tx_event.flag = UBR_STATE_CONNECTED;
    trx.close_cnt.store(0);
    EXPECT_EQ(nullptr, UBRing::UbrTrxCloseCallback(&trx, 9));

    // Heartbeat: a stale generation is ignored.
    trx.ubr_tx.trx_state = UBR_STATE_CONNECTED;
    trx.ubr_rx.trx_state = UBR_STATE_CONNECTED;
    trx.ubr_tx.hb_retry_cnt = 0;
    EXPECT_EQ(nullptr, UBRing::UbrTrxHBCallback(&trx, 8));
    EXPECT_EQ(0, remote_status.heart_beat);
    // A matching fire publishes the heartbeat and counts a retry.
    EXPECT_EQ(nullptr, UBRing::UbrTrxHBCallback(&trx, 9));
    EXPECT_EQ(1, remote_status.heart_beat);
    EXPECT_EQ(1, trx.ubr_tx.hb_retry_cnt);
    // The peer's echo resets the retry counter.
    local_status.heart_beat = 1;
    EXPECT_EQ(nullptr, UBRing::UbrTrxHBCallback(&trx, 9));
    EXPECT_EQ(0, local_status.heart_beat);
    EXPECT_EQ(0, trx.ubr_tx.hb_retry_cnt);
    // A disconnected transaction is not heartbeated.
    trx.ubr_rx.trx_state = UBR_STATE_CLOSING;
    EXPECT_EQ(nullptr, UBRing::UbrTrxHBCallback(&trx, 9));
}

}  // namespace ubring
}  // namespace brpc

// ---------------------------------------------------------------------------
// Endpoint guards, polling mode, hello format helpers and transport options.
// These do not need a UB device: the endpoint's UT switch short-circuits the
// handshake and the data path, while the format helpers are pure functions.

TEST(UBShmEndpointGuardTest, skip_mode_short_circuits_io) {
    bool saved_skip = brpc::ubring::g_skip_ub_init;
    brpc::ubring::g_skip_ub_init = true;
    {
        brpc::ubring::UBShmEndpoint ep(nullptr);
        EXPECT_FALSE(ep.IsWritable());

        butil::IOBuf data;
        data.append("payload");
        butil::IOBuf* list[1] = {&data};
        errno = 0;
        EXPECT_EQ(-1, ep.CutFromIOBufList(list, 1));
        EXPECT_EQ(EAGAIN, errno);

        brpc::ubring::SHM shm = {};
        EXPECT_EQ(0, ep.AllocateClientResources(&shm, "UBRING_ut_skip"));
        ep.DeallocateResources();
        ep.Reset();
        EXPECT_EQ(brpc::ubring::UBR_DATA_FORMAT_NONE,
                  ep._negotiated_data_format);

        // An empty IOBuf list short-circuits before the ring is dereferenced.
        brpc::ubring::g_skip_ub_init = false;
        EXPECT_EQ(0, ep.CutFromIOBufList(list, 0));
        brpc::ubring::g_skip_ub_init = true;
    }
    brpc::ubring::g_skip_ub_init = saved_skip;
}

TEST(UBShmEndpointGuardTest, poll_callbacks_ignore_unknown_socket) {
    // An endpoint whose socket id is invalid must not be dereferenced by the
    // polling callbacks.
    brpc::ubring::UBShmEndpoint ep(nullptr);
    brpc::ubring::UBShmEndpoint::PollIn(&ep, 0);
    brpc::ubring::UBShmEndpoint::PollOut(&ep, 0);
}

TEST(UBShmEndpointPollerTest, polling_mode_starts_and_stops) {
    using brpc::ubring::UBShmEndpoint;
    const int saved_pollers = brpc::ubring::FLAGS_ub_poller_num;
    const bool saved_yield = brpc::ubring::FLAGS_ub_poller_yield;
    brpc::ubring::FLAGS_ub_poller_num = 1;
    // The default polling loop is a spin; yield so the rest of the test
    // process (and the timer thread) still runs promptly.
    brpc::ubring::FLAGS_ub_poller_yield = true;
    // Only tag 0 is used here. Building the group directly (instead of
    // GlobalInitialize) avoids allocating the process-wide mutex this test
    // would otherwise leave behind, and needs no endpoint to own it.
    UBShmEndpoint::_poller_groups =
        std::vector<UBShmEndpoint::PollerGroup>(1);

    std::atomic<int> init_calls(0);
    std::atomic<int> callback_calls(0);
    std::atomic<int> release_calls(0);
    auto init_fn = [&init_calls] { init_calls.fetch_add(1); };
    auto callback = [&callback_calls] { callback_calls.fetch_add(1); };
    auto release_fn = [&release_calls] { release_calls.fetch_add(1); };

    ASSERT_EQ(0, UBShmEndpoint::PollingModeInitialize(
                     0, callback, init_fn, release_fn));
    ASSERT_TRUE(WaitUbrTimerCondition([&] {
        return init_calls.load() == 1 && callback_calls.load() >= 1;
    }));
    // Starting an already running tag is a no-op and must not spawn another
    // group (two consumers would race on the same op queue).
    EXPECT_EQ(0, UBShmEndpoint::PollingModeInitialize(
                     0, callback, init_fn, release_fn));
    EXPECT_EQ(1, init_calls.load());

    UBShmEndpoint::PollingModeRelease(0);
    EXPECT_EQ(1, release_calls.load());

    // The helper wrapper uses the same start/release pair.
    EXPECT_TRUE(brpc::ubring::InitPollingModeWithTag(0));
    UBShmEndpoint::PollingModeRelease(0);

    brpc::ubring::FLAGS_ub_poller_num = saved_pollers;
    brpc::ubring::FLAGS_ub_poller_yield = saved_yield;
}

TEST(UBShmEndpointFormatTest, hello_extension_and_format_selection) {
    using namespace brpc::ubring;

    char wire[HelloFormatExtension::WIRE_SIZE] = {};
    HelloFormatExtension extension = {HelloFormatExtension::WIRE_SIZE,
                                      UBR_DATA_FORMAT_IPC_V2};
    extension.Serialize(wire);
    uint16_t net_len = 0;
    memcpy(&net_len, wire, sizeof(net_len));
    EXPECT_EQ(4, butil::NetToHost16(net_len));
    uint16_t net_format = 0;
    memcpy(&net_format, wire + sizeof(net_len), sizeof(net_format));
    EXPECT_EQ(UBR_DATA_FORMAT_IPC_V2, butil::NetToHost16(net_format));

    HelloFormatExtension parsed = {};
    parsed.Deserialize(wire);
    EXPECT_EQ(4, parsed.extension_len);
    EXPECT_EQ(UBR_DATA_FORMAT_IPC_V2, parsed.format_id);

    EXPECT_EQ(UBR_DATA_FORMAT_IPC_V2,
              PreferredDataFormatForShmType(SHM_TYPE_IPC));
    EXPECT_EQ(UBR_DATA_FORMAT_LEGACY_64,
              PreferredDataFormatForShmType(SHM_TYPE_UBS));
    EXPECT_EQ(UBR_DATA_FORMAT_NONE,
              PreferredDataFormatForShmType(SHM_TYPE_UB));
    EXPECT_EQ(UBR_DATA_FORMAT_NONE,
              PreferredDataFormatForShmType(SHM_TYPE_UNSUPPORT));

    EXPECT_EQ(UBR_DATA_FORMAT_IPC_V2,
              SelectDataFormat(UBR_DATA_FORMAT_IPC_V2,
                               UBR_DATA_FORMAT_IPC_V2));
    EXPECT_EQ(UBR_DATA_FORMAT_LEGACY_64,
              SelectDataFormat(UBR_DATA_FORMAT_LEGACY_64,
                               UBR_DATA_FORMAT_LEGACY_64));
    // Mismatched or unsupported formats fall back to TCP.
    EXPECT_EQ(UBR_DATA_FORMAT_NONE,
              SelectDataFormat(UBR_DATA_FORMAT_IPC_V2,
                               UBR_DATA_FORMAT_LEGACY_64));
    EXPECT_EQ(UBR_DATA_FORMAT_NONE,
              SelectDataFormat(UBR_DATA_FORMAT_LEGACY_64,
                               UBR_DATA_FORMAT_IPC_V2));
    EXPECT_EQ(UBR_DATA_FORMAT_NONE,
              SelectDataFormat(UBR_DATA_FORMAT_NONE, 0));
    EXPECT_EQ(UBR_DATA_FORMAT_NONE,
              SelectDataFormat(UBR_DATA_FORMAT_IPC_V2, 99));
}

TEST(UBShmTransportTest, option_validation) {
    using brpc::UBShmTransport;

    brpc::ChannelOptions channel_options;
    channel_options.protocol = "baidu_std";
    EXPECT_TRUE(UBShmTransport::OptionsAvailableForUB(&channel_options));
    channel_options.protocol = "http";
    EXPECT_FALSE(UBShmTransport::OptionsAvailableForUB(&channel_options));
    channel_options.protocol = "baidu_std";
    channel_options.mutable_ssl_options();
    EXPECT_FALSE(UBShmTransport::OptionsAvailableForUB(&channel_options));

    brpc::ServerOptions server_options;
    EXPECT_TRUE(UBShmTransport::OptionsAvailableOverUB(&server_options));
    server_options.mutable_ssl_options();
    EXPECT_FALSE(UBShmTransport::OptionsAvailableOverUB(&server_options));
}

TEST(UBShmHelperTest, availability_and_protocol_whitelist) {
    using namespace brpc::ubring;
    EXPECT_FALSE(IsUBAvailable());
    // Disabling an unavailable UB is a no-op, not a fatal error.
    GlobalDisableUb();
    EXPECT_FALSE(IsUBAvailable());
    EXPECT_TRUE(SupportedByUB("baidu_std"));
    EXPECT_FALSE(SupportedByUB("http"));
    EXPECT_FALSE(SupportedByUB(""));

    // In UT the global initializer is a no-op, so it must not publish UB as
    // available either.
    const bool saved_skip = g_skip_ub_init;
    g_skip_ub_init = true;
    GlobalUBInitializeOrDie();
    EXPECT_FALSE(IsUBAvailable());
    g_skip_ub_init = saved_skip;
}

// ---------------------------------------------------------------------------
// UbrCleanupWorker: a failed lazy start must be retried, not latched.

namespace {
butil::atomic<int> g_ub_test_cleanup_runs(0);

void UbTestCleanupStep(brpc::ubring::ShmList*) {
    g_ub_test_cleanup_runs.fetch_add(1);
}
}  // namespace

// The worker is a process-wide singleton started lazily by the first post and
// never stopped, so its lazy start can only be observed before any other test
// posted a cleanup job. This suite is therefore declared ahead of
// UBShmEndpointTest, whose teardown paths post cleanups. Should a shuffled
// run still find the worker started, the probe below detects that and skips
// instead of asserting on a start that is never attempted.
TEST(UbrCleanupWorkerTest, start_failure_is_retried_not_latched) {
    using brpc::ubring::UbrCleanupWorker;
    brpc::ubring::ShmList shm_list;
    memset(&shm_list, 0, sizeof(shm_list));
    g_ub_test_cleanup_runs.store(0);

    // Fail the lazy start exactly like a transient bthread creation failure
    // (e.g. ENOMEM) would.
    brpc::ubring::g_ub_test_worker_start_rc.store(-1);

    // The first post doubles as the probe: a post that succeeds despite the
    // injection means the worker was already started by an earlier test and
    // no start -- the code under test -- is attempted anymore.
    if (UbrCleanupWorker::PostShmDrain(&shm_list, UbTestCleanupStep)) {
        brpc::ubring::g_ub_test_worker_start_rc.store(0);
        UbrCleanupWorker::DrainAndWait();
        GTEST_SKIP() << "the cleanup worker was already started by an "
                        "earlier test; the lazy start cannot be exercised";
    }

    // While the start keeps failing, every post fails and fails only itself:
    // no job is queued, so the caller must run it inline.
    EXPECT_FALSE(UbrCleanupWorker::PostShmDrain(&shm_list, UbTestCleanupStep));
    EXPECT_FALSE(UbrCleanupWorker::PostShmDrain(&shm_list, UbTestCleanupStep));
    EXPECT_EQ(0, g_ub_test_cleanup_runs.load());

    // The failure must not latch: clear the injection and post again. The
    // start is retried, the worker comes up, the job runs exactly once and
    // DrainAndWait observes quiescence.
    brpc::ubring::g_ub_test_worker_start_rc.store(0);
    EXPECT_TRUE(UbrCleanupWorker::PostShmDrain(&shm_list, UbTestCleanupStep));
    UbrCleanupWorker::DrainAndWait();
    EXPECT_EQ(1, g_ub_test_cleanup_runs.load());
}

namespace brpc {
namespace ubring {
class UBShmEndpointTest : public ::testing::Test {
protected:
    void SetUp() override {
        _saved_skip = g_skip_ub_init;
        g_skip_ub_init = false;
        ShmMgrInit();
        UBRingManager::UbrMgrInit();
        UBShmEndpoint::GlobalInitialize();

        brpc::SocketOptions options;
        ASSERT_EQ(0, brpc::Socket::Create(options, &_socket_id));
        brpc::SocketUniquePtr s;
        ASSERT_EQ(0, brpc::Socket::Address(_socket_id, &s));
        _socket = s.get();
        _ep = new UBShmEndpoint(_socket);
    }

    void TearDown() override {
        delete _ep;
        brpc::SocketUniquePtr s;
        if (brpc::Socket::Address(_socket_id, &s) == 0) {
            s->SetFailed();
        }
        UBShmEndpoint::GlobalRelease();
        UBRingManager::UbrMgrFini();
        ShmMgrFini();
        g_skip_ub_init = _saved_skip;
    }

    brpc::SocketId _socket_id = brpc::INVALID_SOCKET_ID;
    brpc::Socket* _socket = nullptr;
    UBShmEndpoint* _ep = nullptr;
    bool _saved_skip = false;
};
}  // namespace ubring
}  // namespace brpc

using brpc::ubring::UBShmEndpointTest;

TEST_F(UBShmEndpointTest, construct_initial_state) {
    ASSERT_NE(nullptr, _ep);
    EXPECT_EQ(brpc::ubring::UBR_DATA_FORMAT_NONE,
              _ep->_negotiated_data_format);
}

TEST_F(UBShmEndpointTest, reset_clears_negotiated_data_format) {
    _ep->_negotiated_data_format = brpc::ubring::UBR_DATA_FORMAT_LEGACY_64;

    _ep->Reset();

    EXPECT_EQ(brpc::ubring::UBR_DATA_FORMAT_NONE,
              _ep->_negotiated_data_format);
}

TEST_F(UBShmEndpointTest, allocate_client_resources_real_shm) {
    brpc::ubring::SHM local_trx_shm =
        {nullptr, 4 * 1024 * 1024, 0, {0}, (uint32_t)_socket->fd()};
    int ret = _ep->AllocateClientResources(&local_trx_shm, "UBRING_ut_client");
    EXPECT_EQ(0, ret);
}

TEST_F(UBShmEndpointTest, reset_cleans_up_resources) {
    brpc::ubring::SHM local_trx_shm =
        {nullptr, 4 * 1024 * 1024, 0, {0}, (uint32_t)_socket->fd()};
    _ep->AllocateClientResources(&local_trx_shm, "UBRING_ut_reset");
    _ep->Reset();
}

TEST_F(UBShmEndpointTest, reset_is_idempotent) {
    _ep->Reset();
    _ep->Reset();
}

// The receive paths (UbrTrxRecvBlockMode / StartReadv) read `msg_len' and
// `cur_index' out of a chunk header the remote peer writes into the ring, then
// copy `msg_len - cur_index' bytes from the 60-byte `payload.inner'. A peer
// that writes msg_len > 60, or cur_index > msg_len (which underflows the
// uint8_t subtraction), makes that copy over-read the payload into adjacent
// shared memory. IsRecvChunkHeaderValid is the guard both paths now apply.
TEST(UBRingRecvChunkHeaderTest, reject_out_of_range_len_and_index) {
    using brpc::ubring::UBRing;
    // Legitimate values a well-formed peer produces: full payload, partial
    // consume, and the fully-consumed boundary.
    EXPECT_TRUE(UBRing::IsRecvChunkHeaderValid(UBR_MSG_PAYLOAD_LEN, 0));
    EXPECT_TRUE(UBRing::IsRecvChunkHeaderValid(10, 5));
    EXPECT_TRUE(UBRing::IsRecvChunkHeaderValid(0, 0));
    EXPECT_TRUE(UBRing::IsRecvChunkHeaderValid(UBR_MSG_PAYLOAD_LEN,
                                               UBR_MSG_PAYLOAD_LEN));
    // msg_len past the payload capacity -> over-read source.
    EXPECT_FALSE(UBRing::IsRecvChunkHeaderValid(UBR_MSG_PAYLOAD_LEN + 1, 0));
    EXPECT_FALSE(UBRing::IsRecvChunkHeaderValid(255, 0));
    // cur_index past msg_len -> `msg_len - cur_index' underflows to a large
    // uint8_t.
    EXPECT_FALSE(UBRing::IsRecvChunkHeaderValid(0, 1));
    EXPECT_FALSE(UBRing::IsRecvChunkHeaderValid(10, 20));
}

// A crafted chunk laid out exactly like one in the ring: the guard rejects it
// so the recv loop never reaches the over-reading memcpy.
TEST(UBRingRecvChunkHeaderTest, crafted_chunk_is_rejected) {
    brpc::ubring::UbrMsgFormat chunk;
    memset(&chunk, 0xAB, sizeof(chunk));
    chunk.header[UBR_MSG_LEN_INDEX] = 255;  // peer claims 255 bytes in a 60-byte payload
    chunk.header[UBR_MSG_CUR_INDEX] = 0;
    EXPECT_FALSE(brpc::ubring::UBRing::IsRecvChunkHeaderValid(
        chunk.header[UBR_MSG_LEN_INDEX], chunk.header[UBR_MSG_CUR_INDEX]));

    chunk.header[UBR_MSG_LEN_INDEX] = UBR_MSG_PAYLOAD_LEN;
    chunk.header[UBR_MSG_CUR_INDEX] = 0;
    EXPECT_TRUE(brpc::ubring::UBRing::IsRecvChunkHeaderValid(
        chunk.header[UBR_MSG_LEN_INDEX], chunk.header[UBR_MSG_CUR_INDEX]));
}

// ---------------------------------------------------------------------------
// Ring lifecycle on real IPC shared memory: server-side setup, a legacy send
// into the peer's mapping, and the delayed cleanup that returns the pool slot.
// This runs last because it starts the cleanup worker and arms real timers.

namespace brpc {
namespace ubring {
namespace {

class UBRingLifecycleTest : public ::testing::Test {
protected:
    void SetUp() override {
        _saved_shm_type = FLAGS_ub_shm_type;
        _saved_flying_timeout = FLAGS_ub_flying_io_timeout_s;
        _saved_cap = FLAGS_ubr_max_managed_num;
        FLAGS_ub_shm_type = SHM_TYPE_IPC;
        // Keep the delayed cleanup observable but short.
        FLAGS_ub_flying_io_timeout_s = 1;
        FLAGS_ubr_max_managed_num = 4;
        ASSERT_EQ(UBRING_OK, ShmMgrInit());
        ASSERT_EQ(UBRING_OK, UBRingManager::UbrMgrInit());
    }

    void TearDown() override {
        UbrCleanupWorker::DrainAndWait();
        UBRingManager::UbrMgrFini();
        ShmMgrFini();
        for (const auto& name : _shm_names) {
            shm_unlink(name.c_str());
        }
        FLAGS_ub_shm_type = _saved_shm_type;
        FLAGS_ub_flying_io_timeout_s = _saved_flying_timeout;
        FLAGS_ubr_max_managed_num = _saved_cap;
    }

    std::string NewShmName(const char* tag) {
        static std::atomic<int> sequence(0);
        std::string name = std::string("UBRING_it_") + tag + "_" +
                           std::to_string(getpid()) + "_" +
                           std::to_string(sequence.fetch_add(1));
        _shm_names.push_back(name);
        shm_unlink(name.c_str());
        return name;
    }

    int _saved_shm_type = SHM_TYPE_IPC;
    int _saved_flying_timeout = 5;
    int _saved_cap = 0;
    std::vector<std::string> _shm_names;
};

}  // namespace

TEST_F(UBRingLifecycleTest, server_setup_send_and_delayed_cleanup) {
    using namespace brpc::ubring;

    // The peer's own mapping of the server shared memory. The ring maps the
    // same object again for its transmit queue, so both views see the data.
    SHM peer = {};
    peer.len = SHM_ALLOC_UNIT_SIZE;
    const std::string peer_name = NewShmName("peer");
    snprintf(peer.name, sizeof(peer.name), "%s", peer_name.c_str());
    ASSERT_EQ(UBRING_OK, ShmLocalCalloc(&peer));
    ASSERT_NE(nullptr, peer.addr);

    SHM remote = {};
    remote.len = SHM_ALLOC_UNIT_SIZE;
    snprintf(remote.name, sizeof(remote.name), "%s", peer_name.c_str());
    SHM local = {};
    local.len = SHM_ALLOC_UNIT_SIZE;
    const std::string local_name = NewShmName("local");
    snprintf(local.name, sizeof(local.name), "%s", local_name.c_str());

    UBRing ring;
    ASSERT_EQ(0, ring.UbrAllocateServerShm(
                     &remote, &local, UBR_DATA_FORMAT_LEGACY_64));
    ASSERT_NE(nullptr, ring._trx);
    EXPECT_EQ(UBR_DATA_FORMAT_LEGACY_64, ring.data_format());
    EXPECT_NE(nullptr, ring._trx->local_shm.addr);
    EXPECT_NE(nullptr, ring._trx->remote_shm.addr);
    // The server setup arms both per-transaction timers.
    EXPECT_NE(nullptr, ring._trx->close_timer.load());
    EXPECT_NE(nullptr, ring._trx->hb_timer.load());
    EXPECT_EQ(ring._trx->ubr_rx.local_data_q.len / UBR_MSG_LEN,
              ring._trx->ubr_rx.capacity);
    EXPECT_EQ(ring._trx->ubr_tx.remote_data_q.len / UBR_MSG_LEN,
              ring._trx->ubr_tx.capacity);

    const char payload[] = "ubshm";
    ASSERT_EQ(6, ring.UbrTrxSend(payload, 6));
    // Read the chunk through the peer's mapping of the same shared object.
    const size_t data_offset = DATAQ_ADDR_OFFSET +
        Aligned64Offset(peer.addr + DATAQ_ADDR_OFFSET);
    UbrMsgFormat* chunk = reinterpret_cast<UbrMsgFormat*>(
        peer.addr + data_offset);
    EXPECT_EQ(UBR_MSG_CHUNK_EOF, chunk->header[UBR_MSG_FLAG_INDEX]);
    EXPECT_EQ(6, chunk->header[UBR_MSG_LEN_INDEX]);
    EXPECT_EQ(0, memcmp(chunk->payload.inner, payload, 6));

    const uint32_t idx = ring._trx->trx_mgr_index;
    const uint64_t id = ring._trx->ubr_id.load();
    EXPECT_TRUE(UBRingManager::IsUbrTrxSlotUsed(idx, id));

    // Simulate the peer having observed the close already, so UbrTrxClose
    // does not have to wait out the disconnect timeout.
    reinterpret_cast<UbrEventQMsg*>(
        local.addr + RX_EVENTQ_ADDR_OFFSET)->flag = UBR_STATE_CLOSED;
    ASSERT_EQ(UBRING_OK, ring.UbrTrxClose());

    // The delayed clear unsets the slot and frees both shared memories.
    ASSERT_TRUE(WaitUbrTimerCondition([idx, id] {
        return !UBRingManager::IsUbrTrxSlotUsed(idx, id);
    }));
    UbrCleanupWorker::DrainAndWait();
    EXPECT_FALSE(UBRingManager::IsUbrTrxSlotUsed(idx, id));

    // The peer still owns its mapping and removes the object.
    EXPECT_EQ(UBRING_OK, ShmLocalFree(&peer));
}

}  // namespace ubring
}  // namespace brpc

#else

TEST(UbringDisabledTest, skip) {
    SUCCEED() << "BRPC_WITH_UBRING is not enabled, skip.";
}

#endif  // BRPC_WITH_UBRING
