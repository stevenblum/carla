// Optional Traffic Manager measurements. No event is produced on ordinary
// CARLA RPCs; the tagged diagnostic batch endpoint is the only writer.
#pragma once

#include "Containers/Queue.h"
#include "HAL/PlatformTime.h"
#include <atomic>
#include <string>
#include <tuple>
#include <vector>

// phase, batch, actor, generation, sequence, QPC cycles, CARLA frame,
// queue depth, source episode, source frame, source platform seconds.
using FTrafficManagerLatencyEvent = std::tuple<std::string, uint64_t, uint32_t,
    uint64_t, uint64_t, uint64_t, uint64_t, uint64_t, uint64_t, uint64_t, double>;

class FTrafficManagerLatency {
public:
  static FTrafficManagerLatency &Get() {
    static FTrafficManagerLatency State;
    return State;
  }

  void Push(const char *Phase, uint64_t Batch, uint32_t Actor, uint64_t Generation,
            uint64_t Sequence, uint64_t Frame, uint64_t QueueDepth,
            uint64_t Episode, uint64_t SourceFrame, double SourcePlatformSeconds) {
    // A bounded MPSC queue prevents tracing from blocking the RPC/Game Thread.
    if (Queued.fetch_add(1, std::memory_order_relaxed) >= Capacity) {
      Queued.fetch_sub(1, std::memory_order_relaxed);
      Dropped.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    Events.Enqueue({Phase, Batch, Actor, Generation, Sequence,
                    FPlatformTime::Cycles64(), Frame, QueueDepth,
                    Episode, SourceFrame, SourcePlatformSeconds});
  }

  std::vector<FTrafficManagerLatencyEvent> Drain() {
    std::vector<FTrafficManagerLatencyEvent> Result;
    Result.reserve(8192);
    FTrafficManagerLatencyEvent Item;
    while (Result.size() < 8192 && Events.Dequeue(Item)) {
      Queued.fetch_sub(1, std::memory_order_relaxed);
      Result.push_back(std::move(Item));
    }
    const uint64_t Lost = Dropped.exchange(0, std::memory_order_relaxed);
    if (Lost) Result.emplace_back("dropped", 0, 0, 0, 0, Lost, 0, 0, 0, 0, 0.0);
    return Result;
  }

private:
  static constexpr uint64_t Capacity = 131072;
  TQueue<FTrafficManagerLatencyEvent, EQueueMode::Mpsc> Events;
  std::atomic<uint64_t> Queued{0}, Dropped{0};
};
