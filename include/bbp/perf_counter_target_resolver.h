#pragma once

#include <boost/json/object.hpp>
#include <cstddef>
#include <optional>
#include <string>

#include "bbp/perf_counter.h"
#include "bbp/tui_view.h"

namespace bbp {

struct PerfCounterSelectionContext {
  TuiView view = TuiView::kNodes;
  std::size_t selected_node = 0;
  std::size_t selected_wallet = 0;
  std::size_t selected_topology_group = 0;
};

PerfCounterTarget ResolvePerfCounterTarget(
    const boost::json::object& report,
    const PerfCounterSelectionContext& selection,
    std::optional<PerfCounterTargetKind> requested_kind,
    const std::optional<std::string>& requested_id);

}  // namespace bbp
