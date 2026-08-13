#pragma once

#include <boost/json/object.hpp>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "bbp/simulator/wallet_transactions_workload.h"

namespace bbp::simulator_app_internal {

AmountDistribution ParseAmountDistribution(const boost::json::object& object,
                                           const char* field);
IntervalDistribution ParseIntervalDistribution(
    const boost::json::object& object, const char* field);
std::vector<std::uint32_t> ParseWalletIndexList(
    const boost::json::object& object, const char* field,
    std::size_t wallet_count);

}  // namespace bbp::simulator_app_internal
