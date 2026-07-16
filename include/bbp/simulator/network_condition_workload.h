#pragma once

#include <cstdint>

#include "bbp/network.h"

namespace bbp {

struct NetworkConditionWorkload {
  std::uint32_t node = 1;
  NetworkCondition condition;
};

}  // namespace bbp
