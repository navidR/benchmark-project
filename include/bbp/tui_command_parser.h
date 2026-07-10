#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <string_view>

#include "bbp/parsed_tui_command.h"

namespace bbp {

class TuiCommandParser {
 public:
  static ParsedTuiCommand Parse(std::string_view input,
                                std::uint64_t block_production_seed);
  static std::string Complete(std::string_view input);
  static std::span<const std::string_view> CommandNames();
};

}  // namespace bbp
