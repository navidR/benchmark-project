#pragma once

#include <string>
#include <vector>

namespace bbp {

class ChainExtraArgs {
 public:
  ChainExtraArgs() = default;
  explicit ChainExtraArgs(std::vector<std::string> arguments);

  const std::vector<std::string>& arguments() const noexcept;
  bool empty() const noexcept;

 private:
  std::vector<std::string> arguments_;
};

}  // namespace bbp
