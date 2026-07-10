#pragma once

namespace bsim {

class MiningDifficulty {
 public:
  explicit MiningDifficulty(double value);

  [[nodiscard]] double value() const { return value_; }

 private:
  double value_;
};

}  // namespace bsim
