#pragma once

namespace bbp {

class MiningDifficulty {
 public:
  explicit MiningDifficulty(double value);

  [[nodiscard]] double value() const { return value_; }

 private:
  double value_;
};

}  // namespace bbp
