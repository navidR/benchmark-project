#pragma once

#include <boost/system/result.hpp>
#include <string>
#include <utility>

namespace bsim {

template <typename T>
using Result = boost::system::result<T, std::string>;

using Status = boost::system::result<void, std::string>;

template <typename T>
Result<T> Ok(T value) {
  return Result<T>(boost::system::in_place_value, std::move(value));
}

template <typename T>
Result<T> Error(std::string message) {
  return Result<T>(boost::system::in_place_error, std::move(message));
}

inline Status OkStatus() { return Status(boost::system::in_place_value); }

inline Status ErrorStatus(std::string message) {
  return Status(boost::system::in_place_error, std::move(message));
}

}  // namespace bsim
