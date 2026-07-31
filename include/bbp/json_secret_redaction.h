#pragma once

#include <boost/json/value.hpp>
#include <string>
#include <string_view>

namespace bbp {

inline constexpr std::string_view kPrivateSigningMaterialRedaction =
    "<redacted>";

inline void RedactPrivateSigningMaterial(boost::json::value* value) {
  if (value->is_object()) {
    for (auto& member : value->as_object()) {
      if (member.key() == "source_private_key") {
        member.value() = std::string(kPrivateSigningMaterialRedaction);
      } else {
        RedactPrivateSigningMaterial(&member.value());
      }
    }
    return;
  }
  if (value->is_array()) {
    for (boost::json::value& item : value->as_array()) {
      RedactPrivateSigningMaterial(&item);
    }
  }
}

}  // namespace bbp
