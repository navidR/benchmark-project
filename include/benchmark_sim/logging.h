#pragma once

#include <boost/log/trivial.hpp>

namespace bsim {

void InitLogging();

}  // namespace bsim

#define BSIM_LOG(severity) BOOST_LOG_TRIVIAL(severity)
