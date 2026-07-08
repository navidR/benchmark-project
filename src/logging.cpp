#include "benchmark_sim/logging.h"

#include <boost/log/expressions.hpp>
#include <boost/log/support/date_time.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/console.hpp>

namespace bsim {

void InitLogging() {
  namespace expr = boost::log::expressions;
  boost::log::add_common_attributes();
  boost::log::add_console_log(
      std::clog,
      boost::log::keywords::format =
          (expr::stream << expr::format_date_time<boost::posix_time::ptime>(
                               "TimeStamp", "%Y-%m-%dT%H:%M:%SZ")
                        << " [" << boost::log::trivial::severity << "] "
                        << expr::smessage));
}

}  // namespace bsim
