#include "bbp/drivers/chain_driver.h"

#include <stdexcept>

namespace bbp {

UnsupportedChainOperation::UnsupportedChainOperation(std::string_view chain,
                                                     std::string_view operation)
    : std::runtime_error(std::string(chain) + " does not support " +
                         std::string(operation) + " functionality.") {}

std::string_view ChainLogSourceName(ChainLogSource source) {
  switch (source) {
    case ChainLogSource::kDaemon:
      return "daemon_log";
    case ChainLogSource::kStdout:
      return "stdout";
    case ChainLogSource::kStderr:
      return "stderr";
  }
  throw std::runtime_error("unknown chain log source");
}

}  // namespace bbp
