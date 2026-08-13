#pragma once

#include <string_view>

#include "bbp/drivers/chain_driver.h"
#include "bbp/simulation_registry.h"
#include "bbp/simulator/wallet_funding_strategy.h"
#include "bbp/simulator/wallet_transactions_workload.h"

namespace bbp {

struct Options;

namespace simulator_app_internal {

WalletFundingStrategy ParseWalletFundingStrategy(std::string_view value);
WalletTransferStrategy ParseWalletTransferStrategy(std::string_view value);
WalletPrivacyMode ParseWalletTransactionMode(std::string_view value);
WalletTransactionFeePolicy ParseWalletTransactionFeePolicy(
    std::string_view value);
ChainWalletMode ToChainWalletMode(WalletPrivacyMode mode);
ChainWalletMode ToChainWalletMode(const WalletInitialization& initialization);
void ValidateWalletTransactionsWorkload(
    const WalletTransactionsWorkload& workload, const Options& options);

}  // namespace simulator_app_internal
}  // namespace bbp
