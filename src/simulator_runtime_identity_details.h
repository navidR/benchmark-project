#pragma once

#include <boost/json/object.hpp>
#include <cstdint>
#include <span>
#include <string>

namespace bbp {

class RuntimeNodeSnapshot;
class RuntimeWalletSnapshot;
struct MasternodeIdentity;
struct NodeRoleTopology;
struct Options;
struct WalletIdentity;
struct WalletInitialization;

namespace simulator_app_internal {

std::string NodeRoleName(const NodeRoleTopology& topology,
                         std::uint32_t node_index);
std::string NodeRoleName(const Options& options, std::uint32_t node_index,
                         const NodeRoleTopology* runtime_topology = nullptr);
std::string WalletAddressDetail(const WalletIdentity& wallet,
                                const WalletInitialization& initialization);
boost::json::object RuntimeWalletIdentityJson(
    const WalletIdentity& wallet, const WalletInitialization& initialization);
boost::json::object RuntimeMasternodeIdentityJson(
    const MasternodeIdentity& masternode);
boost::json::object RuntimeWalletGenerationDetail(
    const RuntimeWalletSnapshot& snapshot,
    std::span<const WalletIdentity> added_wallets,
    std::span<const WalletIdentity> removed_wallets = {});
boost::json::object RuntimeRoleGenerationDetail(
    const RuntimeWalletSnapshot& snapshot, const RuntimeNodeSnapshot& nodes);

}  // namespace simulator_app_internal
}  // namespace bbp
