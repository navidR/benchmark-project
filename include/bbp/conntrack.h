#pragma once

#include <cstdint>
#include <filesystem>

namespace bbp {

inline constexpr std::uint32_t kMinimumConntrackCapacity = 1'048'576U;

// Raises the shared kernel limit, verifies read-back, and never restores it.
// An explicit path lets tests use a file without changing kernel settings.
std::uint32_t EnsureConntrackCapacity(
    const std::filesystem::path& limit_path =
        "/proc/sys/net/netfilter/nf_conntrack_max");

}  // namespace bbp
