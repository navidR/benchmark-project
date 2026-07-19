#include <boost/test/unit_test.hpp>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/util.h"

namespace {

bool IsSourceFile(const std::filesystem::path& path) {
  const std::string extension = path.extension().string();
  return extension == ".cpp" || extension == ".h" || extension == ".cc" ||
         extension == ".hpp";
}

void CheckDirectoryForForbiddenTokens(const std::filesystem::path& directory) {
  const std::vector<std::string_view> forbidden = {
      "system(",       "system (",       "popen(",  "popen (",
      "execvp(",       "execvp (",       "execlp(", "execlp (",
      "posix_spawnp(", "posix_spawnp (", "/bin/sh", "bash -c",
  };

  for (const auto& entry :
       std::filesystem::recursive_directory_iterator(directory)) {
    if (!entry.is_regular_file() || !IsSourceFile(entry.path())) {
      continue;
    }
    const std::string text = bbp::ReadText(entry.path());
    for (std::string_view token : forbidden) {
      if (text.find(token) != std::string::npos) {
        BOOST_FAIL("forbidden shell-out token '" << token << "' in "
                                                 << entry.path().string());
      }
    }
  }
}

}  // namespace

BOOST_AUTO_TEST_CASE(simulator_source_does_not_shell_out) {
  const std::filesystem::path source_root = BBP_SOURCE_DIR;
  CheckDirectoryForForbiddenTokens(source_root / "include");
  CheckDirectoryForForbiddenTokens(source_root / "src");
}
