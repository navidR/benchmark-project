# Vendored Linux libperf

This directory contains the official Linux `libperf` sources and the minimal
Linux tools support tree needed to build them. The files were extracted without
source changes from the official Linux `v7.1` release archive:

- Upstream repository: <https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git>
- Release archive: <https://cdn.kernel.org/pub/linux/kernel/v7.x/linux-7.1.tar.xz>
- Release tag: `v7.1`
- Archive SHA-256:
  `691f44797fbe790dc8a321604c927087526ad27b6d649925d60f8eed0a2564a0`

Vendored paths are `tools/lib/perf`, `tools/lib/api`, `tools/lib/zalloc.c`,
`tools/lib/str_error_r.c`, `tools/include`, `tools/arch`, `COPYING`, and
`LICENSES`. Keeping the upstream layout lets libperf use its official public
headers while the project builds the C sources directly into private static
libraries. No generated objects or installed binaries are vendored.

The unused `tools/include/linux/pci_ids.h` symlink is omitted because its
target is outside this minimal snapshot. All included upstream files retain
their exact release-archive contents.
