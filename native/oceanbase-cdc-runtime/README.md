# OceanBase CDC Runtime Artifact

The MySQL and Oracle connector JARs bundle both `linux-x86_64` and
`linux-aarch64` resources. On Linux/glibc the shared loader selects the helper
and runtime from the JVM architecture (`amd64`/`x86_64` or `arm64`/`aarch64`).
No user configuration is required. Each architecture has a separate cache and
checksum manifest under `~/.tapdata/native/oceanbase-cdc/4.4.2.3/`.
Alpine/musl, 32-bit ARM and other platforms are unsupported.

Build and install **both** runtime JAR artifacts before building either connector:

```bash
./build-runtime.sh /path/to/x86_64/oceanbase /path/to/x86_64/libaio.so.1
./build-runtime.sh /path/to/aarch64/oceanbase /path/to/aarch64/libaio.so.1
```

Extract each vendor RPM into a separate directory with `rpm2cpio` and `cpio`;
do not install the ARM RPM over an existing x86 installation. The script detects
the target from the SDK's ELF header and checks the architecture-specific CentOS
7 `libaio-0.3.109-13.el7` checksum. It installs these Maven coordinates:

```text
io.tapdata.native:oceanbase-cdc-runtime:4.4.2.3:jar:linux-x86_64
io.tapdata.native:oceanbase-cdc-runtime:4.4.2.3:jar:linux-aarch64
```

Publish both JARs from `target/` to the Tapdata Maven repository for clean CI
agents. Normal connector builds consume the artifacts and checked-in helpers;
they do not require an SDK or native compiler.

To rebuild a helper, run `src/main/cpp/obcdchelper/build.sh` in the corresponding
connector. The script selects its resource directory from the SDK architecture.
For example, cross compiling ARM on x86:

```bash
OBCDC_LIBAIO_DIR=/path/to/aarch64/libaio-directory \
CXX=/path/to/aarch64-toolchain/bin/aarch64-none-linux-gnu-g++ \
  ./src/main/cpp/obcdchelper/build.sh /path/to/aarch64/oceanbase
```

Ensure `libaio.so.1` in that directory resolves to the target ARM binary (RPMs
may contain absolute symlinks that need to be made relative after extraction).
The default lookup directory is the SDK `lib64` directory.

Use a compiler/sysroot compatible with the deployment glibc (2.17 baseline).
Both helpers statically link and hide their C++ runtime to avoid interference
with libobcdc's private STL. The optional Maven `-Dobcdc.helper.build` profile
also supports `-Dobcdc.helper.compiler=/path/to/compiler` and
`-Dobcdc.helper.platform=linux-aarch64` for cross compilation; on ARM Linux the
target platform is selected automatically. Set
`-Dobcdc.helper.libaio.dir=/path/to/target/libaio-directory` when libaio is
outside the SDK.

`libaio` is distributed under LGPLv2+; OceanBase files remain subject to their
applicable license. Redistribution must follow the Tapdata third-party software
review process.

## 4.4.2.3 defaults and connection options

The runtime JARs use the vendor RPMs
`oceanbase-cdc-4.4.2.3-103000452026111012.el7.{x86_64,aarch64}.rpm`.
The shared loader is also versioned `4.4.2.3`; this avoids changing the runtime
used by existing connectors that depend on loader `1.0-SNAPSHOT`.

Both connectors expose **CDC memory limit** (`cdcMemoryLimit`) when native CDC
is enabled. The per-task default is `2G`; positive integer `M`/`G` values such as
`512M`, `2G`, and `4G` are accepted. Missing, null or blank values use `2G`.
This is a libobcdc limit, separate from the engine JVM heap, and is not a promise
that a machine with exactly 2GB RAM can run the engine and CDC together.

Packaging also bounds startup allocation: reader/formatter threads are 2,
tenant-manager ceiling is 1G, LOB/reader/sequencer/storager queues are 1024,
record/log-entry/transaction preallocation counts are 1000, and transaction
preallocation pages are 128. Large workloads may need a higher memory limit;
raising it does not automatically enlarge these queues.

The earlier `getconf`-based runtime selection and optional
`linux-aarch64-pagesize-65536` artifact have been removed. Selection now depends
only on architecture. The additional ARM 4KB-only guard and `/proc/self/auxv`
dependency have also been removed. The vendor runtime now handles the host page
size; the loader does not reject 16KB or 64KB kernels before starting the helper.

On a QEMU system emulator booting a CentOS ARM kernel with
`CONFIG_ARM64_64K_PAGES=y`, the original 4.4.2.3 helper initializes and starts
streaming with the packaged 2G defaults. Both kernel `AT_PAGESZ` and libc report
65536 without overrides. The bundled `_rjem` allocator diagnostic still returns
11 and prints `Unsupported system page size`; that result alone does **not**
establish that native CDC cannot run. See [validation details](64k-validation.md).
System emulation verifies the kernel page behavior; it does not establish
performance or stability on customer hardware. No 16KB-kernel integration test
has been performed.

### Reproduce the allocator diagnostic on the target kernel

`allocator-probe.c` checks both the kernel's `AT_PAGESZ` and libc's page size
before loading the vendor library and exercising its exported `_rjem` allocator.
Build with the target architecture's C compiler, then run on the target kernel:

```bash
cc -std=c99 -O2 -Wall -Wextra -Werror allocator-probe.c -ldl -o allocator-probe
LD_LIBRARY_PATH=/path/to/runtime/lib64 \
  ./allocator-probe /path/to/runtime/lib64/libobcdc.so.4.4.2.3 65536
```

Exit 0 means only that the allocator diagnostic passed; native CDC initialization
and committed INSERT/UPDATE/DELETE verification remain required. Exit 1 means the
tested allocator path failed; it is not a CDC compatibility gate. Exit 2 means
the diagnostic is inconclusive (wrong kernel page
size, missing symbols, or a load error). Do not override `sysconf()` or
`getpagesize()` with `LD_PRELOAD` for this test. A system emulator booting a
64KB ARM kernel can reproduce kernel page behavior; user-mode instruction
emulation or spoofing libc return values does not replace that test.

Publish the loader JAR/POM and both runtime JARs/POM as new `4.4.2.3` releases to
both Nexus installations. Verify complete downloads from `maven-public`, not
only the hosted repository or checksum endpoints. Keep existing versions intact.

The runtime archive is distributed with Maven type `jar` (classifiers
`linux-x86_64` and `linux-aarch64`). Its contents are still a standard ZIP/JAR,
and the connector copies it to its internal `runtime.zip` resource. This avoids
the public repository gateway's ZIP routing issue: `.zip` artifacts return 404
from `maven-public` while the corresponding `.jar` artifacts are served there.
The library/config bytes and loader verification remain identical. Legacy ZIP
aliases may coexist in hosted repositories; new connector builds use the JARs.
