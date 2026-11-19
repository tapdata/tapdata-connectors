# OceanBase CDC Runtime Artifact

The MySQL and Oracle connector JARs bundle both `linux-x86_64` and
`linux-aarch64` resources. On Linux/glibc the shared loader selects the helper
and runtime from the JVM architecture (`amd64`/`x86_64` or `arm64`/`aarch64`).
No user configuration is required. Each architecture has a separate cache and
checksum manifest under `~/.tapdata/native/oceanbase-cdc/4.4.2.1/`.
Alpine/musl, 32-bit ARM and other platforms are unsupported.

Build and install **both** runtime artifacts before building either connector:

```bash
./build-runtime.sh /path/to/x86_64/oceanbase /path/to/x86_64/libaio.so.1
./build-runtime.sh /path/to/aarch64/oceanbase /path/to/aarch64/libaio.so.1
```

Extract each vendor RPM into a separate directory with `rpm2cpio` and `cpio`;
do not install the ARM RPM over an existing x86 installation. The script detects
the target from the SDK's ELF header and checks the architecture-specific CentOS
7 `libaio-0.3.109-13.el7` checksum. It installs these Maven coordinates:

```text
io.tapdata.native:oceanbase-cdc-runtime:4.4.2.1:zip:linux-x86_64
io.tapdata.native:oceanbase-cdc-runtime:4.4.2.1:zip:linux-aarch64
```

Publish both ZIPs from `target/` to the Tapdata Maven repository for clean CI
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
