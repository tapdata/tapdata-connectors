# OceanBase CDC Runtime Artifact

This directory builds the vendor `libobcdc` installation into the versioned Maven artifact consumed by the OceanBase connectors:

```text
io.tapdata.native:oceanbase-cdc-runtime:4.4.2.1:zip:linux-x86_64
```

Build and install it locally:

```bash
./build-runtime.sh /home/admin/oceanbase
```

The build also bundles CentOS 7 `libaio-0.3.109-13.el7.x86_64`, which is a
runtime dependency of `libobcdc`. The script locates `libaio.so.1` through
`ldconfig`, verifies its SHA-256, and stores it beside `libobcdc`. A specific
library path can be supplied as the second argument:

```bash
./build-runtime.sh /home/admin/oceanbase /usr/lib64/libaio.so.1
```

The connector build only consumes the Maven artifact. It does not read `OBCDC_HOME`, does not require an OceanBase installation, and does not compile native code. Publish the generated ZIP to the Tapdata Maven repository before building on clean CI agents.

The artifact contains only Linux x86_64 runtime files. `libaio` is distributed
under LGPLv2+; OceanBase files remain subject to their applicable license.
Redistribution must follow the Tapdata third-party software review process.
