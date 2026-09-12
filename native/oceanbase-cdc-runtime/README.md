# OceanBase CDC Runtime Artifact

This directory builds the vendor `libobcdc` installation into the versioned Maven artifact consumed by the OceanBase connectors:

```text
io.tapdata.native:oceanbase-cdc-runtime:4.4.2.1:zip:linux-x86_64
```

Build and install it locally:

```bash
./build-runtime.sh /home/admin/oceanbase
```

The connector build only consumes the Maven artifact. It does not read `OBCDC_HOME`, does not require an OceanBase installation, and does not compile native code. Publish the generated ZIP to the Tapdata Maven repository before building on clean CI agents.

The artifact contains only Linux x86_64 runtime files. Redistribution must follow the applicable OceanBase license and Tapdata third-party software review process.
