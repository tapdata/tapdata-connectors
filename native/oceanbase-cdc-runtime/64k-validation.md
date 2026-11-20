# ARM 64KB kernel validation — 2026-11-20

The 4.4.2.3 MySQL native helper initialized and streamed committed DML on an
ARM Linux kernel with 64KB pages under **QEMU system emulation**. This is a
functional kernel compatibility test, not a physical ARM hardware or customer
environment stability test. Oracle native CDC and 16KB kernels were not tested
in this run.

## Environment and identity

- QEMU system AArch64 6.2, `virt,gic-version=3`, CPU `max`, TCG, 2 vCPUs, 6GB RAM.
- CentOS kernel `4.18.0-193.28.1.el7.aarch64`, `CONFIG_ARM64_64K_PAGES=y`.
- Kernel and libc both report **65536** bytes; no `LD_PRELOAD` or page-size
  overrides. The library and helper execute inside the guest kernel, rather
  than through user-mode QEMU on the x86 host kernel.
- Official RPM: `oceanbase-cdc-4.4.2.3-103000452026111012.el7.aarch64.rpm`.
- Runtime library SHA256:
  `ec9e89bfdca8df2a4fffdb607ad6a4a4bed6fbc32f529b8c76a51927fe5ff70c`.
- Checked-in MySQL AArch64 helper SHA256:
  `8cacef6c5969006a1cdfaa8b41eef79652c2fafdc91f5b9674a7ea853514d0ce`.
- Packaged runtime configuration with `memory_limit=2G`, bounded queues and
  preallocations documented in README; no allocator substitutions or binary patches.

Kernel RPM source:
<https://vault.centos.org/altarch/7.9.2009/os/aarch64/Packages/kernel-core-4.18.0-193.28.1.el7.aarch64.rpm>.
Its SHA256 is `3fdb115e18e28a5ced37115ff51829208fc4cbcd8f95893aef1dc558cd28da96`;
the decompressed kernel Image SHA256 is
`88b1e7ad1b49eecf8476d2af8ccd504a635c45cd82d0024c6ee2063db0a7ea8e`.

## Results

The original helper connected to the existing OceanBase integration database
through virtio networking. A dedicated table received 55 committed
INSERT/UPDATE/DELETE sequences and 55 rolled-back insert controls. Its framed
stdout was captured inside the guest and parsed on the host with the connector's
generated `ReadLogPayload` classes.

```text
KERNEL: 4.18.0-193.28.1.el7.aarch64 aarch64 page=65536
[obcdc-helper] 2026-11-20 16:28:03 init ok
[obcdc-helper] 2026-11-20 16:28:04 launch ok, streaming records
PASS: frames=370 committedDml=165 completeDmlTriples=55; values, event order and rollback exclusion verified
```

Helper startup took approximately 144 seconds. Captured framed output contains
18429 bytes, SHA256
`dc2f2b2944ff0c9fda4060d2228c034fc2d11d32621d81ab8942af77e5ac7561`.
The harness stopped the helper after its fixed 360-second observation interval
(SIGTERM followed by SIGKILL after five seconds); this was a controlled test stop,
not an initialization failure. The dedicated test table was removed afterwards.

The exported `_rjem_mallctl("arenas.page")` diagnostic returns 11 and prints
`Unsupported system page size` on this same kernel. The helper prints this
warning too, then successfully initializes and streams DML. Thus that diagnostic
does not establish that the CDC path fails. It must not justify a blanket
Java-side rejection of all non-4KB ARM kernels.

The loader's page-size guard, auxv parser and rejection tests were removed.
Architecture selection and Linux/glibc checks remain. All three remaining
loader tests pass. Both connectors were rebuilt, and each packaged loader class,
both runtime archive hashes, and both helpers were checked against their sources.
Both packages retain the configurable default memory limit of 2G.

## Evidence and reproduction

Local evidence is under `/home/obcdc-64k-test/`:

- `console-network.log`, `frames.bin`, `event-validation.log`, `dml.log`.
- `init-network.c`, `write-dml.py`, `VerifyFrames.java` describe the guest runner,
  host DML writer, and protocol/row assertions. Test credentials are read from
  the existing private integration fixture; credential-bearing temporary guest
  inputs and initramfs archives were deleted after the test.
- `console-max.log` and `probe-x86.log` contain the standalone allocator diagnostic
  and x86 4KB control. The earlier Cortex-A72 attempt hit SIGILL and is not used
  as a page-size compatibility result; the completed test uses CPU `max`.
- `loader-fixed-build.log`, `mysql-fixed-build.log`, `oracle-fixed-build.log`,
  and `artifacts.json` contain build and package verification results.

The system-emulator invocation was:

```bash
qemu-system-aarch64 \
  -machine virt,gic-version=3 -cpu max -accel tcg,thread=multi -smp 2 -m 6144 \
  -netdev user,id=n0 -device virtio-net-pci,netdev=n0,romfile= \
  -nographic -no-reboot -kernel Image -initrd initramfs-network.cpio \
  -append 'console=ttyAMA0 loglevel=4 panic=-1'
```

`nexus-loader-verification.json` records complete binary downloads of the existing
4.4.2.3 loader from internal and external Nexus `maven-public`. Both class files
match the fixed loader exactly (SHA256
`7a57e2c7f171264e1a7920ff3b53e616f4d312c967e742e88ddbb3323c28e261`).
The previously published loader already had architecture-only selection, so no
release overwrite was necessary. `nexus-runtime-verification.json` verifies
public artifact sizes and SHA256 sidecars for both runtime classifiers on both
Nexus installations; it does not claim complete runtime binary downloads in
this run. Runtime bytes were unchanged by the guard rollback.

MySQL packaging uses command-line PDK 2.11/2.0.11 overrides because unrelated
workspace POM changes reference unavailable 2.12 snapshots. Those POM changes
were preserved. Connector packaging skipped tests; loader tests and the native
64KB functional test above were run separately.
