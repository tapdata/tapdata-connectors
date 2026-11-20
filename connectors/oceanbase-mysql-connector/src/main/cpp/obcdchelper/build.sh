#!/usr/bin/env bash
set -euo pipefail
sdk_dir=${1:?Usage: build.sh /path/to/oceanbase-sdk [output]}
source_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
machine=$(readelf -h "$sdk_dir/lib64/libobcdc.so.4.4.2.3" | awk -F: '/Machine:/ {gsub(/^[ \t]+/, "", $2); print $2}')
case "$machine" in
  AArch64) platform=linux-aarch64 ;;
  "Advanced Micro Devices X86-64") platform=linux-x86_64 ;;
  *) echo "Unsupported libobcdc ELF machine: $machine" >&2; exit 2 ;;
esac
output=${2:-"$source_dir/../../resources/native/obcdc/4.4.2.3/$platform/obcdc-helper"}
mkdir -p "$(dirname -- "$output")"
# CXX may point to a cross compiler. Use an SDK and sysroot for the target.
# Hide our static STL: libobcdc exports a private STL that must stay separate.
"${CXX:-g++}" -std=c++11 -O2 -D_GLIBCXX_USE_CXX11_ABI=0 -DOB_USE_DRCMSG \
  -I"$sdk_dir/include" -I"$sdk_dir/include/libobcdc" -I"$sdk_dir/include/drcmsg" \
  "$source_dir/obcdc_helper.cpp" -L"$sdk_dir/lib64" \
  -Wl,-rpath-link,"${OBCDC_LIBAIO_DIR:-$sdk_dir/lib64}" \
  -Wl,-Bstatic -lstdc++ -Wl,-Bdynamic -lobcdc -Wl,--exclude-libs,ALL \
  -Wl,--disable-new-dtags,-rpath,'$ORIGIN/obcdc/lib64' -static-libstdc++ -static-libgcc -pthread -o "$output"
