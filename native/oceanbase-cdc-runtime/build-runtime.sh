#!/usr/bin/env bash
set -euo pipefail

runtime_home="${1:-}"
if [[ -z "${runtime_home}" || ! -d "${runtime_home}" ]]; then
  echo "Usage: $0 <OceanBase obcdc installation directory> [libaio.so.1 path]" >&2
  exit 2
fi

machine="$(readelf -h "${runtime_home}/lib64/libobcdc.so.4.4.2.3" | awk -F: '/Machine:/ {gsub(/^[ \t]+/, "", $2); print $2}')"
case "${machine}" in
  "Advanced Micro Devices X86-64")
    platform=linux-x86_64
    expected_libaio_sha256=74c6bb0192ba9b116c2567d88056b0d5c62f44bb6af7032e928ef452b0e132b5 ;;
  AArch64)
    platform=linux-aarch64
    expected_libaio_sha256=db543aabc24abf97ba99965c31e947876023d1bba3277b81ffa5a9edff6c86a1 ;;
  *) echo "Unsupported libobcdc ELF machine: ${machine}" >&2; exit 2 ;;
esac
libaio_source="${2:-}"
if [[ -z "${libaio_source}" ]]; then
  libaio_source="$(ldconfig -p 2>/dev/null | awk '/libaio\.so\.1 / { print $NF; exit }')"
fi
if [[ -z "${libaio_source}" || ! -e "${libaio_source}" ]]; then
  echo "Pass the target architecture's CentOS 7 libaio.so.1 as the second argument" >&2
  exit 2
fi
libaio_source="$(readlink -f "${libaio_source}")"
actual_libaio_sha256="$(sha256sum "${libaio_source}" | awk '{print $1}')"
if [[ "${actual_libaio_sha256}" != "${expected_libaio_sha256}" ]]; then
  echo "Unsupported libaio.so.1 for ${platform}: expected ${expected_libaio_sha256}, got ${actual_libaio_sha256}" >&2
  exit 2
fi

base_dir="$(cd "$(dirname "$0")" && pwd)"
work_dir="${base_dir}/target/${platform}/runtime"
archive="${base_dir}/target/oceanbase-cdc-runtime-4.4.2.3-${platform}.jar"
rm -rf "${work_dir}"
mkdir -p "${work_dir}/lib64" "${work_dir}/etc"

cp "${runtime_home}/lib64/libobcdc.so.4.4.2.3" "${work_dir}/lib64/"
cp "${libaio_source}" "${work_dir}/lib64/libaio.so.1.0.1"
cp "${runtime_home}/etc/libobcdc.conf" "${work_dir}/etc/"
cp "${runtime_home}/etc/obcdc_compatiable_ob_info.yaml" "${work_dir}/etc/"
cp "${runtime_home}/etc/timezone_info.conf" "${work_dir}/etc/"

# Bound startup allocations as well as the per-task memory ceiling. These
# defaults are shared by both architectures; the connector overrides memory_limit.
python3 - "${work_dir}/etc/libobcdc.conf" <<'PYCONF'
import pathlib, sys
path = pathlib.Path(sys.argv[1])
settings = {
    "memory_limit": "2G",
    "tenant_manager_memory_upper_limit": "1G",
    "reader_thread_num": "2",
    "formatter_thread_num": "2",
    "lob_data_merger_queue_length": "1024",
    "binlog_record_prealloc_count": "1000",
    "log_entry_task_prealloc_count": "1000",
    "part_trans_task_prealloc_count": "1000",
    "part_trans_task_prealloc_page_count": "128",
    "reader_queue_length": "1024",
    "sequencer_queue_length": "1024",
    "storager_queue_length": "1024",
}
lines = []
for line in path.read_text().splitlines():
    key = line.partition("=")[0].strip()
    lines.append(key + "=" + settings.pop(key) if key in settings else line)
lines.extend(key + "=" + value for key, value in settings.items())
path.write_text("\n".join(lines) + "\n")
PYCONF

(
  cd "${work_dir}"
  jar cf "${archive}" lib64 etc
)

mvn -N -f "${base_dir}/pom.xml" org.apache.maven.plugins:maven-install-plugin:3.1.3:install-file \
  -Dfile="${archive}" \
  -DpomFile="${base_dir}/pom.xml" \
  -Dpackaging=jar \
  -Dclassifier="${platform}"

sha256sum "${archive}"
