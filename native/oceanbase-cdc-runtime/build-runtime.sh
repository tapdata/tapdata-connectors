#!/usr/bin/env bash
set -euo pipefail

runtime_home="${1:-}"
if [[ -z "${runtime_home}" || ! -d "${runtime_home}" ]]; then
  echo "Usage: $0 <OceanBase obcdc installation directory> [libaio.so.1 path]" >&2
  exit 2
fi

libaio_source="${2:-}"
if [[ -z "${libaio_source}" ]]; then
  libaio_source="$(ldconfig -p 2>/dev/null | awk '/libaio\.so\.1 \(libc6,x86-64\)/ { print $NF; exit }')"
fi
if [[ -z "${libaio_source}" || ! -e "${libaio_source}" ]]; then
  echo "libaio.so.1 was not found; install libaio or pass its path as the second argument" >&2
  exit 2
fi
libaio_source="$(readlink -f "${libaio_source}")"

expected_libaio_sha256="74c6bb0192ba9b116c2567d88056b0d5c62f44bb6af7032e928ef452b0e132b5"
actual_libaio_sha256="$(sha256sum "${libaio_source}" | awk '{print $1}')"
if [[ "${actual_libaio_sha256}" != "${expected_libaio_sha256}" ]]; then
  echo "Unsupported libaio.so.1 binary: ${libaio_source}" >&2
  echo "Expected CentOS 7 libaio-0.3.109-13.el7.x86_64 SHA-256 ${expected_libaio_sha256}, got ${actual_libaio_sha256}" >&2
  exit 2
fi

base_dir="$(cd "$(dirname "$0")" && pwd)"
work_dir="${base_dir}/target/runtime"
archive="${base_dir}/target/oceanbase-cdc-runtime-4.4.2.1-linux-x86_64.zip"
rm -rf "${base_dir}/target"
mkdir -p "${work_dir}/lib64" "${work_dir}/etc"

cp "${runtime_home}/lib64/libobcdc.so.4.4.2.1" "${work_dir}/lib64/"
cp "${libaio_source}" "${work_dir}/lib64/libaio.so.1.0.1"
cp "${runtime_home}/etc/libobcdc.conf" "${work_dir}/etc/"
cp "${runtime_home}/etc/obcdc_compatiable_ob_info.yaml" "${work_dir}/etc/"
cp "${runtime_home}/etc/timezone_info.conf" "${work_dir}/etc/"

(
  cd "${work_dir}"
  jar cf "${archive}" lib64 etc
)

mvn org.apache.maven.plugins:maven-install-plugin:3.1.3:install-file \
  -Dfile="${archive}" \
  -DpomFile="${base_dir}/pom.xml" \
  -Dpackaging=zip \
  -Dclassifier=linux-x86_64

sha256sum "${archive}"
