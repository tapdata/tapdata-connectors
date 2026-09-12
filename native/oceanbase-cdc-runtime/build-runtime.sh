#!/usr/bin/env bash
set -euo pipefail

runtime_home="${1:-}"
if [[ -z "${runtime_home}" || ! -d "${runtime_home}" ]]; then
  echo "Usage: $0 <OceanBase obcdc installation directory>" >&2
  exit 2
fi

base_dir="$(cd "$(dirname "$0")" && pwd)"
work_dir="${base_dir}/target/runtime"
archive="${base_dir}/target/oceanbase-cdc-runtime-4.4.2.1-linux-x86_64.zip"
rm -rf "${base_dir}/target"
mkdir -p "${work_dir}/lib64" "${work_dir}/etc"

cp "${runtime_home}/lib64/libobcdc.so.4.4.2.1" "${work_dir}/lib64/"
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
