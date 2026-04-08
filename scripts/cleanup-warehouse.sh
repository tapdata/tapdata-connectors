#!/bin/bash
################################################################################
# Paimon 仓库清理脚本
# 功能：清理测试仓库目录，释放磁盘空间
# 使用：./scripts/cleanup-warehouse.sh
################################################################################

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
WAREHOUSE_DIR="${PROJECT_ROOT}/test-output/paimon-warehouse"

echo "========================================"
echo "  Paimon 仓库清理脚本"
echo "========================================"
echo ""

# 检查目录是否存在
if [ ! -d "$WAREHOUSE_DIR" ]; then
    echo "仓库目录不存在：$WAREHOUSE_DIR"
    exit 0
fi

# 计算目录大小
if command -v du &> /dev/null; then
    dir_size=$(du -sh "$WAREHOUSE_DIR" 2>/dev/null | cut -f1)
    echo "当前仓库大小：$dir_size"
else
    echo "当前仓库：$WAREHOUSE_DIR"
fi
echo ""

# 确认清理（如果不是强制模式）
if [ "$1" != "-f" ] && [ "$1" != "--force" ]; then
    read -p "确定要清理仓库目录吗？(y/N): " confirm
    if [[ ! $confirm =~ ^[Yy]$ ]]; then
        echo "已取消清理"
        exit 0
    fi
fi

# 执行清理
echo "正在清理..."
rm -rf "$WAREHOUSE_DIR"/*
mkdir -p "$WAREHOUSE_DIR"

# 清理 JVM 缓存（macOS）
if command -v sudo &> /dev/null && command -v purge &> /dev/null; then
    echo "清理 JVM 缓存..."
    sudo sync 2>/dev/null || true
    sudo purge 2>/dev/null || true
fi

echo ""
echo "========================================"
echo "  清理完成！"
echo "========================================"
