#!/bin/bash

# StarRocks多表缓存功能调试测试脚本

echo "=== StarRocks多表缓存功能调试测试 ==="
echo "此脚本将编译并运行测试，显示详细的调试日志"
echo ""

# 进入StarRocks连接器目录
cd ../../connectors/starrocks-connector

echo "1. 编译StarRocks连接器..."
mvn compile -q
if [ $? -ne 0 ]; then
    echo "❌ 编译失败"
    exit 1
fi
echo "✅ 编译成功"

echo ""
echo "2. 编译测试文件..."
# 编译测试文件
CLASSPATH=$(find target/classes -name "*.class" | head -1 | xargs dirname)
CLASSPATH="$CLASSPATH:$(find ../../ -name "*.jar" | grep -v target | tr '\n' ':')"

javac -cp "$CLASSPATH" ../../changes/feat-starrocks-multi-table-cache/test_columns_debug.java
if [ $? -ne 0 ]; then
    echo "❌ 测试文件编译失败"
    exit 1
fi
echo "✅ 测试文件编译成功"

echo ""
echo "3. 运行调试测试..."
echo "注意：测试会产生一些预期的异常（因为没有实际的StarRocks服务器），但会显示我们需要的调试信息"
echo ""

# 运行测试
java -cp "$CLASSPATH:../../changes/feat-starrocks-multi-table-cache/" TestColumnsDebug

echo ""
echo "=== 测试完成 ==="
echo ""
echo "📋 日志说明："
echo "- [INFO] 级别的日志显示关键的处理步骤和数据结构内容"
echo "- [DEBUG] 级别的日志显示每个列的详细处理过程"
echo "- tableDataColumns: 当前事件中实际包含数据的列"
echo "- tapTable.getNameFieldMap(): 表结构中定义的所有列"
echo "- shouldInclude: 是否应该包含在最终的columns列表中"
echo ""
echo "🔍 关键验证点："
echo "1. 不同表的dataColumns是否独立存储"
echo "2. columns构建是否只包含实际有数据的列"
echo "3. 多表处理时是否不会相互影响"

# 清理编译产生的class文件
rm -f ../../changes/feat-starrocks-multi-table-cache/*.class
