# TapData Connectors

[![JDK](https://img.shields.io/badge/JDK-17+-green.svg)](https://openjdk.org/)
[![Maven](https://img.shields.io/badge/Maven-3.6+-blue.svg)](https://maven.apache.org/)

TapData Connectors 是用于开发和构建自定义数据连接器的开源项目，可用于 [TapData 数据同步平台](https://github.com/tapdata/tapdata)。通过本项目，开发者可以轻松构建自定义的数据连接器，并注册到 TapData 平台中，从而实现多种异构数据源的快速集成。

## 为什么选择 TapData Connectors？

- **丰富的连接器生态**：支持主流数据库、云服务、SaaS 应用等
- **快速开发**：基于 PDK 框架，几小时即可开发新连接器
- **企业级特性**：支持增量同步、断点续传等
- **跨平台支持**：支持 Windows、Linux、macOS 等主流操作系统
- **高性能**：优化的数据传输引擎，支持大数据量实时同步

## 🚀 快速上手

```bash
# 1️⃣ 克隆项目
git clone https://github.com/tapdata/tapdata-connectors.git && cd tapdata-connectors

# 2️⃣ 构建 MySQL 连接器（示例）
mvn clean install -pl connectors/mysql-connector -am -DskipTests

# 3️⃣ 注册到 TapData 平台
java -jar pdk-deploy.jar register \
  -a YOUR_ACCESS_CODE \
  -t http://localhost:3030 \
  connectors/mysql-connector/target/mysql-connector-*.jar
```

✅ **完成！** 现在你可以在 TapData 平台中使用这个连接器了。

## 项目结构

```bash
tapdata-connectors/
├── connectors                # 各类 Java 连接器
├── connectors-common         # 通用依赖与 Debezium 聚合模块
├── connectors-javascript     # JavaScript 连接器与核心
├── connectors-unpackage      # 未打包的特殊连接器
├── connectors-tdd            # 开发/测试驱动演示
├── file-storages             # 文件存储连接器
├── tapdata-cli               # CLI 工具
```

## 开发环境搭建

### 系统要求
- **JDK**：17 或更高版本
- **Maven**：3.6+
- **操作系统**：Windows / Linux / macOS

### 环境配置

#### Linux
```bash
# Ubuntu/Debian
sudo apt-get install openjdk-17-jdk maven

# CentOS/RHEL
sudo yum install java-17-openjdk-devel maven
```

#### macOS
```bash
# 自动检测并设置 JDK 17
export JAVA_HOME=$(/usr/libexec/java_home -v17 2>/dev/null || echo "/usr/lib/jvm/java-17-openjdk")
export PATH=$JAVA_HOME/bin:$PATH
java -version
```

#### Windows
1. 下载 [JDK 17](https://adoptium.net/) 并安装
2. 下载 [Maven](https://maven.apache.org/download.cgi) 并解压
3. 配置环境变量 `JAVA_HOME` 和 `MAVEN_HOME`

## 编译连接器

```bash
mvn clean install -DskipTests \
  -pl 'module-name'
  -am
```

**可选参数说明**：

- `-DskipTests`：跳过测试，推荐在编译时使用，以提高编译速度。
- `-pl`：仅构建选择的模块；如需排除某些模块，可在 `-pl` 中使用排除语法，例如 `!module-name1,!module-name2`。
- `-am`：自动构建被选模块的依赖。

编译执行完毕后，可在对应模块的 `target/*.jar` 下找到编译后的连接器 Jar 文件。随后，可跟随下述介绍，将其注册到 TapData 平台。


## 注册连接器到 TapData 平台

```bash
java -jar pdk-deploy.jar register -a ${access_code} -t ${tm_url} \
  [-ak ${accessKey} [-sk ${secretKey}]] [-r ${oem_type}] [-f ${filter_type}] [-l] [-h] [-X] \
  /path/to/your-connector.jar
```

**参数说明**：

- `-a`（`--auth`）：TapData 中的 `access_code`（访问码），可登录至 TapData 管理平台，点击右上角用户名，选择**个人设置**，即可查看。
- `-t`（`--tm`）：TapData 管理平台登录地址，例如 `http://localhost:3030`。
- `-f`（`--filter`）：仅注册指定认证类型的连接器，多个值用逗号分隔。
- `-l`（`--latest`）：替换为最新版本。
- `-h` (`--help`)：查看命令帮助信息。

其中，`pdk-deploy.jar` 工具，可在您的 TapData 部署环境中的 `tapdata/apps/lib` 目录中找到。

**执行示例**：

```bash
java -jar pdk-deploy.jar register \
  -a 3324***********8d4562f \
  -t http://127.0.0.1:3030 \
  connectors/starrocks-connector/target/starrocks-connector-1.0-SNAPSHOT.jar
```

连接器成功注册后会提示 “Completed”，随后，可登录至 TapData 管理页面，在创建连接时使用该连接器。


## 常见问题

Q：在 macOS 平台上编译时，提示 protoc 编译错误?

A：Apple Silicon（macOS ARM）建议排除 `debezium-connector-postgres` 与 `debezium-connector-highgo`，以避免本地二进制兼容性问题（见 `connectors-common/debezium-bucket/pom.xml:71-81` 的模块列表）。

编译命令示例：

```bash
mvn clean install -DskipTests \
  -pl '!connectors-common/debezium-bucket/debezium-connector-postgres,!connectors-common/debezium-bucket/debezium-connector-highgo'
  -am
```

## 如何贡献

我们非常欢迎更多贡献者来帮助改进与扩展连接器！**每一个贡献都很重要**，无论大小！

### 贡献方式

| 贡献类型 | 说明 | 难度 |
|---------|------|------|
| **报告 Bug** | 发现 Bug？欢迎提交 [Issue](https://github.com/tapdata/tapdata-connectors/issues) | ⭐ |
| **改进文档** | 修复错别字、补充示例、优化说明 | ⭐ |
| **新功能** | 开发新连接器或增强现有功能 | ⭐⭐⭐ |
| **代码优化** | 性能优化、代码重构 | ⭐⭐ |
| **测试用例** | 补充测试用例，提高代码质量 | ⭐⭐ |

### PR 模板建议
- **标题**：简洁明了，如 `Add MongoDB connector support`
- **描述**：说明变更背景、影响范围、测试结果
- **检查清单**：
  - [ ] 代码通过本地测试
  - [ ] 文档已更新
  - [ ] 遵循代码规范


## 联系我们

遇到问题？想要交流？我们提供多种支持渠道！

### 即时交流
<table>
<tr>
<td>
<a href="https://join.slack.com/t/tapdatacommunity/shared_invite/zt-1biraoxpf-NRTsap0YLlAp99PHIVC9eA">
<img src="https://img.shields.io/badge/Slack-加入频道-4A154B?style=flat&logo=slack&logoColor=white" alt="Slack"/>
</a>
</td>
<td>
<a href="https://twitter.com/tapdata_daas">
<img src="https://img.shields.io/badge/Twitter-关注动态-1DA1F2?style=flat&logo=twitter&logoColor=white" alt="Twitter"/>
</a>
</td>
<td>
<a href="https://github.com/tapdata/tapdata-connectors/issues">
<img src="https://img.shields.io/badge/GitHub-提交问题-181717?style=flat&logo=github&logoColor=white" alt="GitHub Issues"/>
</a>
</td>
</tr>
</table>

### 技术交流群
<p align="left">
<a href="https://20778419.s21i.faiusr.com/4/2/ABUIABAEGAAg-JPfhwYonMrzlwEwZDhk.png" rel="nofollow">
<img src="https://20778419.s21i.faiusr.com/4/2/ABUIABAEGAAg-JPfhwYonMrzlwEwZDhk.png" width="200" alt="微信群"/>
</a>
</p>
	
## 参考文档

- [TapData 社区版开源仓库](https://github.com/tapdata/tapdata)
- [TapData 在线文档](https://docs.tapdata.net/)

## 🎉 致谢

感谢所有为 TapData Connectors 做出贡献的开发者、用户和支持者。正是有了你们的支持，这个项目才能不断发展壮大。

如果这个项目对你有帮助，欢迎 Star 支持我们！

**Contributors**

<a href="https://github.com/tapdata/tapdata-connectors/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=tapdata/tapdata-connectors" alt="贡献者"/>
</a>
