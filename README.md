# TapData Connectors

[![JDK](https://img.shields.io/badge/JDK-17+-green.svg)](https://openjdk.org/)
[![Maven](https://img.shields.io/badge/Maven-3.6+-blue.svg)](https://maven.apache.org/)

TapData Connectors is an open-source project for developing and building custom data connectors, designed for the [TapData data synchronization platform](https://github.com/tapdata/tapdata). Through this project, developers can easily build custom data connectors and register them with the TapData platform, enabling rapid integration of various heterogeneous data sources.

## Why Choose TapData Connectors?

- **Rich Connector Ecosystem**: Supports mainstream databases, cloud services, SaaS applications, and more
- **Rapid Development**: Based on the PDK framework, new connectors can be developed in just a few hours
- **Enterprise Features**: Supports incremental synchronization, resume from breakpoint, and more
- **Cross-Platform Support**: Compatible with Windows, Linux, macOS, and other mainstream operating systems
- **High Performance**: Optimized data transmission engine supporting real-time synchronization of large data volumes

## 🚀 Quick Start

```bash
# 1️⃣ Clone the project
git clone https://github.com/tapdata/tapdata-connectors.git && cd tapdata-connectors

# 2️⃣ Build MySQL connector (example)
mvn clean install -pl connectors/mysql-connector -am -DskipTests

# 3️⃣ Register with TapData platform
java -jar pdk-deploy.jar register \
  -a YOUR_ACCESS_CODE \
  -t http://localhost:3030 \
  connectors/mysql-connector/target/mysql-connector-*.jar
```

✅ **Done!** Now you can use this connector in the TapData platform.

## Project Structure

```bash
tapdata-connectors/
├── connectors                # Various Java connectors
├── connectors-common         # Common dependencies and Debezium aggregation modules
├── connectors-javascript     # JavaScript connectors and core
├── connectors-unpackage      # Unpackaged special connectors
├── connectors-tdd            # Development/test-driven demonstrations
├── file-storages             # File storage connectors
├── tapdata-cli               # CLI tools
```

## Development Environment Setup

### System Requirements
- **JDK**: 17 or higher
- **Maven**: 3.6+
- **Operating System**: Windows / Linux / macOS

### Environment Configuration

#### Linux
```bash
# Ubuntu/Debian
sudo apt-get install openjdk-17-jdk maven

# CentOS/RHEL
sudo yum install java-17-openjdk-devel maven
```

#### macOS
```bash
# Auto-detect and set JDK 17
export JAVA_HOME=$(/usr/libexec/java_home -v17 2>/dev/null || echo "/usr/lib/jvm/java-17-openjdk")
export PATH=$JAVA_HOME/bin:$PATH
java -version
```

#### Windows
1. Download and install [JDK 17](https://adoptium.net/)
2. Download and extract [Maven](https://maven.apache.org/download.cgi)
3. Configure environment variables `JAVA_HOME` and `MAVEN_HOME`

## Build Connectors

```bash
mvn clean install -DskipTests \
  -pl 'module-name'
  -am
```

**Optional Parameters**:

- `-DskipTests`: Skip tests, recommended during compilation to improve build speed
- `-pl`: Build only selected modules; to exclude certain modules, use exclusion syntax in `-pl`, e.g., `!module-name1,!module-name2`
- `-am`: Automatically build dependencies of selected modules

After compilation, you can find the compiled connector JAR files in the corresponding module's `target/*.jar`. Then follow the instructions below to register them with the TapData platform.


## Register Connector to TapData Platform

```bash
java -jar pdk-deploy.jar register -a ${access_code} -t ${tm_url} \
  [-ak ${accessKey} [-sk ${secretKey}]] [-r ${oem_type}] [-f ${filter_type}] [-l] [-h] [-X] \
  /path/to/your-connector.jar
```

**Parameter Description**:

- `-a` (`--auth`): The `access_code` in TapData. Log in to the TapData management platform, click your username in the upper right corner, select **Personal Settings** to view it
- `-t` (`--tm`): TapData management platform login address, e.g., `http://localhost:3030`
- `-f` (`--filter`): Register only connectors of specified authentication types, multiple values separated by commas
- `-l` (`--latest`): Replace with the latest version
- `-h` (`--help`): View command help information

The `pdk-deploy.jar` tool can be found in your TapData deployment environment's `tapdata/apps/lib` directory.

**Execution example**:

```bash
java -jar pdk-deploy.jar register \
  -a 3324***********8d4562f \
  -t http://127.0.0.1:3030 \
  connectors/starrocks-connector/target/starrocks-connector-1.0-SNAPSHOT.jar
```

When the connector is successfully registered, it will prompt "Completed". Then you can log in to the TapData management page and use this connector when creating connections.


## FAQ

Q: Getting protoc compilation errors when building on macOS?

A: For Apple Silicon (macOS ARM), it's recommended to exclude `debezium-connector-postgres` and `debezium-connector-highgo` to avoid local binary compatibility issues (see module list in `connectors-common/debezium-bucket/pom.xml:71-81`).

Build command example:

```bash
mvn clean install -DskipTests \
  -pl '!connectors-common/debezium-bucket/debezium-connector-postgres,!connectors-common/debezium-bucket/debezium-connector-highgo'
  -am
```

## How to Contribute

We warmly welcome more contributors to help improve and expand connectors! **Every contribution matters**, no matter how big or small!

### Contribution Methods

| Contribution Type | Description | Difficulty |
|---------|------|------|
| **Report Bugs** | Found a bug? Feel free to submit an [Issue](https://github.com/tapdata/tapdata-connectors/issues) | ⭐ |
| **Improve Documentation** | Fix typos, add examples, optimize descriptions | ⭐ |
| **New Features** | Develop new connectors or enhance existing functionality | ⭐⭐⭐ |
| **Code Optimization** | Performance optimization, code refactoring | ⭐⭐ |
| **Test Cases** | Add test cases to improve code quality | ⭐⭐ |

### PR Template Suggestions
- **Title**: Clear and concise, e.g., `Add MongoDB connector support`
- **Description**: Explain the background, scope of changes, and test results
- **Checklist**:
  - [ ] Code passes local testing
  - [ ] Documentation has been updated
  - [ ] Follows code standards


## Contact Us

Encountered issues? Want to connect? We provide multiple support channels!

### Instant Communication
<table>
<tr>
<td>
<a href="https://join.slack.com/t/tapdatacommunity/shared_invite/zt-1biraoxpf-NRTsap0YLlAp99PHIVC9eA">
<img src="https://img.shields.io/badge/Slack-Join%20Channel-4A154B?style=flat&logo=slack&logoColor=white" alt="Slack"/>
</a>
</td>
<td>
<a href="https://twitter.com/tapdata_daas">
<img src="https://img.shields.io/badge/Twitter-Follow%20Updates-1DA1F2?style=flat&logo=twitter&logoColor=white" alt="Twitter"/>
</a>
</td>
<td>
<a href="https://github.com/tapdata/tapdata-connectors/issues">
<img src="https://img.shields.io/badge/GitHub-Submit%20Issue-181717?style=flat&logo=github&logoColor=white" alt="GitHub Issues"/>
</a>
</td>
</tr>
</table>

### Technical Community
<p align="left">
<a href="https://20778419.s21i.faiusr.com/4/2/ABUIABAEGAAg-JPfhwYonMrzlwEwZDhk.png" rel="nofollow">
<img src="https://20778419.s21i.faiusr.com/4/2/ABUIABAEGAAg-JPfhwYonMrzlwEwZDhk.png" width="200" alt="WeChat Group"/>
</a>
</p>

## Reference Documentation

- [TapData Community Edition Open Source Repository](https://github.com/tapdata/tapdata)
- [TapData Online Documentation](https://docs.tapdata.io/)

## 🎉 Acknowledgments

Thank you to all developers, users, and supporters who have contributed to TapData Connectors. It's with your support that this project continues to grow and thrive.

If this project has been helpful to you, please give us a Star!

**Contributors**

<a href="https://github.com/tapdata/tapdata-connectors/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=tapdata/tapdata-connectors" alt="Contributors"/>
</a>