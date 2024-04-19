# tapdata-connectors

Tapdata连接器

## 如何注册数据源

### 使用 **pdk** 命令

```shell
    java -jar pdk.jar register -a ${access_code} -t ${tm_url} [-ak ${accessKey} [-sk ${secretKey}]] [-r ${oem_type}] [-f ${filter_type}] [-h] [-X] [-l]
```

参数列表：
  - -l  (--latest): 是否需要覆盖最新版本
  - -a (--auth): 提供access_code
  - -ak (--accessKey): 提供用户 accessKey 用于注册数据源到云版环境
  - -sk (--secretKey): 提供用户 secretKey 用于注册数据源到云版环境
  - -t (--tm): Tapdata TM URL, 这是您需要注册数据源的环境
  - -r (--replace): 如果需要向OEM环境注册数据源，请填写OEM类型参数
  - -f (--filter): 不应跳过数据源类型的列表。如果值为空，则所有连接器都将注册所有连接器。如果包含多个，请用逗号分隔
  - -T (--threadCount): 多线程注册参数，启用线程数，最大值20，默认值1
  - -h (--help): 命令帮助
  
A. Tip: 
    
您必须在文件系统中保存pdk文件， 如果pdk文件路径为：/build/pdk
如果要注册的连接器， 文件路径为 /connectors/dist/demo-connector.jar

B. 完整的命令如下:

 (1) 当您在java运行环境（linux/windows/macOS）中执行命令时

```shell
java -jar /build/pdk register -a ${access_code} -ak ${access-key} -sk ${secret-key} -r ${oem-type} -f ${need-register-connector-type} -t http://${tm-server}  /connectors/dist/demo-connector.jar
```

(2) 当您在简单环境（linux/macOS）中执行命令时

```shell
/build/pdk register -a ${access_code} -ak ${access-key} -sk ${secret-key} -r ${oem-type} -f ${need-register-connector-type} -t http://${tm-server}  /connectors/dist/demo-connector.jar
```