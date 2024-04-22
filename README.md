# tapdata-connectors
Connectors For Tapdata

## Register Connector Help

### Use **pdk** command

```shell
    java -jar pdk.jar register -a ${access_code} -t ${tm_url} [-ak ${accessKey} [-sk ${secretKey}]] [-r ${oem_type}] [-f ${filter_type}] [-h] [-X] [-l]
```

Arguments：
  - -l  (--latest): Whether replace the latest version
  - -a (--auth): Provide auth token to register
  - -ak (--accessKey): Provide auth accessKey when register connector to cloud 
  - -sk (--secretKey): Provide auth secretKey when register connector to cloud 
  - -t (--tm): Tapdata TM url, where you want register connector to
  - -r (--replace): Replace Config file name, value is oem type
  - -f (--filter): The list of the Authentication types should not be skipped. If value is empty, all connectors will register all connectors. if it contains multiple, please separate them with commas
  - -h (--help): TapData cli help
  
A. Tip: 
    
You must save pdk file in file system, if pdk file path is: /build/pdk
if the connector which will be register, path is /connectors/dist/demo-connector.jar

B. Full command such as:

 (1) when you execute command in a java run environment(linux/windows/macOS)

```shell
java -jar /build/pdk register -a ${access_code} -ak ${access-key} -sk ${secret-key} -r ${oem-type} -f ${need-register-connector-type} -t http://${tm-server}  /connectors/dist/demo-connector.jar
```

(2) when you execute command in simple environment（linux/macOS）

```shell
/build/pdk register -a ${access_code} -ak ${access-key} -sk ${secret-key} -r ${oem-type} -f ${need-register-connector-type} -t http://${tm-server}  /connectors/dist/demo-connector.jar
```