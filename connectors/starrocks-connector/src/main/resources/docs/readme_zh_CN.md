## **连接配置帮助**
### **1. StarRocks 安装说明**
请遵循以下说明以确保在 TapData 中成功添加和使用 StarRocks 数据库。

### **2. 支持版本**
StarRocks 3+

### **3. 先决条件 (作为源)**
#### **3.1 创建 StarRocks 账号**
```
// 创建用户
create user 'username'@'localhost' identified with mysql_native_password by 'password';
```

#### **3.2 给账号授权**
对于需要同步的数据库赋于select权限
```
GRANT SELECT, SHOW VIEW, CREATE ROUTINE, LOCK TABLES ON <DATABASE_NAME>.<TABLE_NAME> TO 'username' IDENTIFIED BY 'password';
```
全局权限授予
```
GRANT RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'username' IDENTIFIED BY 'password';
```

###  **4. 先决条件（作为目标）**
对于某个数据库赋于全部权限
```
GRANT ALL PRIVILEGES ON <DATABASE_NAME>.<TABLE_NAME> TO 'username' IDENTIFIED BY 'password';
```
对于全局的权限
```
GRANT PROCESS ON *.* TO 'tapdata' IDENTIFIED BY 'password';
```
###  **5. 建表注意事项**
```
由于 StarRocks 作为目标写入由Stream Load方式进行，以下是建表时的一些注意事项:
```
#### **5.1 Primary**
```
源表有主键, 且数据存在 插入/更新/删除 时建议采用的表模型, 同一个主键的数据将只存在一份, 针对查询进行了优化
```

#### **5.2 Unique**
```
与 Primary 表类似, 针对写入进行了优化, 如果场景中写入远远高于查询, 可以使用此表模型
```

#### **5.3 Aggregate**
```
Aggregate 以聚合模型建表, 支持将数据更新为非 NULL 值, 且不支持删除
```

#### **5.3 Duplicate**
```
仅支持插入, 不支持更新和删除, 适合用在全量任务, 且目标配置为清空目标表数据
```
