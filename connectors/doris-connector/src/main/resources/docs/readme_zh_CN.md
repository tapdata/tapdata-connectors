## **连接配置帮助**
### **1. Doris 安装说明**
请遵循以下说明以确保在 Tapdata 中成功添加和使用Doris数据库。
### **2. 支持版本**
Doris 1.x、Doris 2.x
### **3. 先决条件**
#### **3.1 创建Doris账号**
```
// 创建用户
create user 'username'@'localhost' identified with mysql_native_password by 'password';
// 修改密码
alter user 'username'@'localhost' identified with mysql_native_password by 'password';
```
#### **3.2 给 tapdata 账号授权**
对于某个数据库赋于select权限
```
GRANT SELECT, SHOW VIEW, CREATE ROUTINE, LOCK TABLES ON <DATABASE_NAME>.<TABLE_NAME> TO 'tapdata' IDENTIFIED BY 'password';
```
对于全局的权限
```
GRANT RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'tapdata' IDENTIFIED BY 'password';
```
#### **3.3 约束说明**
```
当从Doris同步到其他异构数据库时，如果源Doris存在表级联设置，因该级联触发产生的数据更新和删除不会传递到目标。如需要在目标端构建级联处理能力，可以视目标情况，通过触发器等手段来实现该类型的数据同步。
```
###  **4. 先决条件（作为目标）**
对于某个数据库赋于全部权限
```
GRANT ALL PRIVILEGES ON <DATABASE_NAME>.<TABLE_NAME> TO 'tapdata' IDENTIFIED BY 'password';
```
对于全局的权限
```
GRANT PROCESS ON *.* TO 'tapdata' IDENTIFIED BY 'password';
```
###  **5. 建表注意事项**
```
由于Doris作为目标写入均是由Stream Load方式进行，以下是建表时的一些注意事项
```
#### **5.1 Duplicate**
```
Duplicate方式建表表示可重复的模式，在非追加写入模式下，默认以更新条件字段为排序键，追加写入模式无更新条件字段时，手动配置

该模式下更新事件将会新增一条记录，不会覆盖原有记录，删除事件则会删除所有满足条件的相同记录，因此推荐在追加写入模式下使用
```
#### **5.2 Aggregate**
```
Aggregate方式建表表示聚合模式，默认以更新条件字段为聚合键，非聚合键使用Replace If Not Null

该模式下大量的更新事件性能更优，缺陷是无法将Set Null的事件生效，另外删除事件是不受支持的。因此推荐无物理删除场景且更新事件无Set Null场景和源端数据字段不全情况使用
```
#### **5.3 Unique**
```
Unique方式建表表示唯一键模式，默认以更新条件字段为唯一键，使用方式与绝大多数关系数据源一致

如果大量的更新事件，且更新的字段不停变化地情况下，推荐源端使用全字段补齐的方式，以保证可观的性能
```
###  **6. 常见错误**
Unknown error 1044
如果权限已经grant了，但是通过tapdata还是无法通过测试连接，可以通过下面的步骤检查并修复
```
SELECT host,user,Grant_priv,Super_priv FROM Doris.user where user='username';
//查看Grant_priv字段的值是否为Y
//如果不是，则执行以下命令
UPDATE Doris.user SET Grant_priv='Y' WHERE user='username';
FLUSH PRIVILEGES;
```
