## **连接配置帮助**
### **1. VASTBASE安装说明**
请遵循以下说明以确保在 Tapdata 中成功添加和使用VASTBASE数据库。
### **2. 支持版本**
VASTBASE-G100 版本
### **3. CDC原理和支持**
#### **3.1 CDC原理**
VASTBASE 的逻辑解码功能与Postgres相同，它是一种机制，允许提取提交到事务日志中的更改，并通过输出插件以用户友好的方式处理这些更改。

#### **3.2 CDC支持**
- **逻辑解码**（Logical Decoding）：用于从 WAL 日志中解析逻辑变更事件
- **复制协议**（Replication Protocol）：提供了消费者实时订阅（甚至同步订阅）数据库变更的机制
- **快照导出**（export snapshot）：允许导出数据库的一致性快照（pg_export_snapshot）
- **复制槽**（Replication Slot）：用于保存消费者偏移量，跟踪订阅者进度。
所以，根据以上，我们需要安装逻辑解码器，现有提供的解码器如下拉框中所示

### **4. 先决条件**
#### **4.1 修改REPLICA IDENTITY**
该属性决定了当数据发生`UPDATE,DELETE`时，日志记录的字段
- **DEFAULT** - 更新和删除将包含primary key列的现前值
- **NOTHING** - 更新和删除将不包含任何先前值
- **FULL** - 更新和删除将包含所有列的先前值
- **INDEX index name** - 更新和删除事件将包含名为index name的索引定义中包含的列的先前值
如果有多表合并同步的场景，则Tapdata需要调整该属性为FULL
示例
```
alter table '[schema]'.'[table name]' REPLICA IDENTITY FULL`
```

#### **4.2 插件安装**
（目前VASTBASE自带wal2json插件）

#### **4.3 权限**
##### **4.3.1 作为源**
- **初始化**<br>
```
GRANT SELECT ON ALL TABLES IN SCHEMA <schemaname> TO <username>;
```
- **增量**<br>
用户需要有replication login权限，如果不需要日志增量功能，则可以不设置replication权限
```
CREATE ROLE <rolename> REPLICATION LOGIN;
CREATE USER <username> ROLE <rolename> PASSWORD '<password>';
// or
CREATE USER <username> WITH REPLICATION LOGIN PASSWORD '<password>';
```
配置文件 pg_hba.conf 需要添加如下内容：<br>
```
pg_hba.conf
local   replication     <youruser>                     trust
host    replication     <youruser>  0.0.0.0/32         md5
host    replication     <youruser>  ::1/128            trust
```

##### **4.3.2 作为目标**
```
GRANT INSERT,UPDATE,DELETE,TRUNCATE
ON ALL TABLES IN SCHEMA <schemaname> TO <username>;
```
> **注意**：以上只是基本权限的设置，实际场景可能更加复杂

##### **4.4  测试日志插件**
> **注意**：以下操作建议在POC环境进行
>连接vastbase数据库，切换至需要同步的数据库，创建一张测试表
```
-- 假设需要同步的数据库为vastbase，模型为public
\c vastbase

create table public.test_decode
(
  uid    integer not null
      constraint users_pk
          primary key,
  name   varchar(50),
  age    integer,
  score  decimal
)
```
可以根据自己情况创建一张测试表<br>
- 创建 slot 连接，以 wal2json 插件为例
```
select * from pg_create_logical_replication_slot('slot_test', 'wal2json')
```
- 创建成功后，对测试表插入一条数据<br>
- 监听日志，查看返回结果，是否有刚才插入操作的信息<br>
```
select * from pg_logical_slot_peek_changes('slot_test', null, null)
```
- 成功后，销毁slot连接，删除测试表<br>
```
select * from pg_drop_replication_slot('slot_test')
drop table public.test_decode
```
#### **4.5 异常处理**
- **Slot清理**<br>
如果 tapdata 由于不可控异常（断电、进程崩溃等），导致cdc中断，会导致 slot 连接无法正确从 pg 主节点删除，将一直占用一个 slot 连接名额，需手动登录主节点，进行删除
查询slot信息
```
// 查看是否有slot_name以tapdata_cdc_开头的信息
 TABLE pg_replication_slots;
```
- **删除slot节点**<br>
```
select * from pg_drop_replication_slot('tapdata');
```
- **删除操作**<br>
在使用 wal2json 插件解码时，如果源表没有主键，则无法实现增量同步的删除操作

#### **4.6 使用最后更新时间戳的方式进行增量同步**
##### **4.6.1 名词解释**
**schema**：中文为模型，pgsql一共有3级目录，库->模型->表，以下命令中<schema>字符，需要填入表所在的模型名称
##### **4.6.2 预先准备（该步骤只需要操作一次）**
- **创建公共函数**
在数据库中，执行以下命令
```
CREATE OR REPLACE FUNCTION <schema>.update_lastmodified_column()
    RETURNS TRIGGER language plpgsql AS $$
    BEGIN
        NEW.last_update = now();
        RETURN NEW;
    END;
$$;
```
- **创建字段和trigger**
> **注意**：以下操作，每张表需要执行一次
假设需要增加last update的表名为mytable
- **创建last_update字段**
```
alter table <schema>.mytable add column last_udpate timestamp default now();
```
- **创建trigger**
```
create trigger trg_uptime before update on <schema>.mytable for each row execute procedure
    update_lastmodified_column();
```
### **5. 全类型字段支持**
- smallint
- integer
- bigint
- numeric
- real
- double precision
- character
- varchar
- text
- bytea
- bit
- bit varying
- boolean
- date
- interval
- timestamp
- timestamp with time zone
- point
- line
- lseg
- box
- path
- polygon
- circle
- cidr
- inet
- macaddr
- uuid
- xml
- json
- tsvector (增量不支持不报错)
- tsquery (增量不支持不报错)
- oid
- regproc (增量不支持不报错)
- regprocedure (增量不支持不报错)
- regoper (增量不支持不报错)
- regoperator (增量不支持不报错)
- regclass (增量不支持不报错)
- regtype (增量不支持不报错)
- regconfig (增量不支持不报错)
- regdictionary (增量不支持不报错)
