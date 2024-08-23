## **Connection Configuration Help**
### **1.  VASTBASE Installation Instructions**
Please follow the following instructions to ensure successful addition and use of the VASTBASE database in Tapdata.
### **2.  Supported versions**
VASTBASE-G100 version
### **3.  CDC Principles and Support**
#### **3.1 CDC principle**
The logic decoding function of VASTBASE is the same as Postgres, which is a mechanism that allows extracting changes submitted to the transaction log and processing these changes in a user-friendly manner through output plugins.

#### **3.2 CDC support**
- Logical Decoding: Used to parse logical change events from WAL logs
- **Replication Protocol**: Provides consumers with a mechanism for real-time subscription (or even synchronous subscription) of database changes
- **snapshot export** (export snapshot): allows exporting consistent snapshots of the database (pg_dexport_stnapshot)
- **Replication Slot**: Used to store consumer offsets and track subscriber progress.
So, based on the above, we need to install a logic decoder. The existing decoder provided is shown in the drop-down box

### **4.  Prerequisite conditions**
#### **4.1 Modify REPLICA Identity**
This attribute determines the fields recorded in the log when data undergoes' Update, DELETE '
- **DEFILT** - Update and delete current values that will include the primary key column
- **NOTHING** - Updates and deletions will not include any previous values
- **FULL** - Updating and deleting will include the previous values of all columns
- **INDEX index name** - Update and delete events will include the previous values of columns included in the index definition named index name
If there are scenarios where multiple tables are merged and synchronized, Tapdata needs to adjust this property to FULL
Example
```
alter table '[schema]'.' [table name]' REPLICA IDENTITY FULL`
```

#### **4.2 Plugin Installation**
(Currently, VASTBASE comes with the wal2json plugin)

#### **4.3 Permissions**
##### **4.3.1 As a Source**
- **Initialize**<br>
```
GRANT SELECT ON ALL TABLES IN SCHEMA <schemaname> TO <username>;
```
- **Incremental**<br>
The user needs to have replication login permission. If the log increment function is not required, replication permission can be omitted
```
CREATE ROLE <rolename> REPLICATION LOGIN;
CREATE USER <username> ROLE <rolename> PASSWORD '<password>';
// or
CREATE USER <username> WITH REPLICATION LOGIN PASSWORD '<password>';
```
The configuration file pg_ hba. conf needs to add the following content:<br>
```
pg_hba.conf
local   replication     <youruser>                     trust
host    replication     <youruser>  0.0.0.0/32         md5
host    replication     <youruser>  ::1/128            trust
```

##### **4.3.2 as a target**
```
GRANT INSERT,UPDATE,DELETE,TRUNCATE
ON ALL TABLES IN SCHEMA <schemaname> TO <username>;
```
> **Note**: The above are only basic permission settings, and the actual scenario may be more complex

##### **4.4 Test Log Plugin**
> **Attention**: The following operations are recommended to be performed in the POC environment
>Connect to the VastBase database, switch to the database that needs to be synchronized, and create a test table
```
-- Assuming the database to be synchronized is VastBase and the model is Public
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
You can create a test form based on your own situation<br>
- Create a slot connection using the wal2json plugin as an example
```
select * from pg_create_logical_replication_slot('slot_test', 'wal2json')
```
- After successful creation, insert a piece of data into the test table<br>
- Monitor the logs, check the return results, and see if there is any information about the insertion operation just now<br>
```
select * from pg_logical_slot_peek_changes('slot_test', null, null)
```
- After success, destroy the slot connection and delete the test table<br>
```
select * from pg_drop_replication_slot('slot_test')
drop table public.test_decode
```
#### **4.5 Exception Handling**
- Slot cleaning
If tapdata is interrupted by an uncontrollable exception (power outage, process crash, etc.), it will cause the slot connection to not be deleted correctly from the PG master node, and will continue to occupy one slot connection slot. Manual login to the master node is required to delete it
Query slot information
```
//Check if there is any information with slot_name starting with tapdata_cdc_
TABLE pg_replication_slots;
```
- **Delete slot node**<br>
```
select * from pg_drop_replication_slot('tapdata');
```
- **Delete operation**<br>
When decoding with the wal2json plugin, if the source table does not have a primary key, incremental synchronization deletion cannot be achieved

#### **4.6 Incremental synchronization using the last updated timestamp**
##### **4.6.1 Definition of Nouns**
**Schema**: In Chinese, it means model. pgSQL has 3 levels of directories, Library ->Model ->Table. The<schema>character in the following command needs to be filled in with the model name where the table is located
##### **4.6.2 Preparation in advance (this step only requires one operation)**
- **Create a public function**
In the database, execute the following command
```
CREATE OR REPLACE FUNCTION <schema>.update_lastmodified_column()
RETURNS TRIGGER language plpgsql AS $$
BEGIN
NEW.last_update = now();
RETURN NEW;
END;
$$;
```
- **Create fields and triggers**
>**Note**: The following operations need to be performed once for each table
     Assuming that the table name for adding the last update is mytable
- **Create last_uUpdate field**
```
alter table <schema>.mytable add column last_udpate timestamp default now();
```
- **Create trigger**
```
create trigger trg_uptime before update on <schema>.mytable for each row execute procedure
update_lastmodified_column();
```
### **5.  Full type field support**
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
- tsvector (does not support increment and does not report errors)
- tsquery (does not support increment without error)
- oid
- regproc (does not support increment and does not report errors)
- regprocedure (incremental does not support error free)
- regoper (does not support increments and does not report errors)
- regoperator (does not support increment without error)
- regclass (does not support increment without error)
- regtype (does not support increment without error)
- regconfig (does not support increment and does not report errors)
- dictionary (does not support increment and does not report errors)
