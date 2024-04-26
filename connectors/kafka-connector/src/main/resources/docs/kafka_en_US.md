# **1. Supported versions**

- Scala: `2.12`
- kafka: `2.0` ~ `2.5`

# **2. Supported data types**

- Object: Corresponds to `TapMap`
- Array: Corresponds to `TapArray`
- Number: Corresponds to `TapNumber`
- Boolean: Corresponds to `TapBoolean`
- Character: Corresponds to `TapString`

# **3. Functional limitations/Notes/Capability boundaries**

- Only supports `JSON Object` string message format (such as `{"id":1, "name": "张三"}`)
- If you select "Ignore non-JSON object format messages" or "Ignore push message exceptions", the `offset` of these
  messages will still be recorded, that is, these messages will not be pushed later, and there is a risk of data loss
- Message push is implemented as `At least once`, and the corresponding consumer end must do idempotent operations
- Message compression type: default `gzip`, large-volume message compression can improve transmission efficiency
- `Kafka` itself does not Distinguish between `DML` and `DDL`:
- Scenario `Oracle` >> `Kafka` >> `MySQL`, supports `DDL` and `DML` synchronization of `Oracle`
- The `DDL` synchronization scenario synchronized from other products to `Kafka` is not supported

# **4. ACK confirmation mechanism**

- No confirmation (`0`): Exit after the message is pushed, and will not check whether kafka has been written correctly
- Write only to the Master partition (`1`): After the message is pushed, it will ensure that the master partition
  receives and writes
- Write to most ISR partitions (`-1`, default option): After the message is pushed, it will ensure that the data is
  received and written by more than half of the partitions
- Write to all ISR partitions (`all`): After the message is pushed, it will ensure that the data is received and written
  by all partitions

# **5. Authentication method**

## **5.1 User And Password**

- If not filled in, it is no authentication mode, and `org.apache.kafka.common.security.plain.PlainLoginModule` is used
  for configuration
- When filling in, `org.apache.kafka.common.security.scram.ScramLoginModule` configuration will be used

## **5.2 Kerberos**

1. Get Kerberos Ticket
    - Can be obtained
      through `kinit -kt <keytab-file> <principal>`, `kadmin.local -q "ktadd -norandkey -k <keytab-file> <principal>"`
        - `keytab-file`: file name
        - `principal`: principal configuration name
        - `-norandkey`: does not change the password
    - Can view the configuration through `klist`, `kadmin.local -q "list_principals"`
2. Configure according to Ticket information
    - Key representation file, such as: `kdc.keytab`
    - Configuration file, such as: `kdc.conf`
    - Principal configuration, such as: `kafka/kkz.com@EXAMPLE.COM`
    - Service name, such as: `kafka`

### **5.2.1 Frequently Asked Questions**

1. The prompt domain is not configured, such as: `kkz.com`
    - Log in to the machine where `FE` is located
    - Modify the `/etc/hosts` configuration:

```shell
cat >> /etc/hosts << EOF
192.168.1.2 kkz.com
EOF
```
