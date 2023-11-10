## **Connection configuration help**

### **1. Hudi Installation instructions**

Follow these instructions to ensure that the Hudi database is successfully added and used in Tapdata.

### **2. Restriction Description**

Tapdata The current version of the Hudi system supports only the target.

### **3. Supported version**

Hudi0.11.0

### **4. Configuration description**

#### Example data source configuration

*   The address of the cluster
    *   ip\:port
*   database
    *   test\_tapdata
*   Kerberos authentication
    *   The file that the key represents
        *   upload user.keytab file
    *   Profiles
        *   upload krb5.conf file
    *   Hive principal configuration
        *   spark2x/hadoop.<hadoop.com@HADOOP.COM> (The value of the principal)
*   account
    *   test\_tapdata
*   password
*   Connection parameters
    *   ;sasl.qop=auth-conf;auth=KERBEROS

### **5. Connection test items**

- Check host/IP and port
- Check database
- Check account and password
- Check the write permissions