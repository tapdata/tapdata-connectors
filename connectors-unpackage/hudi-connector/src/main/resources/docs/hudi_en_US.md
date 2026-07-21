## **Connection configuration help**

### **1. Hudi Installation instructions**

Follow these instructions to ensure that the Hudi database is successfully added and used in Tapdata.

### **2. Restriction Description**

1. Tapdata The current version of the Hudi system supports only the target

2. Environmental configuration requirements

    - The machine of the computing engine should have Hadoop environment variables, and the Hadoop version should be consistent with the Hadoop installed on your server

      ```
      Execute the following command to check if your machine meets this condition:
        In the Windows environment, 
            hold down win+R, 
            enter cmd or powershell, 
            and then enter 'hadoop version' in the command window to check if this condition is met
        In a Linux or Mac environment, 
            open the terminal 
            and enter 'hadoop version' in the command window to check if this condition is met
      ```

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
*   The configuration file for server-side Hadoop is core-site.xml, usually located in the etc/Hadoop directory of your server-side Hadoop installation directory
*   The configuration file for server-side hdfs is hdfs site.xml, usually located in the etc/Hadoop directory of your server-side Hadoop installation directory
*   The configuration file for server-side Hive is hive-site.xml, usually located in the configuration file directory of your server-side Hive installation directory
*   Connection parameters
    *   ;sasl.qop=auth-conf;auth=KERBEROS

### **5. Connection test items**

- Check host/IP and port
- Check database
- Check account and password
- Check the write permissions