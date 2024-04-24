## Custom parsing Kafka message bodies
### Input parameter description
To convert each message into a DML or DDL event of Tapdata, the following protocol needs to be observed.
* To transition to the add field DDL event, the return value of the script needs to conform to the following protocol:
```
{type:209, newFields=[{name:'fieldName',dataType:'fieldDataType'}]}
```
newFields is an array. Represents all new fields. dateType values can be Map, Array, Number, Boolean, String, Integer, and Text.
* To convert to renamed DDL, the return value of the script needs to conform to the following protocol:
```
{type=202, nameChange={after:'afterName',before:'beforeName'}}
```
nameChange is the name of the field before and after the change, and must not be empty; otherwise, the conversion will fail and result in an error.
* To convert to a delete field DDL, the object returned by the script needs to have the following protocol:
```
{type:207，fieldName:'deleteFieldName'}
```
fieldName is the name of the field to remove; this value must not be empty; otherwise, the conversion will be unsuccessful and an error will be thrown.
* To convert to DDL, the return value of the script needs to conform to the following protocol:
```
{
	type:201,
	fieldName:'fieldName'
	// dataType represents the change in field type
	dataTypeChange:{
		before:'String',
		after:'Integer'
	},
	// checkChange represents the change of the field check constraint
	checkChange:{
		before:'',
		after:''
	},
	// constraintChange represents the change to the field constraint
	constraintChange:{
		before:'',
		after:''
	},
	// nullableChange represents changes to the nullable constraint of the field
	nullableChange:{
		before:false,
		after:true
	},
	// commentChange represents the change of the field comment constraint
	commentChange:{
		before:'',
		after:''
	},
	// defaultChange represents the change to the default constraint of the field
	defaultChange:{
		before:beforeDefaultValue,
		after:afterDefaultValue
	},
	// primaryChange represents the change that modifies the field primary constraint
	primaryChange:{
		before:0, //0
		after:1
	}
}
```
Where fieldName represents the name of the column that needs to change the property.fieldName cannot be empty; otherwise, an error will be thrown. <br />
dataTypeChange, checkChange, constraintChange, nullableChange, commentChange, defaultChange, primaryChange One of these must not be null, or you will get an error.
* To be converted to a dml event, the return value of the script needs to have the following protocol:
```
{
	type:'insert或者'update或者'delete',
	after:{} //insert和update不能为空,
	before:{} //delete事件不能为null
}
```
The values of type can be Insert, update, or DELEte.When type is insert or update, after cannot be null, and when type is delete, before cannot be null, otherwise an error will be thrown.
* To discard this message, return null
### Examples
* The following example converts the op values of 1001, 1002, and 1003 to DML events and the op values of 2001, 2002, and 2003 to DDL events, and needs to extract different information needed by different DDLS from the body and put it into the return value.
```
let data = new LinkedHashMap();
switch (record.op) {
   case 1001:
   case 1002:
   case 1003:
    data.put("op", record.optype);
    data.put("before", record.before);
    data.put("after", record.after);
    return data;
   case 2001:
   case 2002:
   case 2003:
    data.put("op", "ddl");
    data.putAll(record.tapType);	// The tapType in kafka's message body contains information needed for the various types of DDLS in tapdata
    return data;
   default:
    return null;
}
```

