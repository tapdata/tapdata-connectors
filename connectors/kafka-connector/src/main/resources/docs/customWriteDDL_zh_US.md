## Custom DDL message body instructions

### Input parameter description

record represents the DDL record coming from the source and has the following structure:

```
var record={
	'referenceTime':1713724313,	// Events are generated in milliseconds and are of type Long
	'time':1713724313,	// Events are generated in milliseconds and are of type Long
	'type':203,		// represents the DDL event type, 209 represents the add field event, 201 represents the change field attribute event, 202 represents the change field name event, 207 represents the delete field event
	'ddl':'alter table add column test varchar(1)',		// represents the original DDL statement of the database
	'tableId':"test",	// represents the name of the table where the DDL occurred
	// New fields DDL event-specific properties
	'newFields':[
		{
			'name':'f1',		// represents the new field name
			'autoInc': false,		// Indicates if it is an autoincrement field
			'dataType':'STRING',		// represents the field type
			'nullable': true,		// Indicates whether it can be null or not
			'partitionKey':false,		// Indicates whether it is partitioned or not
			'primaryKey':false		// Primary key field
		}
	]
	// Change field name DDL event-specific property
	nameChange:{
		after:'afterName',
		before:'beforeName'
	}
	// fieldName、checkChange、constraintChange、nullableChange、commentChange、defaultChange、primaryChange、fieldName is specific to the DDL event that modifies the field property
	fieldName:'Change the field name property of the property'
	dataTypeChange:{
		before:'String',
		after:'Integer'
	},
	// Field check constraint changes, make sure Check expression is satisfied before writing
	checkChange:{
		before:'beforeCheckexpression',
		after:'afterCheckexpression'
	},
	// Field constraint constraint changes, constraint stands for the field constraint
	constraintChange:{
		before:'beforeConstraint',
		after:'afterConstraint'
	},
	// Field nullable constraints change. nullable represents whether a field can be null or not
	nullableChange:{
		before:false,
		after:true
	},
	// Field comment constrains the change, and comment represents the comment of the field
	commentChange:{
		before:'beforeComment',
		after:'afterComment'
	},
	// Field default value changes, default stands for the default value of the field
	defaultChange:{
		before:beforeDefaultValue,
		after:afterDefaultValue,
	},
	// Field primary constraint changes, greater than 0 means primary key, 0 means not primary key
	primaryChange:{
		before:0
		after:1
	}
	// Remove field DDL event-specific attributes
	fieldName:'Removed field name'
}
```
### Return Value Description
1. The return value represents the body structure written to Kafka
2. Returning null means filtering the data
### Examples
1. As in the above example, the field rename DDL event is customized to write into the body of Kafka message. According to the type of the input record, it determines what kind of DDL event is, and then adds opType, and writes the table owner, table name, and the original statement of DDL in the body
```
	// Create a LinkedHashMap object as the body container object
	let data = new LinkedHashMap();

	switch (record.type) {
	   case 209:
		data.put("op", 2001); // Operation type encoding (number)
		data.put("opType", "addField"); // Operation type (string)
		break;
	   case 201:
	   case 202:
		data.put("op", 2002); // Operation type encoding (number)
		data.put("opType", "changeField"); // Operation type (string)
		break;
	   case 207:
		data.put("op", 2003); // Operation type encoding (number)
		data.put("opType", "deleteField"); // Operation type (string)
		break;
	   default:
		return null; // unsupported DDL returns null
	}

  	data.put("time", record.referenceTime); // DDL occurrence timestamp (seconds)
  	data.put("owner", "taptest"); // Table owner
  	data.put("name", record.tableId); // Table name
  	data.put("sql", record.ddl); // DDL original statement
  	data.put("tapType", record); // Tapdata DDL type
  	return data;
```
