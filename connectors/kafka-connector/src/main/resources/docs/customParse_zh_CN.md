## 自定义解析Kafka消息体使用说明
### 输入参数说明
record为每条消息的body 要将每条消息转换为Tapdata 的DML或DDL事件，需要遵守以下协议。
1. 若要转为添加字段DDL事件,脚本的返回值需要遵守以下协议:
```
{type:209, newFields=[{name:'fieldName',dataType:'fieldDataType'}]}
```
newFields是一个数组。代表所有新增的字段。dateType的值可以为Map、Array、Number、Boolean、String、Integer、Text。
2. 若要转为改名DDL,脚本的返回值需要遵守以下协议:
```
{type=202, nameChange={after:'afterName',before:'beforeName'}}
```
nameChange代表修改前后的字段名，这个值一定不能为空，否则会转换不成功，报错。
3. 若要转为删除字段DDL，脚本返回值的对象需要有以下协议:
```
{type:207，fieldName:'deleteFieldName'}
```
fieldName代表要删除的字段名，这个值一定不能为空，否则会转换不成功，报错。
4. 若要转为修改字段属性DDL，脚本的返回值需要遵守以下协议:
```
{
	type:201,
	fieldName:'fieldName'
	// dataType代表字段类型的变化
	dataTypeChange:{
		before:'String',
		after:'Integer'
	},
	// checkChange代表字段check约束的变化
	checkChange:{
		before:'',
		after:''
	},
	// constraintChange代表字段constraint约束的变化
	constraintChange:{
		before:'',
		after:''
	},
	// nullableChange代表字段nullable约束的变化
	nullableChange:{
		before:false,
		after:true
	},
	// commentChange代表字段comment约束的变化
	commentChange:{
		before:'',
		after:''
	},
	// defaultChange代表字段default约束的变化
	defaultChange:{
		before:beforeDefaultValue,
		after:afterDefaultValue,
	},
	// primaryChange代表修改字段primary约束的变化
	primaryChange:{
		before:0
		after:1
	}
}
```
其中fieldName代表需要修改属性的列名,fieldName不能为空，否则会报错。<br/>
dataTypeChange、checkChange、constraintChange、nullableChange、commentChange、defaultChange、primaryChange这其中有一个一定不能为空，否则会报错。
4. 若要转为dml事件，脚本的返回值需要有以下协议:
```
{
	type:'insert或者'update或者'delete',
	after:{} //insert和update不能为空,
	before:{} //delete事件不能为null
}
```
type的值可以为insert、update、delete，其中当type为insert和update时，after不能为空，当type为delete时，before不能为空,否则会报错。
5. 若要丢弃此消息，则return null
### 示例
1. 如下示例将op值为1001、1002、1003转为DML事件，op值为2001、2002、2003转为DDL事件,并且需要将不同DDL需要的不同信息从body中取出放到返回值中。
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
    data.putAll(record.tapType);
    return data;
   default:
    return null;
}
```

