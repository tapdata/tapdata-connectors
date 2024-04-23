## 自定義解析Kafka消息體使用說明
### 輸入參數說明
record爲每條消息的body 要將每條消息轉換爲Tapdata 的DML或DDL事件，需要遵守以下協議。
1. 若要轉爲添加字段DDL事件,腳本的返回值需要遵守以下協議:
```
{type:209, newFields=[{name:'fieldName',dataType:'fieldDataType'}]}
```
newFields是一個數組。代表所有新增的字段。dateType的值可以爲Map、Array、Number、Boolean、String、Integer、Text。
2. 若要轉爲改名DDL,腳本的返回值需要遵守以下協議:
```
{type=202, nameChange={after:'afterName',before:'beforeName'}}
```
nameChange代表修改前後的字段名，這個值一定不能爲空，否則會轉換不成功，報錯。
3. 若要轉爲刪除字段DDL，腳本返回值的對象需要有以下協議:
```
{type:207，fieldName:'deleteFieldName'}
```
fieldName代表要刪除的字段名，這個值一定不能爲空，否則會轉換不成功，報錯。
4. 若要轉爲修改字段屬性DDL，腳本的返回值需要遵守以下協議:
```
{
	type:201,
	fieldName:'fieldName'
	// dataType代表字段類型的變化
	dataTypeChange:{
		before:'String',
		after:'Integer'
	},
	// checkChange代表字段check約束的變化
	checkChange:{
		before:'',
		after:''
	},
	// constraintChange代表字段constraint約束的變化
	constraintChange:{
		before:'',
		after:''
	},
	// nullableChange代表字段nullable約束的變化
	nullableChange:{
		before:false,
		after:true
	},
	// commentChange代表字段comment約束的變化
	commentChange:{
		before:'',
		after:''
	},
	// defaultChange代表字段default約束的變化
	defaultChange:{
		before:beforeDefaultValue,
		after:afterDefaultValue,
	},
	// primaryChange代表修改字段primary約束的變化
	primaryChange:{
		before:0
		after:1
	}
}
```
其中fieldName代表需要修改屬性的列名,fieldName不能爲空，否則會報錯。<br />
dataTypeChange、checkChange、constraintChange、nullableChange、commentChange、defaultChange、primaryChange這其中有一個一定不能爲空，否則會報錯。
4. 若要轉爲dml事件，腳本的返回值需要有以下協議:
```
{
	type:'insert或者'update或者'delete',
	after:{} //insert和update不能爲空,
	before:{} //delete事件不能爲null
}
```
type的值可以爲insert、update、delete，其中當type爲insert和update時，after不能爲空，當type爲delete時，before不能爲空,否則會報錯。
5. 若要丟棄此消息，則return null
### 示例
1. 如下示例將op值爲1001、1002、1003轉爲DML事件，op值爲2001、2002、2003轉爲DDL事件,並且需要將不同DDL需要的不同信息從body中取出放到返回值中。
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

