config.setStreamReadIntervalSeconds(10);
/**
 * @return The returned result cannot be empty and must conform to one of the following forms:
 *      (1)Form one:  A string (representing only one table name)
 *          return 'example_table_name';
 *      (2)Form two: A String Array (It means that multiple tables are returned without description, only table name is provided)
 *          return ['example_table_1', 'example_table_2', ... , 'example_table_n'];
 *      (3)Form three: A Object Array ( The complex description of the table can be used to customize the properties of the table )
 *          return [
 *           {
 *               "name: '${example_table_1}',
 *               "fields": {
 *                   "${example_field_name_1}":{
 *                       "type":"${field_type}",
 *                       "default:"${default_value}"
 *                   },
 *                   "${example_field_name_2}":{
 *                       "type":"${field_type}",
 *                       "default:"${default_value}"
 *                   }
 *               }
 *           },
 *           '${example_table_2}',
 *           ['${example_table_3}', '${example_table_4}', ...]
 *          ];
 * @param connectionConfig  Configuration property information of the connection page
 * */
function discoverSchema(connectionConfig) {
    //return ['Leads','Contacts','Accounts','Potentials','Quotes'];
    return [{
        "name":connectionConfig.table_id,
        "fields": {}
    }]
}

/**
 *
 * @param connectionConfig  Configuration property information of the connection page
 * @param nodeConfig  Configuration attribute information of node page
 * @param offset Breakpoint information, which can save paging condition information
 * @param tableName  The name of the table to obtain the full amount of phase data
 * @param pageSize Processing number of each batch of data in the full stage
 * @param batchReadSender  Sender of submitted data
 * */
function batchRead(connectionConfig, nodeConfig, offset, tableName, pageSize, batchReadSender) {
}


/**
 *
 * @param connectionConfig  Configuration property information of the connection page
 * @param nodeConfig
 * @param offset
 * @param tableNameList
 * @param pageSize
 * @param streamReadSender
 * */
function streamRead(connectionConfig, nodeConfig, offset, tableNameList, pageSize, streamReadSender) {
}


/**
 * @return The returned result is not empty and must be in the following form:
 *          [
 *              {"test": String, "code": Number, "result": String},
 *              {"test": String, "code": Number, "result": String},
 *              ...
 *          ]
 *          param - test :  The type is a String, representing the description text of the test item.
 *          param - code :  The type is a Number, is the type of test result. It can only be [-1, 0, 1], -1 means failure, 1 means success, 0 means warning.
 *          param - result : The type is a String, descriptive text indicating test results.
 * @param connectionConfig  Configuration property information of the connection page
 * */
function connectionTest(connectionConfig) {
    return [{
        "test": "Example test item",
        "code": 1,
        "result": "Pass"
    }];
}


/**
 *
 * @param connectionConfig
 * @param nodeConfig
 * @param commandInfo
 * */

function commandCallback(connectionConfig, nodeConfig, commandInfo) {

}

function insertRecord(connectionConfig, nodeConfig, eventDataMap, settings){
    let tableSettings = {};
    if(settings){
    tableSettings = JSON.parse(settings);
    }
    if(tableSettings && tableSettings.keys && tableSettings.keys.length > 0 &&
    eventDataMap && eventDataMap.afterData && eventDataMap.afterData[tableSettings.keys[0]]){
      let response = invoker.invoke('findRow',{"filter":"CurrentValue.["+tableSettings.keys[0]+"]=\""+eventDataMap.afterData[tableSettings.keys[0]]+"\""});
      if(response && response.result && response.result.code === 0 && response.result.data && response.result.data.items && response.result.data.items.length >0){
        let record_id = response.result.data.items[0].record_id;
        let body = tapUtil.fromJson(eventDataMap.afterData);
        let responseA = invoker.invoke('editRow',{"record_id":record_id,"body":body});
        if(responseA.result && responseA.result.code === 0){
            return true;
        }
        if(responseA.result){
            log.warn("update recode fail:{}",responseA.result.msg);
        }
        return false;
      }
    }
    let body = tapUtil.fromJson(eventDataMap.afterData);
    let response = invoker.invoke('addRow',{"body":body});
    if(response.result && response.result.code === 0){
      return true;
    }
    if(response.result){
       log.warn("update recode fail:{}",response.result.msg);
    }
    return false;
}

function updateRecord(connectionConfig, nodeConfig, eventDataMap, settings){
    let tableSettings = {};
    if(settings){
    tableSettings = JSON.parse(settings);
    }
    if(tableSettings && tableSettings.keys && tableSettings.keys.length > 0 &&
    eventDataMap && eventDataMap.afterData && eventDataMap.afterData[tableSettings.keys[0]]){
      let response = invoker.invoke('findRow',{"filter":"CurrentValue.["+tableSettings.keys[0]+"]=\""+eventDataMap.afterData[tableSettings.keys[0]]+"\""});
      if(response && response.result && response.result.code === 0 && response.result.data && response.result.data.items && response.result.data.items.length >0){
        let record_id = response.result.data.items[0].record_id;
        let body = tapUtil.fromJson(eventDataMap.afterData);
        let responseA = invoker.invoke('editRow',{"record_id":record_id,"body":body});
        if(responseA.result && responseA.result.code === 0){
            return true;
        }
        if(responseA.result){
            log.warn("update recode fail:{}",responseA.result.msg);
        }
        return false;
      }
    }
    let body = tapUtil.fromJson(eventDataMap.afterData);
    let response = invoker.invoke('addRow',{"body":body});
    if(response.result && response.result.code === 0){
      return true;
    }
    if(response.result){
        log.warn("update recode fail:{}",response.result.msg);
    }
    return false;
}

function deleteRecord(connectionConfig, nodeConfig, eventDataMap, settings){
    let tableSettings = {};
    if(settings){
    tableSettings = JSON.parse(settings);
    }
    if(tableSettings && tableSettings.keys && tableSettings.keys.length > 0 &&
    eventDataMap && eventDataMap.beforeData && eventDataMap.beforeData[tableSettings.keys[0]]){
      let response = invoker.invoke('findRow',{"filter":"CurrentValue.["+tableSettings.keys[0]+"]=\""+eventDataMap.beforeData[tableSettings.keys[0]]+"\""});
      if(response && response.result && response.result.code === 0 && response.result.data && response.result.data.items && response.result.data.items.length >0){
        let record_id = response.result.data.items[0].record_id;
        let responseA = invoker.invoke('deleteRow',{"record_id":record_id});
        if(responseA.result && responseA.result.code === 0){
            return true;
        }
        if(responseA.result){
            log.warn("update recode fail:{}",responseA.result.msg);
        }
        return false;
      }
    }
    return false;
}

/**
 * This method is used to update the access key
 *  @param connectionConfig :type is Object
 *  @param nodeConfig :type is Object
 *  @param apiResponse :The following valid data can be obtained from apiResponse : {
 *              result :  type is Object, Return result of interface call
 *              httpCode : type is Integer, Return http code of interface call
 *          }
 *
 *  @return must be {} or null or {"key":"value",...}
 *      - {} :  Type is Object, but not any key-value, indicates that the access key does not need to be updated, and each interface call directly uses the call result
 *      - null : Semantics are the same as {}
 *      - {"key":"value",...} : Type is Object and has key-value ,  At this point, these values will be used to call the interface again after the results are returned.
 * */
function updateToken(connectionConfig, nodeConfig, apiResponse) {
log.warn("http code:{}",apiResponse.httpCode);
log.warn("result code:{}",apiResponse.result.code);
    if (apiResponse.httpCode === 403 || apiResponse.httpCode === 400 || (apiResponse.result && apiResponse.result.code === 99991663 )) {
        try{
            let refreshToken = invoker.invokeWithoutIntercept("getToken");
            if(refreshToken && refreshToken.result &&refreshToken.result.app_access_token){
                log.warn('token:' + refreshToken.result.app_access_token);
                return {"Authorization": 'Bearer ' + refreshToken.result.app_access_token};
            }
        }catch (e) {
            log.warn(e)
        }
    }
    return null;
}

function handleData(data) {
    for(let x in data){
        if(data[x] !== undefined && data[x] !== null) {
            let sss = data[x].toString()
            if (Array.isArray(data[x])) {
                data[x] = handleData(data[x])
            } else if (sss === "[object Object]") {
                data[x] = handleData(data[x])
            }
        }
        if(x && x.startsWith('$')){
            let key = x.replace('$','_');
            data[key] = data[x];
            delete data[x];
        }
    }
    return data
}