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
 * 批量插入记录到飞书表格
 * @param connectionConfig 连接配置
 * @param nodeConfig 节点配置
 * @param eventDataList 事件数据列表，每个元素包含 afterData 字段
 * @param settings 表格设置，包含主键信息等
 * @returns {Object} 返回批量操作结果
 */
function batchInsertRecords(connectionConfig, nodeConfig, eventDataList, settings) {
    if (!eventDataList || !Array.isArray(eventDataList) || eventDataList.length === 0) {
        log.warn("batchInsertRecords: eventDataList is empty or invalid");
        return {
            success: false,
            message: "事件数据列表为空或无效",
            results: []
        };
    }

    let tableSettings = {};
    if (settings) {
        try {
            tableSettings = JSON.parse(settings);
        } catch (e) {
            log.warn("batchInsertRecords: failed to parse settings: {}", e.message);
        }
    }

    // 准备批量插入的记录数据
    let records = [];
    let processResults = [];

    for (let i = 0; i < eventDataList.length; i++) {
        let eventDataMap = eventDataList[i];
        let recordResult = {
            index: i,
            success: false,
            action: 'insert',
            message: ''
        };

        try {
            if (!eventDataMap || !eventDataMap.afterData) {
                recordResult.message = "事件数据或afterData为空";
                processResults.push(recordResult);
                continue;
            }

            // 如果配置了主键，先检查记录是否已存在
            if (tableSettings && tableSettings.keys && tableSettings.keys.length > 0 &&
                eventDataMap.afterData[tableSettings.keys[0]]) {

                let keyValue = eventDataMap.afterData[tableSettings.keys[0]];
                let response = invoker.invoke('findRow', {
                    "filter": "CurrentValue.[" + tableSettings.keys[0] + "]=\"" + keyValue + "\""
                });

                if (response && response.result && response.result.code === 0 &&
                    response.result.data && response.result.data.items &&
                    response.result.data.items.length > 0) {

                    // 记录已存在，执行更新操作
                    let record_id = response.result.data.items[0].record_id;
                    let body = tapUtil.fromJson(eventDataMap.afterData);
                    let updateResponse = invoker.invoke('editRow', {
                        "record_id": record_id,
                        "body": body
                    });

                    if (updateResponse.result && updateResponse.result.code === 0) {
                        recordResult.success = true;
                        recordResult.action = 'update';
                        recordResult.message = "记录更新成功";
                    } else {
                        recordResult.message = updateResponse.result ? updateResponse.result.msg : "更新失败";
                    }
                    processResults.push(recordResult);
                    continue;
                }
            }

            // 准备插入的记录数据
            let body = tapUtil.fromJson(eventDataMap.afterData);
            records.push({
                "fields": body
            });
            recordResult.success = true;
            recordResult.message = "准备批量插入";
            processResults.push(recordResult);

        } catch (e) {
            recordResult.message = "处理记录时发生错误: " + e.message;
            processResults.push(recordResult);
            log.warn("batchInsertRecords: error processing record at index {}: {}", i, e.message);
        }
    }

    // 如果没有需要插入的记录，直接返回结果
    if (records.length === 0) {
        return {
            success: true,
            message: "没有需要插入的新记录",
            results: processResults,
            insertedCount: 0,
            updatedCount: processResults.filter(r => r.action === 'update' && r.success).length
        };
    }

    // 执行批量插入
    try {
        let batchResponse = invoker.invoke('batchAddRows', {
            "records": records
        });

        if (batchResponse.result && batchResponse.result.code === 0) {
            // 更新插入成功的记录状态
            let insertIndex = 0;
            for (let i = 0; i < processResults.length; i++) {
                if (processResults[i].action === 'insert' && processResults[i].success) {
                    processResults[i].message = "批量插入成功";
                    insertIndex++;
                }
            }

            return {
                success: true,
                message: "批量插入操作完成",
                results: processResults,
                insertedCount: records.length,
                updatedCount: processResults.filter(r => r.action === 'update' && r.success).length
            };
        } else {
            // 批量插入失败，更新所有准备插入的记录状态
            for (let i = 0; i < processResults.length; i++) {
                if (processResults[i].action === 'insert') {
                    processResults[i].success = false;
                    processResults[i].message = batchResponse.result ? batchResponse.result.msg : "批量插入失败";
                }
            }

            return {
                success: false,
                message: "批量插入失败: " + (batchResponse.result ? batchResponse.result.msg : "未知错误"),
                results: processResults,
                insertedCount: 0,
                updatedCount: processResults.filter(r => r.action === 'update' && r.success).length
            };
        }
    } catch (e) {
        log.warn("batchInsertRecords: batch insert failed: {}", e.message);

        // 更新所有准备插入的记录状态
        for (let i = 0; i < processResults.length; i++) {
            if (processResults[i].action === 'insert') {
                processResults[i].success = false;
                processResults[i].message = "批量插入异常: " + e.message;
            }
        }

        return {
            success: false,
            message: "批量插入异常: " + e.message,
            results: processResults,
            insertedCount: 0,
            updatedCount: processResults.filter(r => r.action === 'update' && r.success).length
        };
    }
}

/**
 * 简化版批量插入记录（仅插入，不检查重复）
 * @param connectionConfig 连接配置
 * @param nodeConfig 节点配置
 * @param recordsData 记录数据数组，每个元素是要插入的字段数据
 * @returns {Object} 返回批量操作结果
 */
function simpleBatchInsertRecords(connectionConfig, nodeConfig, recordsData) {
    if (!recordsData || !Array.isArray(recordsData) || recordsData.length === 0) {
        log.warn("simpleBatchInsertRecords: recordsData is empty or invalid");
        return {
            success: false,
            message: "记录数据为空或无效",
            insertedCount: 0
        };
    }

    try {
        // 准备批量插入的记录数据
        let records = recordsData.map(function(data) {
            return {
                "fields": handleData(data)
            };
        });

        // 执行批量插入
        let batchResponse = invoker.invoke('batchAddRows', {
            "records": records
        });

        if (batchResponse.result && batchResponse.result.code === 0) {
            return {
                success: true,
                message: "批量插入成功",
                insertedCount: records.length,
                data: batchResponse.result.data
            };
        } else {
            return {
                success: false,
                message: "批量插入失败: " + (batchResponse.result ? batchResponse.result.msg : "未知错误"),
                insertedCount: 0
            };
        }
    } catch (e) {
        log.warn("simpleBatchInsertRecords: batch insert failed: {}", e.message);
        return {
            success: false,
            message: "批量插入异常: " + e.message,
            insertedCount: 0
        };
    }
}

/**
 * 批量插入使用示例：
 *
 * // 示例1：使用完整的批量插入方法（包含重复检查和更新）
 * let eventDataList = [
 *     { afterData: { "姓名": "张三", "年龄": 25, "部门": "技术部" } },
 *     { afterData: { "姓名": "李四", "年龄": 30, "部门": "产品部" } },
 *     { afterData: { "姓名": "王五", "年龄": 28, "部门": "设计部" } }
 * ];
 * let settings = JSON.stringify({ keys: ["姓名"] }); // 以姓名作为主键
 * let result = batchInsertRecords(connectionConfig, nodeConfig, eventDataList, settings);
 *
 * // 示例2：使用简化版批量插入方法（仅插入）
 * let recordsData = [
 *     { "姓名": "张三", "年龄": 25, "部门": "技术部" },
 *     { "姓名": "李四", "年龄": 30, "部门": "产品部" },
 *     { "姓名": "王五", "年龄": 28, "部门": "设计部" }
 * ];
 * let simpleResult = simpleBatchInsertRecords(connectionConfig, nodeConfig, recordsData);
 *
 * // 返回结果格式：
 * // {
 * //     success: true/false,
 * //     message: "操作结果描述",
 * //     insertedCount: 插入的记录数量,
 * //     updatedCount: 更新的记录数量（仅完整版方法有此字段）,
 * //     results: 详细结果数组（仅完整版方法有此字段）
 * // }
 */

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