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
function insertRecordBatch(connectionConfig, nodeConfig, eventDataMaps, settings) {
    return insertOrUpdateRecordBatch(connectionConfig, nodeConfig, eventDataMaps, settings);
}

function insertOrUpdateRecordBatch(connectionConfig, nodeConfig, eventDataMaps, settings) {
    let tableSettings = {};
    if(settings){
        tableSettings = JSON.parse(settings);
    }

    // 检查必要参数
    if(!eventDataMaps || !tableSettings || !tableSettings.keys || tableSettings.keys.length === 0) {
        log.warn("Missing required parameters");
        return false;
    }

    // 确保eventDataMaps是数组
    if(!Array.isArray(eventDataMaps)) {
        eventDataMaps = [eventDataMaps];
    }

    // 提取有效数据
    let validData = [];
    eventDataMaps.forEach((eventData) => {
        validData.push(eventData.afterData);
    });

    if(validData.length === 0) {
        log.warn("No valid data to process");
        return false;
    }

    // 分批处理
    const BATCH_SIZE = 500; // 减小批次大小
    let allSuccess = true;

    let insertCount = 0;
    let updateCount = 0;
    for(let i = 0; i < validData.length; i += BATCH_SIZE) {
        let batchData = validData.slice(i, i + BATCH_SIZE);
        let filterConditions = {
            "conjunction": "or",
            "children": batchData.map(value => {
                let children = [];
                tableSettings.keys.forEach(key => {
                    children.push({
                        "field_name": key,
                        "operator": "is",
                        "value": [value[key]]
                    })
                });
                return {
                    "conjunction": "and",
                    "conditions": children
                }
            })
        };
        // 分离更新和插入的记录
        let recordsToUpdate = [];
        let recordsToInsert = [];
        let singleRecords = [];
        try {
            // 修正搜索请求格式
            let searchRequest = {
                "filter": {"value": filterConditions},
                "page_size": 500,
                "fieldNames": tableSettings.keys
            };
            //log.info("Search request: {}", JSON.stringify(searchRequest));
            let searchResponse = invoker.invoke('searchRecords', searchRequest);
            //log.info("Search response: {}", JSON.stringify(searchResponse));

            if(searchResponse && searchResponse.result && searchResponse.result.code === 0) {
                if(searchResponse.result.data && searchResponse.result.data.items) {
                    for(let index in searchResponse.result.data.items) {
                        let item = searchResponse.result.data.items[index];
                        for (let batchDataIndex in batchData) {
                            let data = batchData[batchDataIndex];
                            let fields = {};
                            data.forEach((key, value) => {
                                fields[key] = value;
                            });
                            //log.info("each existing record: {}", JSON.stringify(item))
                            if (recordsMatch(data, item, tableSettings.keys)) {
                                singleRecords.push({
                                    "record_id": item["record_id"],
                                    "fields": fields,
                                    "dataIndex": batchDataIndex
                                })
                                //log.info("add each existing record to update: {}, {}", JSON.stringify(item), JSON.stringify(data))
                            }
                        }
                    }
                }
            } else if(searchResponse && searchResponse.result) {
                log.warn("Search failed: code={}, msg={}",
                    searchResponse.result.code, searchResponse.result.msg);
            }
        } catch(e) {
            log.error("Search error: {}", e.message);
        }

        //log.info("singleRecords: {}", JSON.stringify(singleRecords));
        if (singleRecords.length > 0) {
            let indexOfExists = []
            singleRecords.forEach(item => {
                recordsToUpdate.push(item);
                indexOfExists.push(item.dataIndex);
            })
            for (let batchDataIndex in batchData) {
                if (!indexOfExists.includes(batchDataIndex)) {
                    let data = batchData[batchDataIndex];
                    let fields = {};
                    data.forEach((key, value) => {
                        fields[key] = value;
                    });
                    recordsToInsert.push({"fields": fields})
                }
            }
        } else {
            for (let batchDataIndex in batchData) {
                let data = batchData[batchDataIndex];
                let fields = {};
                data.forEach((key, value) => {
                    fields[key] = value;
                });
                recordsToInsert.push({"fields": fields})
            }
        }


        // 执行批量更新
        if(recordsToUpdate.length > 0) {
            try {
                let updateRequest = {
                    "records": recordsToUpdate
                };

                //log.info("Update request: {}", JSON.stringify(updateRequest));

                let updateResponse = invoker.invoke('batchUpdateRecords', updateRequest);

                //log.info("Update response: {}", JSON.stringify(updateResponse));

                if(!updateResponse || !updateResponse.result || updateResponse.result.code !== 0) {
                    log.warn("Batch update failed: code={}, msg={} {}",
                        updateResponse?.result?.code, updateResponse?.result?.msg, JSON.stringify(recordsToUpdate));
                    allSuccess = false;
                }
            } catch(e) {
                log.error("Update error: {} {}", e.message, JSON.stringify(recordsToUpdate));
                allSuccess = false;
            }
        }

        // 执行批量插入
        if(recordsToInsert.length > 0) {
            try {
                let insertRequest = {
                    "records": recordsToInsert
                };

                //log.info("Insert request: {}", JSON.stringify(insertRequest));

                let insertResponse = invoker.invoke('batchCreateRecords', insertRequest);

                //log.info("Insert response: {}", JSON.stringify(insertResponse));

                if(!insertResponse || !insertResponse.result || insertResponse.result.code !== 0) {
                    log.warn("Batch insert failed: code={}, msg={}, {}",
                        insertResponse?.result?.code, insertResponse?.result?.msg, JSON.stringify(recordsToInsert));
                    allSuccess = false;
                }
            } catch(e) {
                log.error("Insert error: {}, {}", e.message, JSON.stringify(recordsToInsert));
                allSuccess = false;
            }
        }
        insertCount += recordsToInsert.length;
        updateCount += recordsToUpdate.length;
    }

    return {
        "insert": insertCount,
        "update": updateCount,
        "delete": 0
    };
}

function recordsMatch(objMap, keyMaps, keys) {
    let allMatch = true;
    for (let kIndex in keys) {
        let k = keys[kIndex];
        let element = keyMaps["fields"][k];
        for (let eleIndex in element) {
            let ele = element[eleIndex];
            let eleType = ele["type"];
            let value = ele[eleType];
            //log.info("recordMatch: {}, {}, {}", objMap[k], k, value);
            allMatch = allMatch && recordMatch(objMap, k, value);
            if (!allMatch) {
                return false;
            }
        }
    }
    return allMatch;
}

function recordMatch(objMap, key, value) {
    return objMap[key] !== undefined && value !== undefined && objMap[key] === value;
}

function updateRecordBatch(connectionConfig, nodeConfig, eventDataMap, settings) {
    return insertOrUpdateRecordBatch(connectionConfig, nodeConfig, eventDataMaps, settings);
}

function deleteRecordBatch(connectionConfig, nodeConfig, eventDataMaps, settings){
    let tableSettings = {};
    if(settings){
        tableSettings = JSON.parse(settings);
    }

    // 检查必要参数
    if(!eventDataMaps || !tableSettings || !tableSettings.keys || tableSettings.keys.length === 0) {
        return false;
    }

    // 如果是单条记录，转换为数组格式
    if(!Array.isArray(eventDataMaps)) {
        eventDataMaps = [eventDataMaps];
    }

    // 提取主键值
    let keyValues = [];
    eventDataMaps.forEach((eventData) => {
        if (eventData.beforeData && eventData.beforeData[tableSettings.keys[0]]) {
            keyValues.push(eventData.beforeData[tableSettings.keys[0]]);
        }
    });

    if(keyValues.length === 0) {
        return false;
    }

    // 构建查询条件
    let filterCondition = keyValues.map(value =>
        `CurrentValue.[${tableSettings.keys[0]}]="${value}"`
    ).join(" OR ");

    // 查询要删除的记录
    let recordIdsToDelete = [];
    let searchResponse = invoker.invoke('searchRecords', {
        "filter": filterCondition
    });

    if(searchResponse && searchResponse.result && searchResponse.result.code === 0 &&
        searchResponse.result.data && searchResponse.result.data.items) {
        searchResponse.result.data.items.forEach(item => {
            recordIdsToDelete.push(item.record_id);
        });
    }

    // 执行批量删除
    if(recordIdsToDelete.length > 0) {
        let deleteResponse = invoker.invoke('batchDeleteRecords', {
            "records": recordIdsToDelete
        });

        if(!deleteResponse || !deleteResponse.result || deleteResponse.result.code !== 0) {
            log.warn("Batch delete failed: code={}, msg={}",
                deleteResponse?.result?.code, deleteResponse?.result?.msg);
            return false;
        }

        return true;
    }

    return {
        "insert": 0,
        "update": 0,
        "delete": recordIdsToDelete.length
    };
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
    if (apiResponse.httpCode === 403
        || apiResponse.httpCode === 400
        || apiResponse.result && apiResponse.result.code === 99991663) {
        log.warn("Http code:{}, result code:{}, result msg: {}",apiResponse.httpCode, apiResponse.result.code, apiResponse.result.msg);
        try{
            let refreshToken = invoker.invokeWithoutIntercept("getToken");
            if(refreshToken && refreshToken.result &&refreshToken.result.app_access_token){
                log.info('Token updated: ' + refreshToken.result.app_access_token);
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