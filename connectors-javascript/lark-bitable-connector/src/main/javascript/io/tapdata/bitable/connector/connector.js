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
    return [connectionConfig.table_id]
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
    let app;
    try {
        app = invoker.invoke("getTableMateInfo").result;
        let isApp = 1;
        if (app.code !== 0) {
            return [{
                "test": "Get Multidimensional table metadata",
                "code": -1,
                "result": "Can not get Multidimensional table metadata, please check you App ID or App Secret or table ID"
                    + (app.msg !== undefined && app.msg !== null && app.msg !== "" ? ", error message: " + app.msg : "")
            }];
        }
        if (app.data === undefined || app.data === null) {
            isApp = -1;
        }
        return [{
            "test": "Get Multidimensional table metadata",
            "code": isApp,
            "result": isApp === 1 ? "Table name is: " + app.data.app.name : "Can not get Multidimensional table metadata info, please check you App ID and App Secret and table ID"
        }];
    } catch (e) {
        return [{
            "test": " Input parameter check ",
            "code": -1,
            "result": "Can not get Multidimensional table metadata, please check you App ID and App Secret and table ID"
        }];
    }
}


/**
 *
 * @param connectionConfig
 * @param nodeConfig
 * @param commandInfo
 * */
function commandCallback(connectionConfig, nodeConfig, commandInfo) {

}

/**
 * PDK js-function: insert-batch
 * */
function updateRecordBatch(connectionConfig, nodeConfig, eventDataMaps, settings, capabilities) {
    let dmlUpdatePolicy = capabilities["dml_update_policy"];
    log.debug("updateRecordBatch: {}", dmlUpdatePolicy);
    //@todo
    dmlUpdatePolicy = "insert_on_nonexists";
    return insertOrUpdateRecordBatch(connectionConfig, nodeConfig, eventDataMaps, settings, dmlUpdatePolicy);
}

/**
 * PDK js-function: update-batch
 * */
function insertRecordBatch(connectionConfig, nodeConfig, eventDataMaps, settings, capabilities) {
    let dmlInsertPolicy = capabilities["dml_insert_policy"];
    log.debug("insertRecordBatch: {}", dmlInsertPolicy);
    //@todo
    dmlInsertPolicy = "update_on_exists";
    return insertOrUpdateRecordBatch(connectionConfig, nodeConfig, eventDataMaps, settings, dmlInsertPolicy);
}

/**
 * PDK js-function: delete-batch
 * */
function deleteRecordBatch(connectionConfig, nodeConfig, eventDataMaps, settings, capabilities) {
    let queryApiDelay = getDelayTime(nodeConfig, "queryApiDelay");
    let writeApiDelay = getDelayTime(nodeConfig, "writeApiDelay");
    let tableSettings = {};
    if (settings) {
        tableSettings = JSON.parse(settings);
    }

    // 检查必要参数
    if (!eventDataMaps || !tableSettings || !tableSettings.keys || tableSettings.keys.length === 0) {
        return false;
    }

    // 如果是单条记录，转换为数组格式
    if (!Array.isArray(eventDataMaps)) {
        eventDataMaps = [eventDataMaps];
    }

    // 提取主键值
    let keyValues = [];
    // 提取有效数据
    let validData = [];
    eventDataMaps.forEach((eventData) => {
        if (!isAlive()) {
            return;
        }
        if (eventData.beforeData && eventData.beforeData[tableSettings.keys[0]]) {
            keyValues.push(eventData.beforeData[tableSettings.keys[0]]);
        }
        validData.push(eventData.beforeData);
    });

    if (keyValues.length === 0) {
        return false;
    }

    let deleteCount = 0;
    let recordIdsToDelete = [];

    //单Key可以构建较为简单的查询条件，支持筛选查询条件集合长度限制50，多Key查询条件复杂收API字段校验限制只能查询10条
    const BATCH_SIZE = tableSettings.keys.length === 1 ? 50 : 10;
    for (let i = 0; i < validData.length; i += BATCH_SIZE) {
        if (!isAlive()) {
            break;
        }
        let batchData = validData.slice(i, i + BATCH_SIZE);
        findRecord(batchData, tableSettings, queryApiDelay, (singleRecords) => {
            //log.info("singleRecords: {}", JSON.stringify(singleRecords));
            if (singleRecords.length <= 0) {
                log.info("Not any records need to delete");
                return;
            }
            singleRecords.forEach(item => {
                if (!isAlive()) {
                    return;
                }
                recordIdsToDelete.push(item["record_id"]);
                if (recordIdsToDelete.length >= 1000) {
                    let deleteResponse = invoker.invoke('batchDeleteRecords', {
                        "records": recordIdsToDelete,
                        "_tap_sleep_time_": writeApiDelay
                    });
                    if (!deleteResponse || !deleteResponse.result || deleteResponse.result.code !== 0) {
                        log.warn("Batch delete failed: code={}, msg={}, batch count: {}, {}",
                            deleteResponse?.result?.code, deleteResponse?.result?.msg, recordIdsToDelete.length, JSON.stringify(recordIdsToDelete));
                    } else {
                        deleteCount += recordIdsToDelete.length;
                    }
                    //log.info("B Delete : {}", JSON.stringify(recordIdsToDelete));
                    recordIdsToDelete.length = 0;
                    //log.info("A Delete : {}", JSON.stringify(recordIdsToDelete));
                }
            })
        });
    }


    // 执行批量删除
    if (recordIdsToDelete.length > 0) {
        let deleteResponse = invoker.invoke('batchDeleteRecords', {
            "records": recordIdsToDelete,
            "_tap_sleep_time_": writeApiDelay
        });
        if (!deleteResponse || !deleteResponse.result || deleteResponse.result.code !== 0) {
            log.warn("Batch delete failed: code={}, msg={}, batch count: {}, {}",
                deleteResponse?.result?.code, deleteResponse?.result?.msg, recordIdsToDelete.length, JSON.stringify(recordIdsToDelete));
        } else {
            deleteCount += recordIdsToDelete.length;
        }
    }
    return {
        "insert": 0,
        "update": 0,
        "delete": deleteCount
    };
}

function findRecord(batchData, tableSettings, apiDelay, accept) {
    let filterConditions = {};
    if (tableSettings.keys.length == 1) {
        let key = tableSettings.keys[0];
        filterConditions = {
            "conjunction": "or",
            "conditions": batchData.map(value => {
                return {
                    "field_name": key,
                    "operator": "is",
                    "value": [value[key]]
                }
            })
        };
    } else {
        filterConditions = {
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
    }

    let singleRecords = [];
    try {
        // 修正搜索请求格式
        let searchRequest = {
            "filter": {"value": filterConditions},
            "page_size": 500,
            "fieldNames": tableSettings.keys,
            "pageToken": null,
            "_tap_sleep_time_": apiDelay
        };
        while (isAlive()) {
            let searchResponse = invoker.invoke('searchRecords', searchRequest);
            if (searchResponse
                && searchResponse.result
                && searchResponse.result.code === 0
                && searchResponse.result.data
                && searchResponse.result.data.items) {
                for (let index in searchResponse.result.data.items) {
                    let item = searchResponse.result.data.items[index];
                    for (let batchDataIndex in batchData) {
                        let data = batchData[batchDataIndex];
                        let fields = {};
                        data.forEach((key, value) => {
                            fields[key] = value;
                        });
                        if (recordsMatch(data, item, tableSettings.keys)) {
                            singleRecords.push({
                                "record_id": item["record_id"],
                                "fields": fields,
                                "dataIndex": batchDataIndex
                            })
                        }
                    }
                }
                let hasMore = searchResponse.result.data.has_more;
                if (hasMore) {
                    if (searchResponse.result.data.page_token !== undefined && searchResponse.result.data.page_token !== null) {
                        searchRequest.pageToken = searchResponse.result.data.page_token;
                    } else {
                        break;
                    }
                } else {
                    break;
                }
                if (singleRecords.length >= 500) {
                    accept(singleRecords);
                    singleRecords.length = 0;
                }
            } else if (searchResponse && searchResponse.result) {
                log.warn("Search failed: code={}, msg={}",
                    searchResponse.result.code, searchResponse.result.msg);
                break;
            } else {
                log.warn("Search failed: {}", JSON.stringify(searchResponse));
                break;
            }
        }
    } catch (e) {
        log.error("Search error: {}", e.message);
    }
    accept(singleRecords);
}

function getDelayTime(nodeConfig, key) {
    let apiDelay = nodeConfig[key];
    if (apiDelay === undefined || apiDelay === null || apiDelay > 10000 || apiDelay < 0) {
        apiDelay = 0;
    }
    return apiDelay;
}

function insertOrUpdateRecordBatch(connectionConfig, nodeConfig, eventDataMaps, settings, policy) {
    let queryApiDelay = getDelayTime(nodeConfig, "queryApiDelay");
    let writeApiDelay = getDelayTime(nodeConfig, "writeApiDelay");

    let tableSettings = {};
    if (settings) {
        tableSettings = JSON.parse(settings);
    }

    // 检查必要参数
    if (!eventDataMaps || !tableSettings || !tableSettings.keys || tableSettings.keys.length === 0) {
        log.warn("Missing required parameters");
        return false;
    }

    // 确保eventDataMaps是数组
    if (!Array.isArray(eventDataMaps)) {
        eventDataMaps = [eventDataMaps];
    }

    // 提取有效数据
    let validData = [];
    eventDataMaps.forEach((eventData) => {
        validData.push(eventData.afterData);
    });

    if (validData.length === 0) {
        log.warn("No valid data to process");
        return false;
    }
    // 分批处理
    //单Key可以构建较为简单的查询条件，支持筛选查询条件集合长度限制50，多Key查询条件复杂收API字段校验限制只能查询10条
    const BATCH_SIZE = tableSettings.keys.length === 1 ? 50 : 10;
    let insertCount = 0;
    let updateCount = 0;
    if (policy !== "just_insert") {
        log.info("Will Check data records through primary key({}) to obtain record_id", JSON.stringify(tableSettings.keys));
    } else {
        log.info("Will just insert data all records");
    }
    for (let i = 0; i < validData.length; i += BATCH_SIZE) {
        if (!isAlive()) {
            break;
        }
        let batchData = validData.slice(i, i + BATCH_SIZE);
        // 分离更新和插入的记录
        let recordsToUpdate = [];
        let recordsToInsert = [];
        if (policy !== "just_insert") {
            findRecord(batchData, tableSettings, queryApiDelay, (singleRecords) => {
                if (singleRecords.length > 0) {
                    let indexOfExists = []
                    singleRecords.forEach(item => {
                        recordsToUpdate.push(item);
                        indexOfExists.push(item.dataIndex);
                        if (recordsToUpdate.length >= 1000) {
                            updateCount += acceptRecords(recordsToUpdate, policy, "batchUpdateRecords", "update", writeApiDelay);
                            log.info("B Update : {}", JSON.stringify(recordsToUpdate));
                            recordsToUpdate.length = 0;
                            log.info("A Update : {}", JSON.stringify(recordsToUpdate));
                        }
                    })
                    for (let batchDataIndex in batchData) {
                        if (!isAlive()) {
                            break;
                        }
                        if (!indexOfExists.includes(batchDataIndex)) {
                            let data = batchData[batchDataIndex];
                            let fields = {};
                            data.forEach((key, value) => {
                                fields[key] = value;
                            });
                            recordsToInsert.push({"fields": fields})
                            if (recordsToInsert.length >= 1000) {
                                insertCount += acceptRecords(recordsToInsert, policy, "batchCreateRecords", "insert", writeApiDelay);
                                log.info("B Insert : {}", JSON.stringify(recordsToInsert));
                                recordsToInsert.length = 0;
                                log.info("Insert : {}", JSON.stringify(recordsToInsert));
                            }
                        }
                    }
                } else {
                    for (let batchDataIndex in batchData) {
                        if (!isAlive()) {
                            break;
                        }
                        let data = batchData[batchDataIndex];
                        let fields = {};
                        data.forEach((key, value) => {
                            fields[key] = value;
                        });
                        recordsToInsert.push({"fields": fields});
                        if (recordsToInsert.length >= 1000) {
                            insertCount += acceptRecords(recordsToInsert, policy, "batchCreateRecords", "insert", writeApiDelay);
                            log.info("B Insert : {}", JSON.stringify(recordsToInsert));
                            recordsToInsert.length = 0;
                            log.info("Insert : {}", JSON.stringify(recordsToInsert));
                        }
                    }
                }
            });
        } else {
            for (let batchDataIndex in batchData) {
                let data = batchData[batchDataIndex];
                let fields = {};
                data.forEach((key, value) => {
                    fields[key] = value;
                });
                recordsToInsert.push({"fields": fields});
                if (recordsToInsert.length >= 1000) {
                    insertCount += acceptRecords(recordsToInsert, policy, "batchCreateRecords", "insert", writeApiDelay);
                    recordsToInsert.length = 0;
                }
            }
        }
        if (recordsToUpdate.length > 0) {
            updateCount += acceptRecords(recordsToUpdate, policy, "batchUpdateRecords", "update", writeApiDelay);
        }
        if (recordsToInsert.length > 0) {
            insertCount += acceptRecords(recordsToInsert, policy, "batchCreateRecords", "insert", writeApiDelay);
        }
    }
    return {
        "insert": insertCount,
        "update": updateCount,
        "delete": 0
    };
}

function acceptRecords(records, policy, apiName, type, apiDelay) {
    //log.info("acceptRecords: {}, {}, {}, {}", JSON.stringify(records), policy, apiName, type);
    let acceptCount = 0;
    switch (policy) {
        case "insert_on_nonexists":
        case "update_on_exists":
            if (type === "update") {
                if (callBatchApi(records, "batchUpdateRecords", "update", apiDelay)) {
                    acceptCount += records.length;
                }
            } else {
                if (callBatchApi(records, "batchCreateRecords", "insert", apiDelay)) {
                    acceptCount += records.length;
                }
            }
            break;
        case "ignore_on_exists":
        case "just_insert":
        case "ignore_on_nonexists":
            if (type === "insert") {
                if (callBatchApi(records, "batchCreateRecords", "insert", apiDelay)) {
                    acceptCount += records.length;
                }
            }
            break;
        default:
        //do nothing
    }
    return acceptCount;
}

function callBatchApi(batchData, apiName, type, apiDelay) {
    // 执行批量插入
    if (batchData.length <= 0) {
        return true;
    }
    let allSuccess = true;
    try {
        let requestParam = {
            "records": batchData,
            "_tap_sleep_time_": apiDelay
        };
        //log.info("{} request: {}", type, JSON.stringify(requestParam));
        if (!isAlive()) {
            return false;
        }
        let insertResponse = invoker.invoke(apiName, requestParam);
        //log.info("{} response: {}", type, JSON.stringify(insertResponse));
        if (!insertResponse || !insertResponse.result || insertResponse.result.code !== 0) {
            log.warn("Batch {} failed: code={}, msg={}, {}",
                type,
                insertResponse?.result?.code,
                insertResponse?.result?.msg,
                JSON.stringify(batchData));
            allSuccess = false;
        }
    } catch (e) {
        log.error("Do batch {} error: {}, {}", type, e.message, JSON.stringify(batchData));
        allSuccess = false;
    }
    return allSuccess;
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
    if (!isAlive()) {
        return null;
    }
    if (apiResponse.httpCode === 403
        || apiResponse.httpCode === 400
        || apiResponse.result && apiResponse.result.code === 99991663) {
        log.info("Http code: {}, result code: {}, result msg: {}", apiResponse.httpCode, apiResponse.result.code, apiResponse.result.msg);
        try {
            let refreshToken = invoker.invokeWithoutIntercept("getToken");
            if (refreshToken && refreshToken.result && refreshToken.result.app_access_token) {
                log.info('Token updated: ' + refreshToken.result.app_access_token);
                return {"Authorization": 'Bearer ' + refreshToken.result.app_access_token};
            }
        } catch (e) {
            log.warn(e)
        }
    }
    return null;
}

function handleData(data) {
    for (let x in data) {
        if (data[x] !== undefined && data[x] !== null) {
            let sss = data[x].toString()
            if (Array.isArray(data[x])) {
                data[x] = handleData(data[x])
            } else if (sss === "[object Object]") {
                data[x] = handleData(data[x])
            }
        }
        if (x && x.startsWith('$')) {
            let key = x.replace('$', '_');
            data[key] = data[x];
            delete data[x];
        }
    }
    return data
}