/**
 *  This is the toolkit encapsulated by Tap Data.
 * */
var invoker = loadAPI();
var OptionalUtil = {
    isEmpty: function (obj) {
        return typeof (obj) == 'undefined' || null == obj;
    },
    notEmpty: function (obj) {
        return !this.isEmpty(obj);
    }
}

function iterateAllData(apiName, offset, call) {
    if (OptionalUtil.isEmpty(apiName)) {
        log.error("Please specify the corresponding paging API name or URL .");
    }
    let res;
    let error;
    do {
        try{
            let response = invoker.invoke(apiName, offset);
            res = response.result;
            if(response.httpCode >= 400 || response.httpCode < 200){
               log.warn("Get Http data failed, message: {}", JSON.stringify(response));
            }
        }catch (e){
            log.error(e);
            break;
        }
    } while (call(res, offset));
}

function commandAndConvertData(apiName, params, call){
    if (OptionalUtil.isEmpty(apiName)) {
        log.error("Please specify the corresponding paging API name or URL .");
    }
    let invokerData = invoker.invoke(apiName, params);
    return call(invokerData.result);
}
