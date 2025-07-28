### 写在前面
如果您感兴趣的话，不妨前往飞书多维表格提供的API文档文档，详细了解全部内容：

- API文档：[https://open.feishu.cn/document/server-docs/docs/bitable-v1/bitable-overview](https://open.feishu.cn/document/server-docs/docs/bitable-v1/bitable-overview)

当然您也可以浏览以下内容，快速上手飞书多维表格数据源的配置流程。

---

### 属性说明

App ID：在飞书自建一个应用，建立完成后得到应用的App ID信息。获取方式参见：https://open.feishu.cn/document/home/app-types-introduction/overview

App Secret：在飞书自建一个应用，建立完成后得到应用的App Secret信息。获取方式参见：https://open.feishu.cn/document/home/app-types-introduction/overview

App Token：一篇多维表格可以理解成是一个应用（app），标记该应用的唯一标识叫 app_token。获取方式参见：https://open.feishu.cn/document/server-docs/docs/bitable-v1/notification

Table ID：每篇多维表格是由多个数据表（table）组成的，标记该数据表的唯一标识叫 table_id。获取方式参见：https://open.feishu.cn/document/server-docs/docs/bitable-v1/notification

---

### 使用提示

#### 作为目标

- 飞书多维表支持多种DML类型事件，包括：新增、修改、删除

- 飞书多维表同时支持三种插入策略，两种更新策略：

  - 插入策略：目标存在就更新（需要关闭 `索引增加唯一标识` 开关才能生效）、目标存在就忽略、仅插入
  - 更新策略：不存在就插入、不存在就丢弃

- 支持批量写入，但是飞书服务端各接口存在以下限流措施，请知悉：

  - 批量插入、修改接口：接口访问频次50次/秒，每次请求最大提交记录数1000条
  - 批量删除接口：接口访问频次50次/秒，每次请求最大提交记录数500条
  - 批量查询接口：接口访问频次：20次/秒，针对单主键字段表查询每次能够获取50条记录，针对多主键每次仅能够查询获取10条记录

---