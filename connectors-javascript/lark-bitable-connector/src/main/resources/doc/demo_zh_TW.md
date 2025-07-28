### 寫在前面
如果您感興趣的話，不妨前往飛書多元表格提供的API文檔文檔，詳細了解全部內容：

- OpenAPI文檔：[https://open.feishu.cn/document/server-docs/docs/bitable-v1/bitable-overview](https://open.feishu.cn/document/server-docs/docs/bitable-v1/bitable-overview)

當然您也可以瀏覽以下內容，快速上手飛書多元表格數據源的配置流程。

---

### 1.屬性說明

App ID：在飛書自建一個應用，建立完成後得到應用的App ID資訊。 獲取管道參見：https://open.feishu.cn/document/home/app-types-introduction/overview

App Secret：在飛書自建一個應用，建立完成後得到應用的App Secret資訊。 獲取管道參見：https://open.feishu.cn/document/home/app-types-introduction/overview

App Token：一篇多元表格可以理解成是一個應用（app），標記該應用的唯一標識叫app_ token。 獲取管道參見：https://open.feishu.cn/document/server-docs/docs/bitable-v1/notification

Table ID：每篇多元表格是由多個資料表（table）組成的，標記該資料表的唯一標識叫table_ id。 獲取管道參見：https://open.feishu.cn/document/server-docs/docs/bitable-v1/notification

---


### 使用提示

#### 作為目標

- 飛書多元錶支持多種DML類型事件，包括：新增、修改、删除

- 飛書多元錶同時支持三種插入策略，兩種更新策略：

    - 插入策略：目標存在就更新（需要關閉 `索引增加唯一標識` 開關才能生效）、目標存在就忽略、僅插入
    - 更新策略：不存在就插入、不存在就丟棄

- 支持批量寫入，但是飛書服務端各介面存在以下限流措施，請知悉：

    - 批量插入、修改介面：介面訪問頻次50次/秒，每次請求最大提交記錄數1000條
    - 批量删除介面：介面訪問頻次50次/秒，每次請求最大提交記錄數500條
    - 批量査詢介面：介面訪問頻次：20次/秒，針對單主鍵欄位錶査詢每次能够獲取50條記錄，針對多主鍵每次僅能够査詢獲取10條記錄

---