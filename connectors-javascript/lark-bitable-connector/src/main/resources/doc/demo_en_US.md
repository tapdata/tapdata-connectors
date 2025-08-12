### Written in front
If you are interested, you may want to go to the Lark documents：

- OpenAPI Documentation：[https://open.feishu.cn/document/server-docs/docs/bitable-v1/bitable-overview](https://open.feishu.cn/document/server-docs/docs/bitable-v1/bitable-overview)

Of course, you can also browse the following to quickly get started with the setup process of Lark bitable Data Sources.

---

### Property description

App ID: Build an application on Lark and obtain the App ID information after the establishment is completed.Refer to:https://open.feishu.cn/document/home/app-types-introduction/overview

App Secret: Build an application on Lark and obtain the App Secret information after the establishment is completed.Refer to:https://open.feishu.cn/document/home/app-types-introduction/overview

App Token: Create a new Base in Feishu cloud document, in the url app_token is the framed part of the picture below.Refer to:https://open.feishu.cn/document/server-docs/docs/bitable-v1/notification

Table ID: Create a new Base in Feishu cloud document , in the url table_id is the framed part of the picture below.Refer to:https://open.feishu.cn/document/server-docs/docs/bitable-v1/notification


---


### Hints

#### As Target

- Lark bitble supports multiple DML types of events, including: adding, modifying, and deleting

- Lark bitble supports three insertion strategies and two update strategies simultaneously:

    - Insertion strategy: Update when the target exists (requires turning off the `Index with unique identifier` switch to take effect), ignore when the target exists, insert only
    - Update strategy: Insert if non-existent, discard if non-existent

- Supports batch writing, but there are the following current limiting measures for various interfaces on the Lark server. Please be aware:

    - Batch insertion and modification of interfaces: Interface access frequency of 50 times/second, maximum submission of 1000 records per request
    - Batch deletion of interfaces: Interface access frequency of 50 times/second, maximum number of submitted records per request of 500
    - Batch query interface: Interface access frequency: 20 times/second. For single primary key field table queries, 50 records can be obtained each time. For multiple primary keys, only 10 records can be obtained each time

---