# sys-analyzer

sys-analyzer 的控制面管理 Agent 注册，并提供 Agent 状态查询。

## Language

**Agent 注册表**

Controller 按 Agent ID 维护的注册记录集合。记录包含该 Agent 的元数据和最近一次心跳时间（若有）。在线记录关联一个有效的注册会话。租约到期后，记录可在保留期内继续查询；管理操作也可撤销会话并保留记录。
