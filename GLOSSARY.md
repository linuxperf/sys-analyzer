# sys-analyzer

sys-analyzer 的控制面管理 Agent 注册，并提供 Agent 状态查询。

## Language

**Agent 注册表**

Controller 按 Agent ID 维护的注册记录集合。每条记录关联一个注册会话，并包含该 Agent 的元数据和最近一次心跳时间（若有）。
