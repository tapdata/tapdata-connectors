# 任务启动卡住问题分析

## 问题描述
一些任务在启动时会卡在启动中状态，无法进入运行中状态。

## 已添加的日志

### 1. 管理端(Manager)日志
- **TaskServiceImpl.java**: 
  - 任务启动请求接收日志
  - 状态机START事件执行日志
  - 任务停止请求接收日志
  - 状态机STOP事件执行日志

- **TaskScheduleServiceImpl.java**:
  - WebSocket启动消息发送日志

- **DataSyncHandler.java**:
  - 接收引擎状态变化消息日志

- **StateMachineExecutor.java**:
  - 状态机转换详细日志

### 2. 引擎端(iEngine)日志
- **DataSyncEventHandler.java**:
  - 接收WebSocket启动/停止命令日志
  - 任务查找和验证日志

- **TapdataTaskScheduler.java**:
  - 任务调度器启动任务日志
  - 任务状态更新日志
  - Hazelcast任务启动日志
  - 任务停止处理日志

## 可能导致任务卡在启动中的原因分析

### 1. WebSocket通信问题
**现象**: 管理端发送启动消息，但引擎端未收到
**可能原因**:
- 网络连接不稳定
- WebSocket连接断开但未及时重连
- 消息队列满或处理延迟

**排查方法**:
- 检查管理端日志: "Manager sending START task websocket message to engine"
- 检查引擎端日志: "Engine received START task websocket message"
- 如果管理端有发送日志但引擎端无接收日志，说明WebSocket通信有问题

### 2. 任务查找失败
**现象**: 引擎端收到启动命令但找不到任务
**可能原因**:
- 任务数据不一致
- 数据库连接问题
- 任务已被删除但状态未同步

**排查方法**:
- 检查引擎端日志: "Engine failed to find task for START command"
- 检查数据库中任务状态是否正确

### 3. 状态机转换失败
**现象**: 状态机无法从当前状态转换到目标状态
**可能原因**:
- 任务当前状态不允许启动操作
- 并发操作导致状态冲突
- 状态机配置错误

**排查方法**:
- 检查管理端日志: "Manager state machine START failed"
- 检查状态机转换日志中的状态信息

### 4. 引擎任务启动异常
**现象**: 引擎端开始启动任务但在过程中失败
**可能原因**:
- Hazelcast集群问题
- 资源不足(内存、CPU)
- 任务配置错误
- 连接器初始化失败

**排查方法**:
- 检查引擎端日志: "Engine scheduler starting Hazelcast task"
- 检查是否有"Engine scheduler successfully started task"日志
- 查看异常堆栈信息

### 5. 状态更新失败
**现象**: 任务实际已启动但状态未更新为running
**可能原因**:
- 数据库更新失败
- 状态同步机制异常
- 心跳机制问题

**排查方法**:
- 检查引擎端日志: "Engine scheduler updating task status to running"
- 检查管理端是否收到状态变化消息: "Manager received task status change websocket message"

### 6. 任务锁竞争
**现象**: 多个操作同时竞争任务锁
**可能原因**:
- 并发启动操作
- 任务调度器重复调度
- 锁超时设置不合理

**排查方法**:
- 检查日志中的锁相关信息
- 查看是否有"failed because of task lock"消息

## 排查步骤建议

1. **检查WebSocket通信**:
   - 确认管理端发送消息: "Manager sending START task websocket message"
   - 确认引擎端接收消息: "Engine received START task websocket message"

2. **检查状态机转换**:
   - 查看状态机转换日志: "State machine transition starting"
   - 确认转换成功: "State machine transition completed successfully"

3. **检查任务调度**:
   - 确认引擎开始调度: "Engine scheduler starting task"
   - 确认Hazelcast启动: "Engine scheduler starting Hazelcast task"
   - 确认启动成功: "Engine scheduler successfully started task"

4. **检查状态同步**:
   - 确认状态更新: "Engine scheduler updating task status to running"
   - 确认管理端接收: "Manager received task status change websocket message"

## 监控建议

1. **设置告警**: 当任务在启动中状态超过5分钟时触发告警
2. **定期检查**: 定期扫描卡在启动中状态的任务
3. **日志聚合**: 将相关日志聚合到统一平台便于分析
4. **性能监控**: 监控WebSocket连接状态、数据库性能、引擎资源使用情况

## 临时解决方案

1. **重启任务**: 停止卡住的任务后重新启动
2. **重启引擎**: 如果多个任务都卡住，考虑重启引擎
3. **检查资源**: 确保引擎有足够的内存和CPU资源
4. **清理缓存**: 清理可能的缓存数据不一致问题
