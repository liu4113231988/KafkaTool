# KafkaTool TODO

基于 2026-06-23 对项目代码的静态检查与修复进度整理。  
说明：P0 修复后已执行 `dotnet build .\KafkaTool.sln -nologo --no-restore`，构建通过。以下清单用于继续跟踪剩余问题和功能缺口。

## 1. 已确认的功能性问题

### P0 - 高优先级，可能导致功能错误或在真实集群中不可用

- [x] 修复安全连接配置没有完整传递到所有 Kafka 客户端的问题。  
  现状：
  `KafkaService.GetTopicDetailAsync` 中获取水位偏移时新建的 `ConsumerConfig` 只设置了 `BootstrapServers`，没有带上 SASL/SSL 配置；`ConsumeFromTimestampAsync` 里新建的 `AdminClientConfig` 也没有应用连接配置；`ResetConsumerGroupOffsetAsync` 里内部 consumer 同样没有应用安全配置。  
  影响：
  在开启 SASL/SSL 的集群上，这几个功能会单独失败，出现“测试连接正常，但详情/时间浏览/重置偏移失败”的割裂体验。  
  位置：
  `KafkaToolWpf/KafkaToolWpf/Services/KafkaService.cs` 第 128-131、352-353、455-460 行附近。

- [x] 修复“按时间浏览消息”实际上只会保留最后一次分区分配的问题。  
  现状：
  `ConsumeFromTimestampAsync` 在遍历分区时反复调用 `consumer.Assign(new TopicPartitionOffset(...))`，这会覆盖之前的 assignment。  
  影响：
  多分区 Topic 按时间浏览时，很可能只读到最后一个分区的数据，结果不完整。  
  位置：
  `KafkaToolWpf/KafkaToolWpf/Services/KafkaService.cs` 第 360-374 行附近。

- [x] 重做消费者组 offset 重置实现，避免使用不可靠的“Assign + Commit”方式。  
  现状：
  `ResetConsumerGroupOffsetAsync` 对每个分区 `Assign(new TopicPartitionOffset(...))` 后直接 `Commit()`，实现依赖客户端当前位置行为，缺少明确的 offsets 提交语义。  
  影响：
  可能出现偏移未按预期重置、行为依赖客户端版本、部分分区提交不一致等问题。  
  建议：
  优先改为显式提交目标 offsets，或直接使用更适合的 Admin API。  
  位置：
  `KafkaToolWpf/KafkaToolWpf/Services/KafkaService.cs` 第 445-473 行附近。

### P1 - 中高优先级，结果不准确或体验明显不完整

- [x] 修复集群信息里的 `ClusterId` 取值错误。  
  现状：
  `GetClusterInfoAsync` 把 `metadata.OriginatingBrokerName` 当成 `ClusterId`。  
  影响：
  UI 显示的“Cluster ID”并不是真正的 cluster id，容易误导排障和环境识别。  
  位置：
  `KafkaToolWpf/KafkaToolWpf/Services/KafkaService.cs` 第 51-62 行附近。

- [x] 修复消费者组成员信息是占位数据的问题。  
  现状：
  `GetConsumerGroupsAsync` 里 `AssignedPartitions = new List<string> { $"{g.Group}" }`，并没有真实分区分配信息。  
  影响：
  现在的成员详情不可信，后续如果 UI 展示这些字段会直接误导用户。  
  位置：
  `KafkaToolWpf/KafkaToolWpf/Services/KafkaService.cs` 第 210-216 行附近。

- [x] 修复消费者组详情查询方式过重且结果不完整的问题。  
  现状：
  `GetConsumerGroupDetailsAsync` 会遍历所有 topic/partition，再用 consumer 去读 committed offsets；`ClientId` 和 `Host` 直接写空字符串。  
  影响：
  集群 topic 多时查询很慢；结果缺少活跃消费者关键信息；对非当前 group 使用场景也不够稳妥。  
  位置：
  `KafkaToolWpf/KafkaToolWpf/Services/KafkaService.cs` 第 230-283 行附近。

- [x] 修复连接配置窗口的 SASL 机制绑定问题。  
  现状：
  `ComboBox` 用 `SelectedItem="{Binding EditSaslMechanism}"`，但内部项是 `ComboBoxItem`。  
  影响：
  选中值可能无法正确回写为 `"PLAIN"` / `"SCRAM-SHA-256"` 等纯字符串，导致认证机制配置不稳定。  
  位置：
  `KafkaToolWpf/KafkaToolWpf/Views/ConnectionConfigDialogWindow.xaml` 第 71-77 行附近。

- [x] 修复连接管理弹窗“只能新增，不能真正编辑”的问题。  
  现状：
  选择已有连接后会把值填到右侧表单，但保存逻辑不会回写到选中的连接对象，只能新增或删除。  
  影响：
  用户以为自己在编辑连接，实际不会更新已有配置。  
  位置：
  `KafkaToolWpf/KafkaToolWpf/ViewModels/ConnectionConfigDialogViewModel.cs` 第 18-37、108-127、139-156 行附近。

- [x] 修复连接管理弹窗对原始集合“实时修改，无法取消”的问题。  
  现状：
  弹窗直接操作传入的 `ObservableCollection<ConnectionConfig>`，不是编辑副本。  
  影响：
  即使用户没有点“保存”，新增/删除也已经作用到主界面数据，取消操作没有意义。  
  位置：
  `KafkaToolWpf/KafkaToolWpf/ViewModels/ConnectionConfigDialogViewModel.cs` 第 174-179 行附近；  
  `KafkaToolWpf/KafkaToolWpf/ViewModels/MainWindowViewModel.cs` 第 415-445 行附近。

- [x] 修复 Topic 详情面板“名义上有右侧详情，实际上仍弹 MessageBox”的不一致实现。  
  现状：
  主界面右侧预留了 `TopicDetailPanel`，但 `ViewTopicDetailAsync` 仍然把详情拼成字符串后弹窗显示。  
  影响：
  交互割裂，不利于继续扩展如配置编辑、分区级操作、复制导出等功能。  
  位置：
  `KafkaToolWpf/KafkaToolWpf/Views/MainWindow.xaml` 第 185-197 行附近；  
  `KafkaToolWpf/KafkaToolWpf/ViewModels/MainWindowViewModel.cs` 第 830-862 行附近。

- [x] 修复消息浏览窗口只显示 Header 数量、不显示 Header 内容的问题。  
  现状：
  窗口说明写着会展示 Headers，但 DataGrid 只绑定了 `Headers.Count`。  
  影响：
  用户看不到具体 header 键值，消息排查能力不足。  
  位置：
  `KafkaToolWpf/KafkaToolWpf/Views/BrowseMessageDialogWindow.xaml` 第 37-43 行附近。

- [x] 控制实时消费文本无限累积带来的内存和 UI 性能风险。  
  现状：
  `ConsumerMessage += msg + "\r\n"` 持续拼接大字符串，没有条数或字符上限。  
  影响：
  长时间消费后界面会明显卡顿，内存持续增长。  
  位置：
  `KafkaToolWpf/KafkaToolWpf/ViewModels/MainWindowViewModel.cs` 第 638-645 行附近。

### P2 - 中优先级，设计不完整或结果表达不够好

- [x] 补齐 SSL 证书相关配置的实际使用。  
  现状：
  `ConnectionConfig` 已有 `SslCaLocation`、`SslCertificateLocation`、`SslKeyLocation`，但 UI 没有输入项，`ApplySaslConfig` 也没有把这些字段写入 Kafka config。  
  影响：
  当前“支持 SSL”只覆盖到很基础的模式，证书型连接不可用。  
  位置：
  `KafkaToolWpf/KafkaToolWpf/Models/KafkaModels.cs` 第 86-99 行附近；  
  `KafkaToolWpf/KafkaToolWpf/ViewModels/MainWindowViewModel.cs` 第 468-543 行附近；  
  `KafkaToolWpf/KafkaToolWpf/Views/ConnectionConfigDialogWindow.xaml`。

- [x] 改善消费异常处理，避免直接弹 `StackTrace`。  
  现状：
  出现异常时 `MessageBox.Show(ex.StackTrace)`。  
  影响：
  对最终用户不友好，也没有统一错误提示策略。  
  位置：
  `KafkaToolWpf/KafkaToolWpf/ViewModels/MainWindowViewModel.cs` 第 666-674 行附近。

- [x] 调整默认连接地址和默认集群初始化逻辑。  
  现状：
  默认写死 `192.168.1.100:9092`。  
  影响：
  非该环境用户首次启动体验差，也容易误以为程序自带示例配置。  
  位置：
  `KafkaToolWpf/KafkaToolWpf/ViewModels/MainWindowViewModel.cs` 第 34、371-377 行附近。

## 2. 当前缺少的较重要功能

### 连接与环境管理

- [x] 本地持久化连接配置，并支持启动自动加载。  
  当前连接只保存在内存中，关闭应用后全部丢失。

- [x] 支持连接配置导入/导出。  
  适合多环境切换、团队共享测试集群、备份恢复。

- [x] 支持更完整的安全连接能力。  
  包括 SSL 证书文件、SASL_SSL 证书校验、常见企业认证场景。

### Topic 管理

- [x] 支持 Topic 配置编辑。  
  例如 `retention.ms`、`cleanup.policy`、`max.message.bytes` 等，而不仅是创建时写一次。

- [x] 支持分区扩容。  
  这是 Kafka Topic 管理中的高频操作，目前只能创建和删除。

- [x] 支持查看 Topic 更完整的运行信息。  
  例如每个分区 leader/ISR 异常状态、高水位、消息积压概况。

### 消费者组管理

- [x] 支持更完整的 offset 重置策略。  
  除了重置到 `0`，还应支持 earliest、latest、指定 offset、指定时间、按分区重置。

- [x] 支持先预览再执行 offset 重置。  
  当前是直接危险操作，缺少影响范围确认。

- [x] 支持查看活跃成员的真实分配关系。  
  包括 client id、host、topic-partition assignment，便于排查 rebalance 和消费倾斜。

### 消息生产与浏览

- [x] 支持消息格式化和校验。  
  例如 JSON 美化、JSON 合法性校验、常见 header 模板。

- [x] 支持消息搜索、过滤和复制导出。  
  至少应支持按 key、value 关键字、partition、offset 范围过滤，并支持复制整条消息。

- [ ] 支持更常见的 Kafka 序列化场景。  
  当前基本只适合纯字符串；实际项目常见 Avro、Protobuf、JSON Schema、二进制消息，需要解码能力。

- [x] 支持浏览结果分页或持续拉取。  
  当前一次只读固定条数，排查长区间消息不方便。

- [x] 支持“重发消息 / 复制为生产模板”。  
  这是排查和补消息时很实用的能力。

## 3. 工程与交付层面的缺口

- [x] 增加基础 README。  
  当前 `readme.md` 为空，缺少安装方式、依赖说明、截图、支持的 Kafka 连接模式、已知限制。

- [ ] 增加最基本的测试或可验证脚本。  
  目前没有看到针对连接配置、消息转换、业务逻辑的自动化验证。

- [x] 增加统一日志能力。  
  现在大多依赖状态栏和 `MessageBox`，定位线上问题会比较困难。

- [x] 统一“可继续扩展”的 UI 结构。  
  当前部分功能已经走向管理台形态，但仍混用 MessageBox、临时弹窗、主界面右侧空白区，后续扩展成本会越来越高。

## 4. 建议的落地顺序

- [x] 第一阶段：先修复 P0 问题。  
  优先保证在 SASL/SSL 集群下“能连、能看、能查、能改”。

- [x] 第二阶段：修复 P1 中的数据正确性和连接管理问题。  
  尤其是消费者组详情、SASL 机制绑定、连接编辑能力、消息 header 展示。

- [x] 第三阶段：补齐基础可用性和配置层问题。  
  P0、P1、P2 中已确认的问题已全部完成，下一阶段以“关键功能补齐”为主。

- [x] 第四阶段：补齐连接与 Topic 的关键管理能力。  
  连接持久化、导入导出、Topic 配置编辑、分区扩容和运行信息展示已完成。

- [x] 第五阶段：补齐消费者组与消息管理的常用能力。  
  offset 多策略重置、预览确认、搜索导出、继续读取、生产模板复用已完成。

- [ ] 第六阶段：补齐剩余的重要管理能力。  
  建议优先做“序列化解码支持、消息高级检索、按分区精细化 offset 重置”。

- [ ] 第七阶段：补工程能力。  
  README、日志已补齐，剩余重点是基础测试与后续诊断能力，补完后项目会更适合持续维护和分发。
