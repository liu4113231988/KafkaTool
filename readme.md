# KafkaTool

KafkaTool 是一个基于 `.NET 8 + WPF + Prism` 的 Kafka 桌面管理工具，面向本地单机 Kafka 和企业集群场景，支持连接管理、Topic 管理、消费者组管理、消息生产、实时消费、消息浏览与集群信息查看。

## 当前能力

- 支持 `PLAINTEXT`、`SSL`、`SASL_PLAINTEXT`、`SASL_SSL` 连接模式
- 支持保存多个连接配置，并在启动时自动加载
- 支持连接配置导入、导出和切换
- 支持 Topic 创建、删除、详情查看、配置编辑、分区扩容
- 支持消费者组列表、Lag 详情、成员分配查看、偏移量预览重置
- 支持消息发送、JSON 格式化与校验、Headers 输入
- 支持实时消费、按偏移量浏览、按时间浏览、继续读取下一页
- 支持从浏览结果复制消息为生产模板
- 支持基础文件日志，便于排查连接、管理和消息操作失败问题

## 运行环境

- Windows 10/11
- `.NET SDK 8.0` 或以上
- 可访问的 Kafka 环境
- 如果使用安全集群，需要准备对应的 SASL 账号或 SSL 证书文件

## 本地开发

```powershell
dotnet restore .\KafkaTool.sln
dotnet build .\KafkaTool.sln -nologo
dotnet run --project .\KafkaToolWpf\KafkaToolWpf\KafkaToolWpf.csproj
```

## 发布建议

项目主程序位于：

- [KafkaToolWpf.csproj](/D:/workingfold/KafkaTool/KafkaToolWpf/KafkaToolWpf/KafkaToolWpf.csproj)

当前项目已开启：

- 单文件发布
- 原生库自解压
- WPF 桌面图标资源打包

可以使用下面的命令生成发布包：

```powershell
dotnet publish .\KafkaToolWpf\KafkaToolWpf\KafkaToolWpf.csproj -c Release -r win-x64 --self-contained true
```

## 连接配置与日志目录

- 连接配置保存路径：`%LocalAppData%\KafkaTool\connections.json`
- 运行日志目录：`%LocalAppData%\KafkaTool\logs\`

日志按天滚动，文件名格式为 `kafkatool-yyyyMMdd.log`。

## 典型使用方式

### 1. 连接 Kafka

- 在顶部连接栏选择已有连接，或进入“连接管理”新增环境
- 支持输入 `BootstrapServers`
- 支持选择是否启用 `SSL`、`SASL`
- 支持配置 `PLAIN`、`SCRAM-SHA-256`、`SCRAM-SHA-512`、`GSSAPI`
- 支持填写 `CA`、客户端证书、私钥文件路径

### 2. 管理 Topic

- 在“Topic 管理”页刷新列表并筛选 Topic
- 选择 Topic 后可查看分区、Leader、ISR、消息数量和风险摘要
- 支持在线修改配置项
- 支持按目标分区数执行扩容

### 3. 管理消费者组

- 在“消费者组”页查看 group 列表与成员信息
- 支持查看分区 Lag、ClientId、Host、Owner
- 支持按 `earliest`、`latest`、`specific`、`timestamp` 预览并重置偏移量
- 时间相关操作可直接在界面输入日期和时间

### 4. 生产与浏览消息

- 生产页支持 Topic、分区、Key、Headers、消息体输入
- JSON 消息支持格式化和合法性校验
- 浏览页支持按偏移量或按时间读取消息
- 可指定临时浏览 Group，避免影响正式消费组
- 可从浏览结果一键复制为生产模板，便于补发或回放

## UI 设计约定

主界面已统一为可扩展的区块式布局：

- 顶部固定连接栏
- 各页使用统一卡片区块承载功能
- 参数输入、状态提示、执行按钮采用统一风格
- 后续新增功能建议优先放入对应页签的新卡片区块，而不是继续追加零散弹窗

## 已知限制

- 当前消息体和 Header 仍以字符串为主，尚未内置 Avro / Protobuf / JSON Schema 解码
- 暂未提供自动化测试项目
- 日志能力目前聚焦客户端操作和异常定位，还没有独立日志查看界面

## 后续建议

- 增加序列化协议解码器
- 增加更细粒度的消息过滤与批量导出
- 增加日志查看页或诊断页
- 补齐基础自动化测试
