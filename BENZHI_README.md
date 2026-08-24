# EventPush 实时事件推送网关

EventPush 是一个面向设备与业务系统的实时事件推送网关。客户端通过
WebSocket 长连接订阅主题，服务端把发布的事件按主题广播到订阅连接，
并基于确认跟踪、断线续推、心跳驱逐与背压流控保证投递的稳定与有序。

## 功能

- WebSocket 长连接接入：`GET /ws?session=<id>&topics=<t1,t2>`
- 主题订阅树：支持多级主题（如 `device/a/sensor`）与引用计数
- 广播分发：按批次快照投递，订阅时机不影响批次完整性
- 投递确认：客户端 `ack <seq>` 后记录连续确认位置
- 断线续推：重连后从最后确认位置补发，并与实时事件合并为有序投递
- 心跳与驱逐：超时连接先进入 suspected，再被安全驱逐并释放资源
- 背压流控：慢消费者受写队列与令牌桶限制，不拖垮分发路径
- 事件存储：有界环形缓冲与提交游标，发布失败的事件可重试

## 构建与运行

环境要求：Go 1.23+（vendor 目录已包含全部依赖，可离线构建）。

```bash
go build -mod=vendor -o eventpush ./cmd/eventpush
./eventpush -addr :8080
```

启动后：

- 健康检查：`GET /healthz`
- 状态总览：`GET /api/status`
- 发布事件：`POST /api/publish`，请求体 `{"topic":"device/a","payload":"hello"}`
- 订阅控制台：`GET /`（浏览器打开，可直接订阅、发布与查看实时事件）

## 前端控制台

页面文件位于 `web/console.html`，编译时通过 `go:embed` 打进二进制，
运行后访问根路径即可使用。控制台支持建立 WebSocket 连接、订阅/退订
主题、发布测试事件、确认已收事件以及查看服务状态。

## Docker

```bash
docker build -f benzhi.Dockerfile -t eventpush .
docker run --rm -p 8080:8080 eventpush
```

镜像内关闭模块下载（GOPROXY=off），全部依赖走 vendor 离线构建。
