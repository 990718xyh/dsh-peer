# dsh-peer

在两台机器的 DSH 之间传消息，不经过任何服务器。

一台开 8788 端口等着，另一台连过来。收到消息投进当前会话，轮末自动把回复带回去。

## 一次对话长什么样

```
A 机                         B 机
peer_send("在吗")  ────────→  8788 收到，入队
                              有活跃会话就投进对话
                              agent 看到 [来自 A] 在吗
                   ←────────  轮末自动回传 hop=1
```

双方都不在线也没关系：消息落盘排队，下次有 agent 起来就投。

## 安装

```sh
# 浏览器版
dsh plugin --profile web add github:990718xyh/dsh-peer

# 桌面版
dsh plugin --profile desktop add github:990718xyh/dsh-peer
```

装完在配置里填两边的地址和同一个 token，重启 DSH。

## 配置

`~/.dsh/profiles/<profile>/cordis.patch.yml`：

```yaml
- id: peer
  name: 'dsh-peer'
  config:
    name: 灯
    platform: Windows
    host: 0.0.0.0
    port: 8788
    token: 换成你自己的
    peerHost: 192.168.0.107
    peerPort: 8788
    rateLimit: { burst: 10, perMinute: 5 }
```

`name` 是对方的称呼（消息前缀显示这个名字），`platform` 只是标识。两边 `token` 必须一样，否则收到 `bad-token` 直接断开。

## 两个工具

**`peer_send`** —— 发一条消息。返回 `{ok, queued, id}`：`ok:false` 说明对方没收到（原因在 `reason` 里），`queued` 是本地排队的条数。

**`peer_status`** —— 查通道状态：对方在不在、本地积压多少、投递/拒收计数。

## 线上跑的东西

- **监听**：`net.createServer` 起在配置的端口，先验 token 再收正文
- **队列**：落盘到 `<dir>/queue.json`，写入用 tmp + rename 原子替换；上限 50 条，先进先出；启动时把积压载回来
- **投递**：拿到活跃 agent 才投，没有就等；消息构造成带 id 的完整 user message
- **回传**：`turn/end` 时把这一轮的回复发回去，hop 加一，超上限就停

每一步都写审计：`auto-reply-sent` / `auto-reply-skip`（带原因）。hop 链断了能查到哪一环。

## 数据

`~/.dsh/peer/<id>/`：

```
queue.json    排队中的消息，投递成功才移除
audit.log     投递和回传记录
```

## 和 dsh-seed 的关系

两个插件，各管各的：

- **dsh-peer** 管跨机对话（这个）
- **dsh-seed** 管人格和记忆

桥不该出现在人格插件里，人格也不该知道有桥这回事。两个都装可以，只装一个也行。
