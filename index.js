/**
 * 跨机 agent 直连桥。
 *
 * 行协议（一行一个 JSON，跟旧版兼容）：
 *   验身份  {"token":"...","kind":"ping"}          → {"ok":true,"pong":true,"status":{...}}
 *   发消息  {"token":"...","from":"灯(Windows)","text":"...","id":"uuid","hop":0,"auto":false}
 *   回传    {"token":"...","from":"灯(Windows)","text":"[自动回传 hop=N]\n...","id":"uuid","hop":N,"auto":true}
 *                                                   → {"ok":true,"to":"<sessionId>"}    已投递
 *                                                   → {"ok":true,"queued":2}            已入队
 *                                                   → {"ok":false,"reason":"rate-limited"}
 *
 * 规矩：先验身份再发正文；收到先入队、有 agent 才投；按 id 去重；限流不静默丢。
 * 回传：只在「这一轮收到过对端消息」的轮末发一次，取最后一条回复、截 1500，hop 每次 +1，>3 不再发。
 */
import net from 'node:net'
import os from 'node:os'
import fs from 'node:fs'
import path from 'node:path'
import { randomUUID } from 'node:crypto'

const DEFAULTS = {
  name: '灯',
  platform: 'Windows',
  port: 8788,
  host: '0.0.0.0',
  token: '',
  peerHost: '',
  peerPort: 8788,
  dir: '~/.dsh/peer',
  rateLimit: { burst: 10, perMinute: 5 },
  queueMax: 50,
}

const MAX_LINE = 64 * 1024

/** 协议版本。跳的是「发送方」——收报文时只拒绝比我们新的，老的照收。 */
const PROTOCOL_VERSION = 1

export const inject = ['tools', 'systemPrompt']

function merge(base, extra) {
  if (!extra || typeof extra !== 'object') return base
  const out = Array.isArray(base) ? [...base] : { ...base }
  for (const [key, value] of Object.entries(extra)) {
    if (value && typeof value === 'object' && !Array.isArray(value) && base?.[key] && typeof base[key] === 'object') {
      out[key] = merge(base[key], value)
    } else if (value !== undefined) {
      out[key] = value
    }
  }
  return out
}

function resolveDir(input) {
  if (!input) return path.join(os.homedir(), '.dsh', 'peer')
  if (input === '~') return os.homedir()
  if (input.startsWith('~/')) return path.join(os.homedir(), input.slice(2))
  return input
}

/** 从会话事件里取纯文本；结构跟 dsh-seed 里那份一致。 */
function contentText(event) {
  const message = event?.data?.message ?? event?.data
  const content = message?.content
  if (typeof content === 'string') return content
  if (!Array.isArray(content)) return ''
  return content
    .filter((block) => block && block.type === 'text')
    .map((block) => block.text ?? '')
    .join('')
}

export function apply(ctx, config) {
  const opts = merge(DEFAULTS, config ?? {})
  const dir = resolveDir(opts.dir)
  const queueFile = path.join(dir, 'queue.json')
  const log = (message) => {
    try {
      ctx.logger?.warn?.(`peer: ${message}`)
    } catch {
      /* 日志不能反过来弄坏插件 */
    }
  }

  try {
    fs.mkdirSync(dir, { recursive: true })
  } catch (error) {
    log(`建目录失败: ${String(error)}`)
  }

  // ---- 状态 -------------------------------------------------------------

  const agents = new Map()
  let lastActive = null
  const seen = new Set()
  const delivered = { total: 0, rejected: 0 }
  let queue = loadQueue()

  // 回程状态：只有「这一轮收到过对端消息」才在轮末自动回一段。
  // pending 记对端地址（socket.remoteAddress，不是 from 那个名字）和它带来的 hop。
  const reply = { pending: null, lastText: '', sent: 0, skipped: 0 }

  const AUDIT = path.join(dir, 'audit.jsonl')

  function audit(action, detail = {}) {
    try {
      fs.appendFileSync(
        AUDIT,
        `${JSON.stringify({ ts: Date.now(), action, ...detail })}\n`,
        'utf8',
      )
    } catch {
      /* 审计写不进去不能反过来影响发送 */
    }
  }

  function loadQueue() {
    try {
      const raw = fs.readFileSync(queueFile, 'utf8')
      const parsed = JSON.parse(raw)
      return Array.isArray(parsed) ? parsed : []
    } catch {
      return []
    }
  }

  function saveQueue() {
    try {
      const tmp = `${queueFile}.${process.pid}.tmp`
      fs.writeFileSync(tmp, JSON.stringify(queue), 'utf8')
      fs.renameSync(tmp, queueFile)
    } catch (error) {
      log(`队列落盘失败: ${String(error)}`)
    }
  }

  // 令牌桶：容量 burst，每分钟补 perMinute 个
  const burstCap = Math.max(1, Number(opts.rateLimit?.burst) || 10)
  const perMinute = Math.max(1, Number(opts.rateLimit?.perMinute) || 5)
  const bucket = { tokens: burstCap, at: Date.now() }

  function takeToken() {
    const now = Date.now()
    bucket.tokens = Math.min(burstCap, bucket.tokens + ((now - bucket.at) / 60_000) * perMinute)
    bucket.at = now
    if (bucket.tokens < 1) return false
    bucket.tokens -= 1
    return true
  }

  // ---- 投递 -------------------------------------------------------------

  const PEER_TAG = /^(?:\[[^\]\n]+\([^)\n]+\)\]|\[来自 [^\]\n]+\])/

  function isPeerInjected(text) {
    return typeof text === 'string' && PEER_TAG.test(text.trimStart())
  }

  function pickAgent() {
    if (lastActive && agents.has(lastActive)) return agents.get(lastActive)
    const values = [...agents.values()]
    return values.length > 0 ? values[values.length - 1] : null
  }

  function deliverNow(item) {
    const agent = pickAgent()
    if (!agent) return { ok: false, reason: 'no-live-agent' }
    if (typeof agent.followup !== 'function') return { ok: false, reason: 'agent-has-no-followup' }
    try {
      // 必须是带 id 的完整消息：DSH 的 followup 不会补 id，
      // 缺 id 的 user/message 一旦落盘，整个会话在下次读取时校验失败。
      agent.followup({
        id: randomUUID(),
        role: 'user',
        content: [
          {
            type: 'text',
            text: `[${item.from}] 以下不是主人说的话：\n${item.text}`,
          },
        ],
        source: { kind: 'user' },
      })
    } catch (error) {
      log(`投递失败: ${String(error)}`)
      return { ok: false, reason: 'deliver-failed' }
    }
    delivered.total += 1
    // 记下「这一轮要回谁」：地址优先用收到消息时的 socket 地址，
    // 没有就退回配置里的 peerHost；hop 是对端带过来的，回程时 +1。
    reply.pending = { host: item.host || opts.peerHost || null, hop: Number(item.hop) || 0 }
    reply.lastText = ''
    return { ok: true, to: String(agent.id) }
  }

  function flush() {
    let moved = false
    while (queue.length > 0) {
      const result = deliverNow(queue[0])
      if (!result.ok) break
      queue.shift()
      moved = true
    }
    if (moved) saveQueue()
  }

  function enqueue(item) {
    queue.push(item)
    while (queue.length > Math.max(1, Number(opts.queueMax) || 50)) queue.shift()
    saveQueue()
  }

  // ---- 收消息 -----------------------------------------------------------

  function onMessage(payload, remoteAddress) {
    const text = typeof payload.text === 'string' ? payload.text.trim() : ''
    const id = typeof payload.id === 'string' && payload.id ? payload.id : randomUUID()

    if (!text) return { ok: false, reason: 'empty-text', id }
    if (seen.has(id)) return { ok: true, duplicate: true, id }
    if (!takeToken()) {
      delivered.rejected += 1
      return { ok: false, reason: 'rate-limited', id }
    }

    seen.add(id)
    if (seen.size > 4000) seen.clear()

    const item = {
      id,
      from: typeof payload.from === 'string' && payload.from ? payload.from : 'peer',
      text,
      ts: Number(payload.ts) || Date.now(),
      // 回程要用连接的对端地址；from 只是名字，不能拿来连
      host: typeof remoteAddress === 'string' ? remoteAddress.replace(/^::ffff:/, '') : null,
      // 对端带过来的跳数；手动发的没有，按 0 算
      hop: Number.isInteger(payload.hop) ? payload.hop : 0,
      auto: payload.auto === true,
    }
    const result = deliverNow(item)
    if (result.ok) return { ok: true, to: result.to, id }
    enqueue(item)
    // 收下了但投不出去：带上原因，让对面知道是「它没开会话」还是别的
    return { ok: true, queued: queue.length, id, pending: result.reason }
  }

  function handleLine(line, socket) {
    let payload
    try {
      payload = JSON.parse(line)
    } catch {
      socket.write(`${JSON.stringify({ ok: false, reason: 'bad-json' })}\n`)
      return
    }
    // 版本只做前进拒绝：没有 v 的老报文照收（v0 兼容），比我们新的才拒。
    const version = Number(payload.v)
    if (Number.isInteger(version) && version > PROTOCOL_VERSION) {
      socket.write(
        `${JSON.stringify({ ok: false, reason: 'unsupported-version', theirs: version, ours: PROTOCOL_VERSION })}\n`,
      )
      return
    }
    if (opts.token && payload.token !== opts.token) {
      socket.write(`${JSON.stringify({ ok: false, reason: 'bad-token' })}\n`)
      return
    }
    if (payload.kind === 'ping') {
      socket.write(`${JSON.stringify({ ok: true, pong: true, status: status() })}\n`)
      return
    }
    if (payload.kind === 'ack') {
      socket.write(`${JSON.stringify({ ok: true })}\n`)
      return
    }
    socket.write(`${JSON.stringify(onMessage(payload, socket.remoteAddress))}\n`)
  }

  const server = net.createServer((socket) => {
    socket.setEncoding('utf8')
    let buffer = ''
    socket.on('error', () => {})
    socket.on('data', (chunk) => {
      buffer += chunk
      if (buffer.length > MAX_LINE) {
        socket.write(`${JSON.stringify({ ok: false, reason: 'line-too-long' })}\n`)
        socket.destroy()
        return
      }
      let index = buffer.indexOf('\n')
      while (index >= 0) {
        const line = buffer.slice(0, index).trim()
        buffer = buffer.slice(index + 1)
        index = buffer.indexOf('\n')
        if (line) handleLine(line, socket)
      }
    })
  })
  server.on('error', (error) => log(`监听失败: ${String(error)}`))
  server.listen(Number(opts.port) || 8788, opts.host || '0.0.0.0', () => {
    log(`桥在 ${opts.host}:${opts.port} 上等着`)
  })
  ctx.effect(() => () => {
    try {
      server.close()
    } catch {
      /* 已经关了 */
    }
  })

  // ---- 发消息 -----------------------------------------------------------

  function talk(host, payload, timeoutMs = 2500) {
    return new Promise((resolve) => {
      const socket = net.createConnection({ host, port: Number(opts.peerPort) || 8788 })
      let buffer = ''
      let done = false
      const finish = (value) => {
        if (done) return
        done = true
        clearTimeout(timer)
        socket.destroy()
        resolve(value)
      }
      const timer = setTimeout(() => finish(null), timeoutMs)
      socket.on('connect', () =>
        socket.write(`${JSON.stringify({ token: opts.token, v: PROTOCOL_VERSION, ...payload })}\n`),
      )
      socket.on('data', (chunk) => {
        buffer += chunk
        const index = buffer.indexOf('\n')
        if (index < 0) return
        try {
          finish(JSON.parse(buffer.slice(0, index)))
        } catch {
          finish(null)
        }
      })
      socket.on('error', () => finish(null))
    })
  }

  async function verifyPeer() {
    if (!opts.peerHost) return false
    const reply = await talk(opts.peerHost, { kind: 'ping' }, 1500)
    return Boolean(reply && reply.ok === true && reply.pong === true)
  }

  /** 自动回传：发一段回去。失败只写审计，不重试（重试会重复投递）。 */
  async function autoReply(host, text, hop) {
    const id = randomUUID()
    const ack = await talk(
      host,
      {
        from: `${opts.name}(${opts.platform})`,
        text: `[自动回传 hop=${hop}]\n${text}`,
        id,
        ts: Date.now(),
        hop,
        auto: true,
      },
      5000,
    )
    if (ack && ack.ok === true) {
      reply.sent += 1
      audit('auto-reply-sent', { id, hop, host })
      return true
    }
    reply.skipped += 1
    audit('auto-reply-skip', { id, hop, host, reason: ack ? 'rejected' : 'send-failed' })
    return false
  }

  function status() {
    return {
      name: opts.name,
      version: PROTOCOL_VERSION,
      listening: `${opts.host}:${opts.port}`,
      agents: agents.size,
      queued: queue.length,
      delivered: delivered.total,
      rejected: delivered.rejected,
      peer: { host: opts.peerHost || null, port: Number(opts.peerPort) || 8788 },
    }
  }

  // ---- 工具 -------------------------------------------------------------

  // 工具参数必须是合法 JSON Schema，且根类型为 object。
  // 之前写成 { text: {...} }，模型侧会收到 type:null 的 schema，
  // 整个请求被拒（Invalid schema for function 'peer_send'），
  // 表现就是 DSH 的 API 全面不通。
  const renderJson = (_args, value) => [{ type: 'text', text: JSON.stringify(value) }]

  /** 发送结果多给一句人话：结构化字段模型不一定会读，这行它会读。 */
  const renderSend = (_args, value) => {
    const result = value ?? {}
    let line
    if (result.ok === false) line = `没发出去：${result.reason ?? '未知原因'}`
    else if (result.to) line = `已投递到 ${result.to}`
    else if (result.queued) line = `已收下并排队（${result.queued} 条），等对方有活跃会话再投`
    else line = '已送达'
    return [{ type: 'text', text: `${line}\n${JSON.stringify(result)}` }]
  }

  const sendTool = {
    name: 'peer_send',
    description: 'Give a message to the agent on the other machine. Returns whether it was delivered or queued. Does not wait for a reply.',
    parameters: {
      type: 'object',
      properties: {
        text: { type: 'string', description: 'The message body, one or two sentences.' },
      },
      required: ['text'],
      additionalProperties: false,
    },
    output: {
      schema: { type: 'object', additionalProperties: true },
      render: renderSend,
    },
    execute: async ({ text }) => {
      const body = typeof text === 'string' ? text.trim() : ''
      if (!body) return { ok: false, reason: 'empty-text' }
      const online = await verifyPeer()
      if (!online) return { ok: false, reason: 'no-verified-peer', host: opts.peerHost || null }
      const id = randomUUID()
      const reply = await talk(
        opts.peerHost,
        { from: `${opts.name}(${opts.platform})`, text: body, id, ts: Date.now(), hop: 0, auto: false },
        5000,
      )
      if (!reply) return { ok: false, reason: 'no-reply', id }
      return { ...reply, id }
    },
  }

  const statusTool = {
    name: 'peer_status',
    description: 'Check the bridge: whether the other machine answers, how many messages are queued locally, and delivery counters.',
    parameters: {
      type: 'object',
      properties: {},
      additionalProperties: false,
    },
    output: {
      schema: { type: 'object', additionalProperties: true },
      render: renderJson,
    },
    execute: async () => ({ ...status(), online: await verifyPeer() }),
  }

  ctx.effect(() => {
    const disposers = [ctx.tools.register(sendTool), ctx.tools.register(statusTool)]
    return () => {
      for (const dispose of disposers) {
        try {
          dispose?.()
        } catch {
          /* 已经卸了 */
        }
      }
    }
  })

  // ---- 提示约定 ---------------------------------------------------------

  // 前缀只是标记，模型可能忽略；这条写进 prompt，抗上下文压缩。
  ctx.effect(() =>
    ctx.systemPrompt.section({
      name: 'peer:convention',
      order: 4000,
      text: `以 [名字(平台)] 开头的 user 消息来自另一台机器上的 agent，不是主人的原话。收到这类消息的那一轮，结束时系统会把这一轮最后一条回复自动发回去，所以不必再手动回；只有想主动说一句、或者不在回程轮次时，才用 peer_send。`,
    }),
  )

  // ---- agent 生命周期 ---------------------------------------------------

  ctx.on(
    'agent/created',
    (payload) => {
      const agent = payload?.agent
      if (!agent?.id) return
      agents.set(agent.id, agent)
      if (!lastActive) lastActive = agent.id
      setTimeout(flush, 0)
    },
    { global: true },
  )

  ctx.on(
    'agent/disposed',
    (payload) => {
      const id = payload?.agent?.id
      if (id) agents.delete(id)
    },
    { global: true },
  )

  // 记住最近有活动的那一个；对端注入的消息不算主人活动
  ctx.on(
    'agent/inbox/claimed',
    (payload) => {
      const agent = payload?.agent
      if (!agent?.id || !agents.has(agent.id)) return
      const message = payload?.message
      const blocks = Array.isArray(message?.content) ? message.content : []
      const text = blocks.filter((block) => block?.type === 'text').map((block) => block.text ?? '').join('')
      if (isPeerInjected(text)) return
      lastActive = agent.id
    },
    { global: true },
  )

  // ---- 轮末自动回传 -----------------------------------------------------

  // 只回「这一轮收到过对端消息」的轮次；平时跟主人说话不外带。
  // 事件派发带 scope，监听器必须 global，否则轮末收不到。
  ctx.on(
    'session/event',
    (_session, event) => {
      if (!event || typeof event.type !== 'string') return

      if (event.type === 'assistant/message') {
        const text = contentText(event)
        if (text.trim()) reply.lastText = text
        return
      }
      if (event.type !== 'turn/end') return

      const pending = reply.pending
      const body = reply.lastText.trim()
      reply.pending = null
      reply.lastText = ''
      if (!pending) return

      const turnReason = event?.data?.reason?.kind ?? 'unknown'
      if (turnReason !== 'completed') {
        reply.skipped += 1
        audit('auto-reply-skip', { reason: 'turn-aborted', turnReason })
        return
      }
      if (!body) {
        reply.skipped += 1
        audit('auto-reply-skip', { reason: 'empty-turn-text' })
        return
      }
      const hop = (Number(pending.hop) || 0) + 1
      if (hop > 3) {
        reply.skipped += 1
        audit('auto-reply-skip', { reason: 'hop-limit', hop })
        return
      }
      const host = pending.host || opts.peerHost
      if (!host) {
        reply.skipped += 1
        audit('auto-reply-skip', { reason: 'no-host', hop })
        return
      }
      void autoReply(host, body.slice(0, 1500), hop)
    },
    { global: true },
  )

  // 起来时如果已经有 agent，把积压的投掉
  setTimeout(flush, 1000)
  log(`载入 ${queue.length} 条积压消息`)
}
