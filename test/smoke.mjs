/**
 * 离线自测：不开 DSH，用假 ctx 跑两个实例互发。
 * 覆盖：投递、入队后补投、按 id 去重、限流、工具返回、状态查询。
 */
import net from 'node:net'
import os from 'node:os'
import fs from 'node:fs'
import path from 'node:path'
import { randomUUID } from 'node:crypto'
import { apply } from '../index.js'

const TOKEN = 'test-token'

function fakeCtx() {
  const handlers = new Map()
  const tools = new Map()
  const inbox = []
  const ctx = {
    logger: { warn() {} },
    on(name, fn) {
      const list = handlers.get(name) ?? []
      list.push(fn)
      handlers.set(name, list)
    },
    effect(fn) {
      const dispose = fn()
      return () => {
        try {
          dispose?.()
        } catch {}
      }
    },
    tools: {
      register(definition) {
        tools.set(definition.name, definition)
        return () => tools.delete(definition.name)
      },
    },
    systemPrompt: { section: () => () => {} },
  }
  return {
    ctx,
    tools,
    inbox,
    emit(name, payload) {
      for (const fn of handlers.get(name) ?? []) fn(payload)
    },
  }
}

function talk(port, payload, timeoutMs = 2000) {
  return new Promise((resolve) => {
    const socket = net.createConnection({ host: '127.0.0.1', port })
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
    socket.on('connect', () => socket.write(`${JSON.stringify(payload)}\n`))
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

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
const results = []
const check = (name, ok, detail) => {
  results.push({ name, ok })
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail && !ok ? `  -> ${JSON.stringify(detail)}` : ''}`)
}

const root = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-peer-'))

const A = fakeCtx()
apply(A.ctx, { name: '灯', platform: 'Windows', port: 18790, token: TOKEN, host: '127.0.0.1', dir: path.join(root, 'a') })

const B = fakeCtx()
apply(B.ctx, {
  name: '尺',
  platform: 'Mac',
  port: 18791,
  token: TOKEN,
  host: '127.0.0.1',
  peerHost: '127.0.0.1',
  peerPort: 18790,
  dir: path.join(root, 'b'),
})

const C = fakeCtx()
apply(C.ctx, {
  name: '尺',
  platform: 'Mac',
  port: 18792,
  token: TOKEN,
  host: '127.0.0.1',
  peerHost: '127.0.0.1',
  peerPort: 18793,
  dir: path.join(root, 'c'),
  rateLimit: { burst: 1, perMinute: 0.1 },
})

await sleep(250)

// 1) 没 agent 时入队
// tool.execute 有时回对象、有时回 JSON 字符串，两种都接住
const asObject = (value) => (typeof value === 'string' ? JSON.parse(value) : value)
const queuedValue = asObject(await B.tools.get('peer_send').execute({ text: '第一条，入队' }))
check('无 agent 时入队', queuedValue.ok === true && queuedValue.queued === 1, queuedValue)
check('入队时 A 没有收到', A.inbox.length === 0, A.inbox.length)

// 2) agent 起来后补投
A.emit('agent/created', { agent: { id: 'sess-a', followup: (message) => A.inbox.push(message) } })
await sleep(150)
check('agent 起来后补投', A.inbox.length === 1, A.inbox.length)
const injected = A.inbox[0]?.content?.[0]?.text ?? ''
check('注入带对端标识和免责句', injected.startsWith('[peer v1 from=尺(Mac) ') && injected.includes('不是主人说的话'), injected)

// 3) 有 agent 时直接投递
const direct = asObject(await B.tools.get('peer_send').execute({ text: '第二条，直达' }))
check('有 agent 时直达', direct.ok === true && direct.to === 'sess-a', direct)
check('直达不入队', direct.queued === undefined, direct)

// 4) 按 id 去重
const id = randomUUID()
const first = await talk(18790, { token: TOKEN, from: '尺(Mac)', text: '重复测试', id })
const second = await talk(18790, { token: TOKEN, from: '尺(Mac)', text: '重复测试', id })
check('第一次正常收', first?.ok === true, first)
check('同 id 第二次判重', second?.duplicate === true, second)

// 5) 限流：burst 1，第二条应被拒
const r1 = await talk(18792, { token: TOKEN, from: '灯(Windows)', text: '限流一', id: randomUUID() })
const r2 = await talk(18792, { token: TOKEN, from: '灯(Windows)', text: '限流二', id: randomUUID() })
check('限流首条通过', r1?.ok === true, r1)
check('限流次条被拒且不静默', r2?.ok === false && r2.reason === 'rate-limited', r2)

// 6) 错 token 被拦
const bad = await talk(18790, { token: 'wrong', from: 'x', text: 'y', id: randomUUID() })
check('错 token 被拒', bad?.ok === false && bad.reason === 'bad-token', bad)

// 7) 状态查询
const status = asObject(await B.tools.get('peer_status').execute({}))
check('状态能报到在线', status.online === true && status.queued === 0, status)

fs.rmSync(root, { recursive: true, force: true })

const failed = results.filter((item) => !item.ok)
console.log(`\n${results.length - failed.length}/${results.length} 通过`)
process.exit(failed.length === 0 ? 0 : 1)
