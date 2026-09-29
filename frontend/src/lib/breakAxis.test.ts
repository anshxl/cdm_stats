/// <reference types="node" />
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { buildBreakAxis } from './breakAxis.ts'

const DAY = 86_400_000
const d = (n: number) => n * DAY
const opts = { gapDays: 21, width: 1000, breakWidth: 30 }
const close = (a: number, b: number, msg?: string) => assert.ok(Math.abs(a - b) < 1e-6, msg ?? `${a} != ${b}`)

test('no gap: linear mapping over [0, width]', () => {
  const ax = buildBreakAxis([d(0), d(5), d(10)], opts)
  close(ax.toX(d(0)), 0)
  close(ax.toX(d(5)), 500)
  close(ax.toX(d(10)), 1000)
  assert.equal(ax.breaks.length, 0)
  assert.equal(ax.segments.length, 1)
})

test('one gap: empty span removed, breakWidth inserted', () => {
  // Segment A: days 0-10, gap to day 50, segment B: days 50-60.
  const ax = buildBreakAxis([d(0), d(10), d(50), d(60)], opts)
  assert.equal(ax.breaks.length, 1)
  const b = ax.breaks[0]
  assert.equal(b.from, d(10))
  assert.equal(b.to, d(50))
  close(b.x1 - b.x0, 30)
  close(ax.toX(d(10)), b.x0)
  close(ax.toX(d(50)), b.x1)
  close(ax.toX(d(0)), 0)
  close(ax.toX(d(60)), 1000)
})

test('segment widths are proportional to their durations', () => {
  // A lasts 30 days, B lasts 10 days -> 3:1 of the 970px left after the break.
  const ax = buildBreakAxis([d(0), d(15), d(30), d(100), d(110)], opts)
  const [a, b] = ax.segments
  close(a.x1 - a.x0, 970 * 0.75)
  close(b.x1 - b.x0, 970 * 0.25)
  assert.equal(a.start, d(0))
  assert.equal(a.end, d(30))
  assert.equal(b.start, d(100))
  assert.equal(b.end, d(110))
})

test('two gaps', () => {
  const ax = buildBreakAxis([d(0), d(10), d(40), d(50), d(80), d(90)], opts)
  assert.equal(ax.segments.length, 3)
  assert.equal(ax.breaks.length, 2)
  for (const s of ax.segments) close(s.x1 - s.x0, (1000 - 60) / 3)
  close(ax.breaks[0].x0, ax.segments[0].x1)
  close(ax.breaks[0].x1, ax.segments[1].x0)
  close(ax.breaks[1].x0, ax.segments[1].x1)
  close(ax.breaks[1].x1, ax.segments[2].x0)
  close(ax.toX(d(90)), 1000)
})

test('exactly 21 days is not a gap; 22 is', () => {
  assert.equal(buildBreakAxis([d(0), d(21)], opts).breaks.length, 0)
  assert.equal(buildBreakAxis([d(0), d(22)], opts).breaks.length, 1)
})

test('unsorted, duplicate input is handled', () => {
  const ax = buildBreakAxis([d(60), d(0), d(10), d(10), d(50)], opts)
  assert.equal(ax.breaks.length, 1)
  close(ax.toX(d(0)), 0)
  close(ax.toX(d(60)), 1000)
})

test('single date / all-same dates do not divide by zero', () => {
  for (const dates of [[d(3)], [d(3), d(3), d(3)]]) {
    const ax = buildBreakAxis(dates, opts)
    const x = ax.toX(d(3))
    assert.ok(Number.isFinite(x))
    assert.ok(x >= 0 && x <= 1000)
    assert.equal(ax.breaks.length, 0)
    assert.ok(ax.ticks().every((t) => Number.isFinite(t.x)))
  }
})

test('empty input does not throw', () => {
  const ax = buildBreakAxis([], opts)
  assert.ok(Number.isFinite(ax.toX(0)))
  assert.deepEqual(ax.breaks, [])
})

test('toX is monotonic, including inside a gap and outside the range', () => {
  const ax = buildBreakAxis([d(0), d(10), d(50), d(60), d(100), d(101)], opts)
  let prev = -Infinity
  for (let t = d(-5); t <= d(110); t += DAY / 4) {
    const x = ax.toX(t)
    assert.ok(x >= prev, `toX not monotonic at day ${t / DAY}`)
    prev = x
  }
})

test('ticks fall inside segments, never inside a break, and map through toX', () => {
  const ax = buildBreakAxis([d(0), d(30), d(100), d(110)], opts)
  const ticks = ax.ticks(6)
  assert.ok(ticks.length >= 2)
  for (const s of ax.segments) assert.ok(ticks.some((t) => t.t >= s.start && t.t <= s.end), 'every segment gets a tick')
  for (const tk of ticks) {
    assert.ok(ax.segments.some((s) => tk.t >= s.start && tk.t <= s.end))
    close(tk.x, ax.toX(tk.t))
    assert.equal(tk.t % DAY, 0, 'ticks land on UTC midnight')
  }
})
