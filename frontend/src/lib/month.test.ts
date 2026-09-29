/// <reference types="node" />
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { currentMonth, daysInMonth, monthLabel, monthOptions, readMonth } from './month.ts'

test('daysInMonth handles leap years and December', () => {
  assert.equal(daysInMonth('2026-09'), 30)
  assert.equal(daysInMonth('2026-12'), 31)
  assert.equal(daysInMonth('2028-02'), 29)
  assert.equal(daysInMonth('2026-02'), 28)
})

test('monthLabel is the long month and year', () => {
  assert.equal(monthLabel('2026-09'), 'September 2026')
  assert.equal(monthLabel('2027-01'), 'January 2027')
})

test('currentMonth uses the bot timezone (Asia/Kolkata)', () => {
  assert.equal(currentMonth(new Date('2026-09-30T18:00:00Z')), '2026-09')
  assert.equal(currentMonth(new Date('2026-09-30T20:00:00Z')), '2026-10')
})

test('readMonth accepts only YYYY-MM', () => {
  assert.equal(readMonth('2026-08'), '2026-08')
  for (const bad of [null, '', '2026-13', '2026-8', 'sept', '2026-08-01']) assert.equal(readMonth(bad), null)
})

test('monthOptions: newest first, always includes current and selected', () => {
  assert.deepEqual(monthOptions(['2026-07', '2026-08'], '2026-09', '2026-09'), ['2026-09', '2026-08', '2026-07'])
  assert.deepEqual(monthOptions(['2026-08', '2026-09'], '2026-09', '2026-08'), ['2026-09', '2026-08'])
  assert.deepEqual(monthOptions([], '2026-09', '2025-01'), ['2026-09', '2025-01'])
})
