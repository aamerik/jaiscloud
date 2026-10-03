import { describe, expect, it } from 'vitest'
import { parseJsonObject } from './util'

describe('parseJsonObject', () => {
  it('parses a JSON object', () => {
    const { value, error } = parseJsonObject('{"a":1,"b":true}')
    expect(error).toBeUndefined()
    expect(value).toEqual({ a: 1, b: true })
  })

  it('rejects invalid JSON', () => {
    const { value, error } = parseJsonObject('{')
    expect(value).toBeUndefined()
    expect(error).toBeTruthy()
  })

  it('rejects non-object top-level values', () => {
    for (const text of ['[1,2]', '"str"', '42', 'null']) {
      const { error } = parseJsonObject(text)
      expect(error).toBe('Document data must be a JSON object')
    }
  })
})
