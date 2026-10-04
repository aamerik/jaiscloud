import { describe, expect, it } from 'vitest'
import {
  collectionId,
  isNestedCollection,
  parentDocument,
  parseJsonObject,
  pathSegments,
} from './util'

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

describe('firestore path helpers', () => {
  it('splits paths into non-empty segments', () => {
    expect(pathSegments('users/alice/orders')).toEqual(['users', 'alice', 'orders'])
    expect(pathSegments('/users//orders/')).toEqual(['users', 'orders'])
    expect(pathSegments('users')).toEqual(['users'])
  })

  it('returns the leaf collection id', () => {
    expect(collectionId('users')).toBe('users')
    expect(collectionId('users/alice/orders')).toBe('orders')
  })

  it('detects nested collections', () => {
    expect(isNestedCollection('users')).toBe(false)
    expect(isNestedCollection('users/alice/orders')).toBe(true)
    // Even-segment (document) paths are not collections.
    expect(isNestedCollection('users/alice')).toBe(false)
    expect(isNestedCollection('users/alice/orders/o1')).toBe(false)
  })

  it('locates the parent document of a nested collection', () => {
    expect(parentDocument('users')).toBeUndefined()
    expect(parentDocument('users/alice')).toBeUndefined()
    expect(parentDocument('users/alice/orders')).toEqual({
      collection: 'users',
      document: 'alice',
    })
    expect(parentDocument('users/alice/orders/o1/invoices')).toEqual({
      collection: 'users/alice/orders',
      document: 'o1',
    })
  })
})
