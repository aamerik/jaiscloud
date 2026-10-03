import { describe, expect, it } from 'vitest'
import { filterRows, pageCount, paginate } from './pagination'

const rows = [0, 1, 2, 3, 4, 5, 6]

describe('paginate', () => {
  it('slices the requested page', () => {
    expect(paginate(rows, 0, 3)).toEqual([0, 1, 2])
    expect(paginate(rows, 2, 3)).toEqual([6])
  })

  it('returns every row when the size is non-positive', () => {
    expect(paginate(rows, 1, 0)).toEqual(rows)
  })

  it('clamps a negative page to the first page', () => {
    expect(paginate(rows, -1, 3)).toEqual([0, 1, 2])
  })
})

describe('pageCount', () => {
  it('rounds up and never drops below one', () => {
    expect(pageCount(7, 3)).toBe(3)
    expect(pageCount(0, 3)).toBe(1)
    expect(pageCount(7, 0)).toBe(1)
  })
})

describe('filterRows', () => {
  const people = [
    { name: 'Alice', role: 'admin' },
    { name: 'Bob', role: 'viewer' },
  ]

  it('matches case-insensitively over the derived text', () => {
    expect(filterRows(people, 'ali', (p) => p.name)).toEqual([people[0]])
    expect(filterRows(people, 'ADMIN', (p) => p.role)).toEqual([people[0]])
  })

  it('returns all rows for a blank query', () => {
    expect(filterRows(people, '   ', (p) => p.name)).toEqual(people)
  })
})
