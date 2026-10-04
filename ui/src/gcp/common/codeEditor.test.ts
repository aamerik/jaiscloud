import { describe, expect, it } from 'vitest'
import {
  aceEditorOptions,
  aceModeFor,
  aceThemeFor,
  DEFAULT_MAX_LINES,
  DEFAULT_MIN_LINES,
} from './codeEditor'

describe('aceModeFor', () => {
  it('maps each console language to its ace mode', () => {
    expect(aceModeFor('json')).toBe('ace/mode/json')
    expect(aceModeFor('yaml')).toBe('ace/mode/yaml')
    expect(aceModeFor('text')).toBe('ace/mode/text')
  })
})

describe('aceThemeFor', () => {
  it('picks a light theme for light and a dark theme for dark', () => {
    expect(aceThemeFor('light')).toBe('ace/theme/dawn')
    expect(aceThemeFor('dark')).toBe('ace/theme/tomorrow_night')
  })
})

describe('aceEditorOptions', () => {
  it('enables folding and soft wrap and disables the worker', () => {
    const opts = aceEditorOptions({ language: 'json', theme: 'light', readOnly: false })
    expect(opts.mode).toBe('ace/mode/json')
    expect(opts.theme).toBe('ace/theme/dawn')
    expect(opts.showFoldWidgets).toBe(true)
    expect(opts.wrap).toBe(true)
    expect(opts.useWorker).toBe(false)
    expect(opts.minLines).toBe(DEFAULT_MIN_LINES)
    expect(opts.maxLines).toBe(DEFAULT_MAX_LINES)
  })

  it('turns the cursor line off when read-only', () => {
    const opts = aceEditorOptions({ language: 'json', theme: 'dark', readOnly: true })
    expect(opts.readOnly).toBe(true)
    expect(opts.highlightActiveLine).toBe(false)
    expect(opts.highlightGutterLine).toBe(false)
    expect(opts.theme).toBe('ace/theme/tomorrow_night')
  })

  it('clamps maxLines to at least minLines and floors minLines at one', () => {
    const opts = aceEditorOptions({
      language: 'yaml',
      theme: 'light',
      readOnly: false,
      minLines: 16,
      maxLines: 4,
    })
    expect(opts.minLines).toBe(16)
    expect(opts.maxLines).toBe(16)

    const tiny = aceEditorOptions({
      language: 'text',
      theme: 'light',
      readOnly: false,
      minLines: 0,
    })
    expect(tiny.minLines).toBe(1)
  })
})
