import { lazy, Suspense } from 'react'
import { Spinner } from '@cloudscape-design/components'
import type { JsonEditorProps } from './JsonEditorImpl'

const Impl = lazy(() =>
  import('./JsonEditorImpl').then((module) => ({ default: module.JsonEditorImpl })),
)

/** Lazily-loaded JSON code editor; ace is fetched on first render. */
export function JsonEditor(props: JsonEditorProps) {
  return (
    <Suspense fallback={<Spinner size="large" />}>
      <Impl {...props} />
    </Suspense>
  )
}
