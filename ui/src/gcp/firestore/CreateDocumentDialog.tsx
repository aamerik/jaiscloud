import { useEffect, useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Stack,
  TextField,
  Typography,
} from '@mui/material'
import { createDocument } from '../../api/gcp/firestore'
import { JsonEditor } from './JsonEditor'
import { parseJsonObject } from './util'

const EMPTY = '{\n  \n}'

export interface CreateDocumentDialogProps {
  open: boolean
  onClose: () => void
  /** When set, the collection is fixed and cannot be edited. */
  collection?: string
  onCreated?: (collection: string, documentId: string) => void
}

/** Create a document. Firestore collections are implicit, so a collection id
 * supplied here is created on the first write. */
export function CreateDocumentDialog({
  open,
  onClose,
  collection,
  onCreated,
}: CreateDocumentDialogProps) {
  const queryClient = useQueryClient()
  const [collectionId, setCollectionId] = useState(collection ?? '')
  const [documentId, setDocumentId] = useState('')
  const [text, setText] = useState(EMPTY)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (open) {
      setCollectionId(collection ?? '')
      setDocumentId('')
      setText(EMPTY)
      setError(null)
    }
  }, [open, collection])

  const create = useMutation({
    mutationFn: () => {
      const parsed = parseJsonObject(text)
      if (parsed.error || !parsed.value) throw new Error(parsed.error ?? 'invalid document')
      return createDocument(collectionId, {
        documentId: documentId || undefined,
        fields: parsed.value,
      })
    },
    onSuccess: (doc) => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'firestore'] })
      onCreated?.(doc.collection, doc.id)
      onClose()
    },
  })

  const submit = () => {
    const parsed = parseJsonObject(text)
    setError(parsed.error ?? null)
    if (parsed.error) return
    if (!collectionId) {
      setError('Collection ID is required')
      return
    }
    create.mutate()
  }

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
      <DialogTitle>Create document</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {create.isError && (
            <Alert severity="error">
              Could not create the document: {(create.error as Error).message}
            </Alert>
          )}
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
            <TextField
              autoFocus
              label="Collection ID"
              value={collectionId}
              onChange={(e) => setCollectionId(e.target.value)}
              disabled={Boolean(collection)}
              placeholder="users"
              fullWidth
            />
            <TextField
              label="Document ID (optional)"
              value={documentId}
              onChange={(e) => setDocumentId(e.target.value)}
              placeholder="auto-generated"
              fullWidth
            />
          </Stack>
          <Typography variant="caption" color="text.secondary">
            Fields use Firestore&apos;s typed encoding, e.g. {'{'} <code>name</code>:{' '}
            {'{'} <code>stringValue</code>: &quot;Ada&quot; {'}'}, <code>age</code>:{' '}
            {'{'} <code>integerValue</code>: &quot;42&quot; {'}'} {'}'}. Values keep their exact
            type when saved.
          </Typography>
          <JsonEditor label="Fields" value={text} onChange={setText} error={error} />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button variant="contained" disabled={!collectionId || create.isPending} onClick={submit}>
          Create
        </Button>
      </DialogActions>
    </Dialog>
  )
}
