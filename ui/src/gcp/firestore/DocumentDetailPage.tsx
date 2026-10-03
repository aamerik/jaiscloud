import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  Divider,
  IconButton,
  Stack,
  Typography,
} from '@mui/material'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import SaveOutlinedIcon from '@mui/icons-material/SaveOutlined'
import { Link as RouterLink, useNavigate, useParams } from 'react-router-dom'
import { APIError } from '../../api/client'
import { deleteDocument, getDocument, updateDocument } from '../../api/gcp/firestore'
import { useAccount } from '../../context/AccountContext'
import { JsonEditor } from './JsonEditor'
import { parseJsonObject } from './util'
import { GcpPageTitle } from '../common/PageTitle'

function shortDate(value?: string): string {
  if (!value) return '—'
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? value : d.toLocaleString()
}

/** A single Firestore document: edit the fields as JSON and save or delete. */
export function DocumentDetailPage() {
  const { collection = '', document: documentId = '' } = useParams()
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const [text, setText] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [saved, setSaved] = useState(false)

  const query = useQuery({
    queryKey: ['gcp', 'firestore', 'document', collection, documentId, accountId],
    queryFn: () => getDocument(collection, documentId),
    enabled: Boolean(collection && documentId),
  })

  // Seed the editor whenever a fresh document snapshot arrives.
  useEffect(() => {
    if (query.data) {
      setText(JSON.stringify(query.data.fields ?? {}, null, 2))
      setError(null)
      setSaved(false)
    }
  }, [query.data])

  const save = useMutation({
    mutationFn: (fields: Record<string, unknown>) =>
      updateDocument(collection, documentId, { fields, updateTime: query.data?.updateTime }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'firestore'] })
      setSaved(true)
    },
  })

  const remove = useMutation({
    mutationFn: () => deleteDocument(collection, documentId),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'firestore'] })
      navigate(`/gcp/firestore/collections/${encodeURIComponent(collection)}`)
    },
  })

  const submit = () => {
    const parsed = parseJsonObject(text)
    setError(parsed.error ?? null)
    setSaved(false)
    if (!parsed.error && parsed.value) save.mutate(parsed.value)
  }

  const conflicted =
    save.error instanceof APIError &&
    (save.error.code === 'FailedPrecondition' || save.error.status === 409 || save.error.status === 412)

  return (
    <Box>
      <Stack
        direction="row"
        spacing={1}
        sx={{ alignItems: 'center', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <IconButton
          component={RouterLink}
          to={`/gcp/firestore/collections/${encodeURIComponent(collection)}`}
          aria-label="Back to documents"
        >
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ flexGrow: 1, minWidth: 0 }}>
          <GcpPageTitle id="firestore">{documentId}</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            {collection} · project {accountId || '—'}
          </Typography>
        </Box>
        <Button
          variant="contained"
          startIcon={<SaveOutlinedIcon />}
          disabled={save.isPending || query.isLoading}
          onClick={submit}
        >
          Save
        </Button>
        <Button
          color="error"
          startIcon={<DeleteOutlineIcon />}
          disabled={remove.isPending}
          onClick={() => remove.mutate()}
        >
          Delete
        </Button>
      </Stack>

      {query.isError && <Alert severity="error">Failed to load the document.</Alert>}
      {query.isLoading && <CircularProgress size={24} />}
      {conflicted && (
        <Alert
          severity="warning"
          sx={{ mb: 2 }}
          action={
            <Button color="inherit" size="small" onClick={() => void query.refetch()}>
              Reload
            </Button>
          }
        >
          This document changed since it was loaded. Reload before saving to avoid overwriting.
        </Alert>
      )}
      {save.isError && !conflicted && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Save failed: {(save.error as Error).message}
        </Alert>
      )}
      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}
      {saved && !save.isPending && (
        <Alert severity="success" sx={{ mb: 2 }}>
          Document saved.
        </Alert>
      )}

      {query.data && (
        <Box sx={{ maxWidth: 900 }}>
          <Stack spacing={0.5} sx={{ mb: 2 }}>
            <Typography variant="caption" color="text.secondary">
              Full resource name
            </Typography>
            <Typography variant="body2" sx={{ overflowWrap: 'anywhere' }}>
              {query.data.name}
            </Typography>
            <Typography variant="caption" color="text.secondary" sx={{ mt: 1 }}>
              Created {shortDate(query.data.createTime)} · updated {shortDate(query.data.updateTime)}
            </Typography>
            <Divider sx={{ mt: 1 }} />
          </Stack>
          <JsonEditor
            label="Fields (Firestore value encoding)"
            value={text}
            onChange={setText}
            error={error}
            minRows={16}
            disabled={save.isPending}
          />
          <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
            Saving replaces all fields with the encoded object above, guarded by the loaded
            updateTime.
          </Typography>
        </Box>
      )}
    </Box>
  )
}
