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
} from '@mui/material'
import { createSecret } from '../../api/gcp/secretmanager'
import { parseKeyValueLines } from './util'

export interface CreateSecretDialogProps {
  open: boolean
  onClose: () => void
}

/** Create a Secret Manager secret. */
export function CreateSecretDialog({ open, onClose }: CreateSecretDialogProps) {
  const queryClient = useQueryClient()
  const [secretId, setSecretId] = useState('')
  const [labels, setLabels] = useState('')
  const [rotationPeriod, setRotationPeriod] = useState('')

  useEffect(() => {
    if (open) {
      setSecretId('')
      setLabels('')
      setRotationPeriod('')
    }
  }, [open])

  const create = useMutation({
    mutationFn: () =>
      createSecret({
        secretId,
        labels: parseKeyValueLines(labels),
        rotationPeriod: rotationPeriod || undefined,
      }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'secretmanager'] })
      onClose()
    },
  })

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>Create secret</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {create.isError && (
            <Alert severity="error">
              Could not create the secret: {(create.error as Error).message}
            </Alert>
          )}
          <TextField
            autoFocus
            label="Secret ID"
            value={secretId}
            onChange={(e) => setSecretId(e.target.value)}
            placeholder="db-password"
            fullWidth
          />
          <TextField
            label="Labels (one key=value per line, optional)"
            value={labels}
            onChange={(e) => setLabels(e.target.value)}
            multiline
            minRows={2}
            fullWidth
          />
          <TextField
            label="Rotation period (optional)"
            value={rotationPeriod}
            onChange={(e) => setRotationPeriod(e.target.value)}
            placeholder="7776000s"
            fullWidth
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button
          variant="contained"
          disabled={!secretId || create.isPending}
          onClick={() => create.mutate()}
        >
          Create
        </Button>
      </DialogActions>
    </Dialog>
  )
}
