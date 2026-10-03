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
import { updateSecret } from '../../api/gcp/secretmanager'
import { formatKeyValueLines, parseKeyValueLines } from './util'

export interface EditSecretDialogProps {
  open: boolean
  secret: string
  labels?: Record<string, string>
  rotationPeriod?: string
  onClose: () => void
}

/** Edit a secret's labels and rotation period; absent fields are preserved. */
export function EditSecretDialog({
  open,
  secret,
  labels,
  rotationPeriod,
  onClose,
}: EditSecretDialogProps) {
  const queryClient = useQueryClient()
  const [labelText, setLabelText] = useState('')
  const [rotation, setRotation] = useState('')

  useEffect(() => {
    if (open) {
      setLabelText(formatKeyValueLines(labels))
      setRotation(rotationPeriod ?? '')
    }
  }, [open, labels, rotationPeriod])

  const update = useMutation({
    mutationFn: () =>
      updateSecret(secret, {
        labels: parseKeyValueLines(labelText),
        rotationPeriod: rotation || undefined,
      }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'secretmanager'] })
      onClose()
    },
  })

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>Edit secret</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {update.isError && (
            <Alert severity="error">Could not update: {(update.error as Error).message}</Alert>
          )}
          <TextField
            label="Labels (one key=value per line)"
            value={labelText}
            onChange={(e) => setLabelText(e.target.value)}
            multiline
            minRows={3}
            fullWidth
          />
          <TextField
            label="Rotation period"
            value={rotation}
            onChange={(e) => setRotation(e.target.value)}
            placeholder="7776000s"
            fullWidth
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button variant="contained" disabled={update.isPending} onClick={() => update.mutate()}>
          Save
        </Button>
      </DialogActions>
    </Dialog>
  )
}
