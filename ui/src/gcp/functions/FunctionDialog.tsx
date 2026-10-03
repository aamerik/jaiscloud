import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControl,
  FormControlLabel,
  InputLabel,
  MenuItem,
  Select,
  Stack,
  Switch,
  TextField,
  Typography,
} from '@mui/material'
import { createFunction, type CreateFunctionInput } from '../../api/gcp/functions'
import { parseKV } from './util'

type SourceMode = 'none' | 'archive' | 'inline'

function emptyInput(): CreateFunctionInput {
  return {
    id: '',
    location: 'us-central1',
    runtime: 'nodejs20',
    entryPoint: 'handler',
    description: '',
    triggerType: 'http',
    availableMemoryMB: 256,
    timeout: '60s',
  }
}

export interface FunctionDialogProps {
  open: boolean
  onClose: () => void
  onSaved?: (id: string) => void
}

/** Create a Cloud Functions function (gen2). */
export function FunctionDialog({ open, onClose, onSaved }: FunctionDialogProps) {
  const queryClient = useQueryClient()
  const [input, setInput] = useState<CreateFunctionInput>(emptyInput())
  const [sourceMode, setSourceMode] = useState<SourceMode>('none')
  const [sourceInline, setSourceInline] = useState('')
  const [sourceFilename, setSourceFilename] = useState('index.js')
  const [labelsText, setLabelsText] = useState('')
  const [envText, setEnvText] = useState('')

  const set = <K extends keyof CreateFunctionInput>(key: K, value: CreateFunctionInput[K]) =>
    setInput((cur) => ({ ...cur, [key]: value }))

  const reset = () => {
    setInput(emptyInput())
    setSourceMode('none')
    setSourceInline('')
    setSourceFilename('index.js')
    setLabelsText('')
    setEnvText('')
  }

  const save = useMutation({
    mutationFn: (body: CreateFunctionInput) => createFunction(body),
    onSuccess: (saved) => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'functions'] })
      onSaved?.(saved.id)
      reset()
      onClose()
    },
  })

  const isEvent = input.triggerType === 'event'
  const valid =
    Boolean(input.id && input.location && input.runtime) &&
    (!isEvent || Boolean(input.eventType)) &&
    (sourceMode !== 'inline' || sourceInline.trim().length > 0)

  const handleSave = () => {
    const body: CreateFunctionInput = {
      ...input,
      sourceArchiveUrl: sourceMode === 'archive' ? input.sourceArchiveUrl : undefined,
      sourceInline: sourceMode === 'inline' ? sourceInline : undefined,
      sourceFilename: sourceMode === 'inline' ? sourceFilename || 'index.js' : undefined,
      labels: parseKV(labelsText),
      environmentVariables: parseKV(envText),
    }
    save.mutate(body)
  }

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
      <DialogTitle>Create function</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {save.isError && (
            <Alert severity="error">Could not create the function: {(save.error as Error).message}</Alert>
          )}
          <Stack direction="row" spacing={2}>
            <TextField
              autoFocus
              label="Function name"
              value={input.id}
              onChange={(e) => set('id', e.target.value)}
              placeholder="my-function"
              fullWidth
            />
            <TextField
              label="Region"
              value={input.location}
              onChange={(e) => set('location', e.target.value)}
              placeholder="us-central1"
              fullWidth
            />
          </Stack>
          <Stack direction="row" spacing={2}>
            <TextField
              label="Runtime"
              value={input.runtime}
              onChange={(e) => set('runtime', e.target.value)}
              placeholder="nodejs20"
              fullWidth
            />
            <TextField
              label="Entry point"
              value={input.entryPoint}
              onChange={(e) => set('entryPoint', e.target.value)}
              placeholder="handler"
              fullWidth
            />
          </Stack>

          <FormControl fullWidth>
            <InputLabel id="functions-trigger-type">Trigger</InputLabel>
            <Select
              labelId="functions-trigger-type"
              label="Trigger"
              value={input.triggerType}
              onChange={(e) => set('triggerType', e.target.value as CreateFunctionInput['triggerType'])}
            >
              <MenuItem value="http">HTTP</MenuItem>
              <MenuItem value="event">Event</MenuItem>
            </Select>
          </FormControl>
          {isEvent && (
            <>
              <TextField
                label="Event type"
                value={input.eventType ?? ''}
                onChange={(e) => set('eventType', e.target.value)}
                placeholder="google.cloud.pubsub.topic.v1.messagePublished"
                fullWidth
              />
              <TextField
                label="Event resource"
                value={input.eventResource ?? ''}
                onChange={(e) => set('eventResource', e.target.value)}
                placeholder="projects/my-project/topics/my-topic"
                fullWidth
              />
              <FormControlLabel
                control={<Switch checked={Boolean(input.retry)} onChange={(e) => set('retry', e.target.checked)} />}
                label="Retry failed invocations"
              />
            </>
          )}

          <Stack direction="row" spacing={2}>
            <TextField
              label="Memory (MB)"
              type="number"
              value={input.availableMemoryMB ?? ''}
              onChange={(e) => set('availableMemoryMB', Number(e.target.value))}
              fullWidth
            />
            <TextField
              label="Timeout"
              value={input.timeout ?? ''}
              onChange={(e) => set('timeout', e.target.value)}
              placeholder="60s"
              fullWidth
            />
            <TextField
              label="Max instances"
              type="number"
              value={input.maxInstanceCount ?? ''}
              onChange={(e) => set('maxInstanceCount', Number(e.target.value))}
              fullWidth
            />
          </Stack>

          <TextField
            label="Description"
            value={input.description}
            onChange={(e) => set('description', e.target.value)}
            fullWidth
          />

          <Box>
            <Typography variant="subtitle2" sx={{ mb: 1 }}>
              Source (optional)
            </Typography>
            <FormControl fullWidth sx={{ mb: 1 }}>
              <InputLabel id="functions-source-mode">Source</InputLabel>
              <Select
                labelId="functions-source-mode"
                label="Source"
                value={sourceMode}
                onChange={(e) => setSourceMode(e.target.value as SourceMode)}
              >
                <MenuItem value="none">No source (metadata-only)</MenuItem>
                <MenuItem value="archive">gs:// archive URL</MenuItem>
                <MenuItem value="inline">Inline file</MenuItem>
              </Select>
            </FormControl>
            {sourceMode === 'archive' && (
              <TextField
                label="Source archive URL"
                value={input.sourceArchiveUrl ?? ''}
                onChange={(e) => set('sourceArchiveUrl', e.target.value)}
                placeholder="gs://my-bucket/source.zip"
                fullWidth
              />
            )}
            {sourceMode === 'inline' && (
              <Stack spacing={1}>
                <TextField
                  label="File name"
                  value={sourceFilename}
                  onChange={(e) => setSourceFilename(e.target.value)}
                  placeholder="index.js"
                  fullWidth
                />
                <TextField
                  label="Source"
                  value={sourceInline}
                  onChange={(e) => setSourceInline(e.target.value)}
                  multiline
                  minRows={8}
                  fullWidth
                  slotProps={{ input: { sx: { fontFamily: 'monospace', fontSize: 13 } } }}
                />
              </Stack>
            )}
          </Box>

          <Stack direction="row" spacing={2}>
            <TextField
              label="Labels (key=value per line)"
              value={labelsText}
              onChange={(e) => setLabelsText(e.target.value)}
              multiline
              minRows={2}
              fullWidth
            />
            <TextField
              label="Environment variables (key=value per line)"
              value={envText}
              onChange={(e) => setEnvText(e.target.value)}
              multiline
              minRows={2}
              fullWidth
            />
          </Stack>
          {(labelsText || envText) && (
            <Typography variant="caption" color="text.secondary">
              {`labels: ${JSON.stringify(parseKV(labelsText) ?? {})} · env: ${JSON.stringify(parseKV(envText) ?? {})}`}
            </Typography>
          )}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button variant="contained" disabled={!valid || save.isPending} onClick={handleSave}>
          Create
        </Button>
      </DialogActions>
    </Dialog>
  )
}
