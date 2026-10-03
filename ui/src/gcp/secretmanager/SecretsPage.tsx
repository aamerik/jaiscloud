import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  IconButton,
  Link,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Tooltip,
  Typography,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { Link as RouterLink } from 'react-router-dom'
import { deleteSecret, listSecrets, type Secret } from '../../api/gcp/secretmanager'
import { useAccount } from '../../context/AccountContext'
import { CreateSecretDialog } from './CreateSecretDialog'

function labelSummary(labels?: Record<string, string>): string {
  if (!labels) return '—'
  const entries = Object.entries(labels)
  if (entries.length === 0) return '—'
  return entries.map(([k, v]) => `${k}=${v}`).join(', ')
}

/** Secret Manager secrets in the project, with create and delete. */
export function SecretsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const invalidate = () =>
    void queryClient.invalidateQueries({ queryKey: ['gcp', 'secretmanager'] })

  const secrets = useQuery({
    queryKey: ['gcp', 'secretmanager', 'secrets', accountId],
    queryFn: listSecrets,
  })

  const remove = useMutation({
    mutationFn: (secret: Secret) => deleteSecret(secret.secretId || ''),
    onSuccess: invalidate,
  })

  return (
    <Box>
      <Stack
        direction="row"
        sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <Box>
          <Typography variant="h5">Secret Manager</Typography>
          <Typography variant="body2" color="text.secondary">
            Secrets · project {accountId || '—'}
          </Typography>
        </Box>
        <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
          Create secret
        </Button>
      </Stack>

      {secrets.isError && <Alert severity="error">Failed to load secrets.</Alert>}
      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Secret ID</TableCell>
              <TableCell>Labels</TableCell>
              <TableCell>Rotation</TableCell>
              <TableCell>Created</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {secrets.isLoading && (
              <TableRow>
                <TableCell colSpan={5} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!secrets.isLoading && (secrets.data?.secrets.length ?? 0) === 0 && (
              <TableRow>
                <TableCell colSpan={5} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No secrets in this project.
                </TableCell>
              </TableRow>
            )}
            {secrets.data?.secrets.map((secret) => (
              <TableRow key={secret.name} hover>
                <TableCell>
                  <Link
                    component={RouterLink}
                    to={`/gcp/secretmanager/secrets/${encodeURIComponent(secret.secretId || '')}`}
                  >
                    {secret.secretId}
                  </Link>
                </TableCell>
                <TableCell>{labelSummary(secret.labels)}</TableCell>
                <TableCell>{secret.rotationPeriod || '—'}</TableCell>
                <TableCell>{secret.createTime || '—'}</TableCell>
                <TableCell align="right">
                  <Tooltip title="Delete secret">
                    <span>
                      <IconButton
                        size="small"
                        disabled={remove.isPending}
                        onClick={() => remove.mutate(secret)}
                        aria-label={`Delete ${secret.secretId}`}
                      >
                        <DeleteOutlineIcon fontSize="small" />
                      </IconButton>
                    </span>
                  </Tooltip>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>

      <CreateSecretDialog open={createOpen} onClose={() => setCreateOpen(false)} />
    </Box>
  )
}
