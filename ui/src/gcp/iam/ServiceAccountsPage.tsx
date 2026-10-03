import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Chip,
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
import {
  deleteServiceAccount,
  listServiceAccounts,
  type ServiceAccount,
} from '../../api/gcp/iam'
import { useAccount } from '../../context/AccountContext'
import { CreateServiceAccountDialog } from './CreateServiceAccountDialog'
import { GcpPageTitle } from '../common/PageTitle'

/** IAM service accounts in the project, with create and delete. */
export function ServiceAccountsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'iam'] })

  const accounts = useQuery({
    queryKey: ['gcp', 'iam', 'serviceAccounts', accountId],
    queryFn: listServiceAccounts,
  })

  const remove = useMutation({
    mutationFn: (account: ServiceAccount) => deleteServiceAccount(account.email),
    onSuccess: invalidate,
  })

  return (
    <Box>
      <Stack
        direction="row"
        sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <Box>
          <GcpPageTitle id="iam">IAM</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            Service accounts · project {accountId || '—'}
          </Typography>
        </Box>
        <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
          Create service account
        </Button>
      </Stack>

      {accounts.isError && <Alert severity="error">Failed to load service accounts.</Alert>}
      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Email</TableCell>
              <TableCell>Display name</TableCell>
              <TableCell>Status</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {accounts.isLoading && (
              <TableRow>
                <TableCell colSpan={4} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!accounts.isLoading && (accounts.data?.accounts.length ?? 0) === 0 && (
              <TableRow>
                <TableCell colSpan={4} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No service accounts in this project.
                </TableCell>
              </TableRow>
            )}
            {accounts.data?.accounts.map((account) => (
              <TableRow key={account.email} hover>
                <TableCell>
                  <Link
                    component={RouterLink}
                    to={`/gcp/iam/service-accounts/${encodeURIComponent(account.email)}`}
                  >
                    {account.email}
                  </Link>
                </TableCell>
                <TableCell>{account.displayName || '—'}</TableCell>
                <TableCell>
                  <Chip
                    size="small"
                    label={account.disabled ? 'Disabled' : 'Enabled'}
                    color={account.disabled ? 'default' : 'success'}
                  />
                </TableCell>
                <TableCell align="right">
                  <Tooltip title="Delete service account">
                    <span>
                      <IconButton
                        size="small"
                        disabled={remove.isPending}
                        onClick={() => remove.mutate(account)}
                        aria-label={`Delete ${account.email}`}
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

      <CreateServiceAccountDialog open={createOpen} onClose={() => setCreateOpen(false)} />
    </Box>
  )
}
