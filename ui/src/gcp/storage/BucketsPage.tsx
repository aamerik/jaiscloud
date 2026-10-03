import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  IconButton,
  Link,
  MenuItem,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { Link as RouterLink } from 'react-router-dom'
import { createBucket, deleteBucket, listBuckets } from '../../api/gcp/storage'
import { useAccount } from '../../context/AccountContext'
import { GcpPageTitle } from '../common/PageTitle'

const LOCATIONS = ['US', 'EU', 'ASIA']

function shortDate(value?: string): string {
  if (!value) return '—'
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? value : d.toLocaleString()
}

/** Cloud Storage bucket list — the sample GCP service page. */
export function BucketsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [name, setName] = useState('')
  const [location, setLocation] = useState('US')

  const buckets = useQuery({
    queryKey: ['gcp', 'storage', 'buckets', accountId],
    queryFn: listBuckets,
  })

  const create = useMutation({
    mutationFn: createBucket,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'storage', 'buckets'] })
      setCreateOpen(false)
      setName('')
      setLocation('US')
    },
  })

  const remove = useMutation({
    mutationFn: deleteBucket,
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'storage', 'buckets'] }),
  })

  return (
    <Box>
      <Stack
        direction="row"
        sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <Box>
          <GcpPageTitle id="storage">Buckets</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            Cloud Storage · project {accountId || '—'}
          </Typography>
        </Box>
        <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
          Create bucket
        </Button>
      </Stack>

      {buckets.isError && <Alert severity="error">Failed to load buckets.</Alert>}
      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed. The bucket may not be empty.
        </Alert>
      )}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Name</TableCell>
              <TableCell>Location</TableCell>
              <TableCell>Storage class</TableCell>
              <TableCell>Created</TableCell>
              <TableCell>Versioning</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {buckets.isLoading && (
              <TableRow>
                <TableCell colSpan={6} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!buckets.isLoading && (buckets.data?.items.length ?? 0) === 0 && (
              <TableRow>
                <TableCell colSpan={6} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No buckets in this project.
                </TableCell>
              </TableRow>
            )}
            {buckets.data?.items.map((bucket) => (
              <TableRow key={bucket.name} hover>
                <TableCell>
                  <Link component={RouterLink} to={`/gcp/storage/buckets/${bucket.name}`}>
                    {bucket.name}
                  </Link>
                </TableCell>
                <TableCell>{bucket.location || '—'}</TableCell>
                <TableCell>{bucket.storageClass || 'STANDARD'}</TableCell>
                <TableCell>{shortDate(bucket.timeCreated)}</TableCell>
                <TableCell>{bucket.versioning ? 'Enabled' : 'Off'}</TableCell>
                <TableCell align="right">
                  <Tooltip title="Delete bucket">
                    <span>
                      <IconButton
                        size="small"
                        disabled={remove.isPending}
                        onClick={() => remove.mutate(bucket.name)}
                        aria-label={`Delete ${bucket.name}`}
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

      <Dialog open={createOpen} onClose={() => setCreateOpen(false)} fullWidth maxWidth="xs">
        <DialogTitle>Create bucket</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ mt: 1 }}>
            {create.isError && <Alert severity="error">Could not create the bucket.</Alert>}
            <TextField
              autoFocus
              label="Name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="my-bucket"
              fullWidth
            />
            <TextField
              select
              label="Location"
              value={location}
              onChange={(e) => setLocation(e.target.value)}
              fullWidth
            >
              {LOCATIONS.map((loc) => (
                <MenuItem key={loc} value={loc}>
                  {loc}
                </MenuItem>
              ))}
            </TextField>
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setCreateOpen(false)}>Cancel</Button>
          <Button
            variant="contained"
            disabled={!name || create.isPending}
            onClick={() => create.mutate({ name, location })}
          >
            Create
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  )
}
