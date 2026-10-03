import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  Link,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  Typography,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import { Link as RouterLink } from 'react-router-dom'
import { listKeyRings } from '../../api/gcp/kms'
import { useAccount } from '../../context/AccountContext'
import { CreateKeyRingDialog } from './CreateKeyRingDialog'

/** KMS key rings in one location, with create. */
export function KeyRingsPage() {
  const { accountId } = useAccount()
  const [location, setLocation] = useState('global')
  const [createOpen, setCreateOpen] = useState(false)

  const keyRings = useQuery({
    queryKey: ['gcp', 'kms', 'keyRings', location, accountId],
    queryFn: () => listKeyRings(location),
    enabled: Boolean(location),
  })

  return (
    <Box>
      <Stack
        direction="row"
        sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <Box>
          <Typography variant="h5">Cloud KMS</Typography>
          <Typography variant="body2" color="text.secondary">
            Key rings · project {accountId || '—'}
          </Typography>
        </Box>
        <Stack direction="row" spacing={1} sx={{ alignItems: 'center' }}>
          <TextField
            label="Location"
            value={location}
            onChange={(e) => setLocation(e.target.value)}
            size="small"
          />
          <Button
            variant="contained"
            startIcon={<AddIcon />}
            disabled={!location}
            onClick={() => setCreateOpen(true)}
          >
            Create key ring
          </Button>
        </Stack>
      </Stack>

      {keyRings.isError && <Alert severity="error">Failed to load key rings.</Alert>}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Key ring</TableCell>
              <TableCell>Location</TableCell>
              <TableCell>Created</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {keyRings.isLoading && (
              <TableRow>
                <TableCell colSpan={3} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!keyRings.isLoading && (keyRings.data?.keyRings.length ?? 0) === 0 && (
              <TableRow>
                <TableCell colSpan={3} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No key rings in {location || 'this location'}.
                </TableCell>
              </TableRow>
            )}
            {keyRings.data?.keyRings.map((keyRing) => (
              <TableRow key={keyRing.name} hover>
                <TableCell>
                  <Link
                    component={RouterLink}
                    to={`/gcp/kms/keyrings/${encodeURIComponent(keyRing.location || location)}/${encodeURIComponent(keyRing.keyRingId || '')}`}
                  >
                    {keyRing.keyRingId}
                  </Link>
                </TableCell>
                <TableCell>{keyRing.location}</TableCell>
                <TableCell>{keyRing.createTime || '—'}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>

      <CreateKeyRingDialog open={createOpen} location={location} onClose={() => setCreateOpen(false)} />
    </Box>
  )
}
