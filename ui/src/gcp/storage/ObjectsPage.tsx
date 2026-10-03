import { useQuery } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Breadcrumbs,
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
  Typography,
} from '@mui/material'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import FolderOutlinedIcon from '@mui/icons-material/FolderOutlined'
import { Link as RouterLink, useParams, useSearchParams } from 'react-router-dom'
import { listObjects } from '../../api/gcp/storage'
import { useAccount } from '../../context/AccountContext'

function formatSize(value?: string): string {
  if (!value) return '—'
  const n = Number(value)
  if (!Number.isFinite(n)) return value
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`
  return `${(n / (1024 * 1024)).toFixed(1)} MiB`
}

/** Object browser for one Cloud Storage bucket. */
export function ObjectsPage() {
  const { bucket = '' } = useParams()
  const [params, setParams] = useSearchParams()
  const prefix = params.get('prefix') ?? ''
  const { accountId } = useAccount()

  const objects = useQuery({
    queryKey: ['gcp', 'storage', 'objects', accountId, bucket, prefix],
    queryFn: () => listObjects(bucket, prefix || undefined),
    enabled: bucket !== '',
  })

  return (
    <Box>
      <Stack direction="row" spacing={1} sx={{ alignItems: 'center', mb: 2 }}>
        <IconButton component={RouterLink} to="/gcp/storage/buckets" aria-label="Back to buckets">
          <ArrowBackIcon />
        </IconButton>
        <Box>
          <Typography variant="h5">{bucket}</Typography>
          <Breadcrumbs separator="/">
            <Link component={RouterLink} to="/gcp/storage/buckets">
              Buckets
            </Link>
            <Typography color="text.secondary">{prefix || bucket}</Typography>
          </Breadcrumbs>
        </Box>
      </Stack>

      {objects.isError && <Alert severity="error">Failed to list objects.</Alert>}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Name</TableCell>
              <TableCell>Size</TableCell>
              <TableCell>Type</TableCell>
              <TableCell>Updated</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {objects.isLoading && (
              <TableRow>
                <TableCell colSpan={4} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {objects.data?.prefixes?.map((folder) => (
              <TableRow
                key={folder}
                hover
                sx={{ cursor: 'pointer' }}
                onClick={() => setParams({ prefix: folder })}
              >
                <TableCell sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <FolderOutlinedIcon fontSize="small" color="action" />
                  {folder}
                </TableCell>
                <TableCell>—</TableCell>
                <TableCell>Folder</TableCell>
                <TableCell>—</TableCell>
              </TableRow>
            ))}
            {objects.data?.items.map((object) => (
              <TableRow key={object.name} hover>
                <TableCell>{object.name}</TableCell>
                <TableCell>{formatSize(object.size)}</TableCell>
                <TableCell>{object.contentType || '—'}</TableCell>
                <TableCell>{object.updated || '—'}</TableCell>
              </TableRow>
            ))}
            {!objects.isLoading &&
              (objects.data?.items.length ?? 0) === 0 &&
              (objects.data?.prefixes?.length ?? 0) === 0 && (
                <TableRow>
                  <TableCell colSpan={4} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                    No objects{prefix ? ` under ${prefix}` : ''}.
                  </TableCell>
                </TableRow>
              )}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  )
}
