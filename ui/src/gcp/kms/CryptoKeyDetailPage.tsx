import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  IconButton,
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
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { Link as RouterLink, useParams } from 'react-router-dom'
import {
  createCryptoKeyVersion,
  destroyCryptoKeyVersion,
  disableCryptoKeyVersion,
  enableCryptoKeyVersion,
  getCryptoKey,
  getCryptoKeyIam,
  listCryptoKeyVersions,
  putCryptoKeyIam,
  setPrimaryVersion,
  type CryptoKeyVersion,
} from '../../api/gcp/kms'
import { useAccount } from '../../context/AccountContext'
import { IamPolicyPanel } from '../common/IamPolicyPanel'

function versionColor(state?: string): 'success' | 'default' | 'warning' | 'error' {
  switch (state) {
    case 'ENABLED':
      return 'success'
    case 'DESTROY_SCHEDULED':
      return 'warning'
    case 'DESTROYED':
      return 'error'
    default:
      return 'default'
  }
}

/** One KMS crypto key: metadata, versions and IAM policy. */
export function CryptoKeyDetailPage() {
  const { location = '', keyRing = '', key = '' } = useParams()
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'kms'] })

  const detail = useQuery({
    queryKey: ['gcp', 'kms', 'cryptoKey', location, keyRing, key, accountId],
    queryFn: () => getCryptoKey(location, keyRing, key),
    enabled: Boolean(location && keyRing && key),
  })

  const versions = useQuery({
    queryKey: ['gcp', 'kms', 'versions', location, keyRing, key, accountId],
    queryFn: () => listCryptoKeyVersions(location, keyRing, key),
    enabled: Boolean(location && keyRing && key),
  })

  const create = useMutation({
    mutationFn: () => createCryptoKeyVersion(location, keyRing, key),
    onSuccess: invalidate,
  })

  const toggle = useMutation({
    mutationFn: (version: CryptoKeyVersion) =>
      version.state === 'ENABLED'
        ? disableCryptoKeyVersion(location, keyRing, key, version.versionId || '')
        : enableCryptoKeyVersion(location, keyRing, key, version.versionId || ''),
    onSuccess: invalidate,
  })

  const destroy = useMutation({
    mutationFn: (version: CryptoKeyVersion) =>
      destroyCryptoKeyVersion(location, keyRing, key, version.versionId || ''),
    onSuccess: invalidate,
  })

  const promote = useMutation({
    mutationFn: (version: CryptoKeyVersion) =>
      setPrimaryVersion(location, keyRing, key, version.versionId || ''),
    onSuccess: invalidate,
  })

  return (
    <Box>
      <Stack
        direction="row"
        spacing={1}
        sx={{ alignItems: 'center', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <IconButton
          component={RouterLink}
          to={`/gcp/kms/keyrings/${encodeURIComponent(location)}/${encodeURIComponent(keyRing)}`}
          aria-label="Back to key ring"
        >
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ flexGrow: 1, minWidth: 0 }}>
          <Typography variant="h5" sx={{ overflowWrap: 'anywhere' }}>
            {key}
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Crypto key · {keyRing} · {location} · project {accountId || '—'}
          </Typography>
        </Box>
        <Button
          variant="contained"
          startIcon={<AddIcon />}
          disabled={create.isPending}
          onClick={() => create.mutate()}
        >
          Create version
        </Button>
      </Stack>

      {detail.isError && <Alert severity="error">Failed to load the crypto key.</Alert>}
      {detail.data && (
        <Box sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2, p: 2, mb: 3 }}>
          <Stack spacing={0.5}>
            <Typography variant="body2">Purpose: {detail.data.purpose || '—'}</Typography>
            <Typography variant="body2">Algorithm: {detail.data.algorithm || '—'}</Typography>
            <Typography variant="body2" color="text.secondary">
              Primary version: v{detail.data.primaryVersion || '—'} ({detail.data.primaryState || '—'})
            </Typography>
            {detail.data.rotationPeriod && (
              <Typography variant="body2" color="text.secondary">
                Rotation: every {detail.data.rotationPeriod}
              </Typography>
            )}
            <Typography variant="body2" color="text.secondary">
              Created: {detail.data.createTime || '—'}
            </Typography>
          </Stack>
        </Box>
      )}

      {(toggle.isError || destroy.isError || promote.isError || create.isError) && (
        <Alert severity="error" sx={{ mb: 2 }}>
          The version operation failed.
        </Alert>
      )}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Version</TableCell>
              <TableCell>State</TableCell>
              <TableCell>Created</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {versions.isLoading && (
              <TableRow>
                <TableCell colSpan={4} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!versions.isLoading && (versions.data?.cryptoKeyVersions.length ?? 0) === 0 && (
              <TableRow>
                <TableCell colSpan={4} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No versions.
                </TableCell>
              </TableRow>
            )}
            {versions.data?.cryptoKeyVersions.map((version) => {
              const isPrimary = version.versionId === detail.data?.primaryVersion
              const mutable = version.state === 'ENABLED' || version.state === 'DISABLED'
              return (
                <TableRow key={version.versionId} hover>
                  <TableCell>
                    v{version.versionId} {isPrimary && <Chip size="small" label="primary" sx={{ ml: 1 }} />}
                  </TableCell>
                  <TableCell>
                    <Chip size="small" label={version.state || '—'} color={versionColor(version.state)} />
                  </TableCell>
                  <TableCell>{version.createTime || '—'}</TableCell>
                  <TableCell align="right">
                    {mutable && (
                      <Button size="small" onClick={() => toggle.mutate(version)}>
                        {version.state === 'ENABLED' ? 'Disable' : 'Enable'}
                      </Button>
                    )}
                    {mutable && version.state === 'ENABLED' && !isPrimary && (
                      <Button size="small" onClick={() => promote.mutate(version)}>
                        Set primary
                      </Button>
                    )}
                    {mutable && (
                      <Tooltip title="Schedule destruction (30 days)">
                        <span>
                          <IconButton
                            size="small"
                            disabled={destroy.isPending}
                            onClick={() => destroy.mutate(version)}
                            aria-label={`Destroy version ${version.versionId}`}
                          >
                            <DeleteOutlineIcon fontSize="small" />
                          </IconButton>
                        </span>
                      </Tooltip>
                    )}
                  </TableCell>
                </TableRow>
              )
            })}
          </TableBody>
        </Table>
      </TableContainer>

      <Box sx={{ mt: 4 }}>
        <IamPolicyPanel
          title="Crypto key IAM policy"
          queryKey={['gcp', 'kms', 'cryptoKey', location, keyRing, key, 'iam']}
          load={() => getCryptoKeyIam(location, keyRing, key)}
          save={(policy) => putCryptoKeyIam(location, keyRing, key, policy)}
          defaultRole="roles/cloudkms.cryptoKeyEncrypterDecrypter"
        />
      </Box>
    </Box>
  )
}
