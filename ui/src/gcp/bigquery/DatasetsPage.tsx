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
import { deleteDataset, listDatasets, type BigQueryDataset } from '../../api/gcp/bigquery'
import { useAccount } from '../../context/AccountContext'
import { CreateDatasetDialog } from './CreateDatasetDialog'

/** BigQuery datasets in the project, with create and delete. */
export function DatasetsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'bigquery'] })

  const datasets = useQuery({
    queryKey: ['gcp', 'bigquery', 'datasets', accountId],
    queryFn: listDatasets,
  })

  const remove = useMutation({
    mutationFn: (dataset: BigQueryDataset) => deleteDataset(dataset.datasetId),
    onSuccess: invalidate,
  })

  return (
    <Box>
      <Stack
        direction="row"
        sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <Box>
          <Typography variant="h5">BigQuery</Typography>
          <Typography variant="body2" color="text.secondary">
            Datasets · project {accountId || '—'}
          </Typography>
        </Box>
        <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
          Create dataset
        </Button>
      </Stack>

      {datasets.isError && <Alert severity="error">Failed to load datasets.</Alert>}
      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Dataset ID</TableCell>
              <TableCell>Location</TableCell>
              <TableCell>Friendly name</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {datasets.isLoading && (
              <TableRow>
                <TableCell colSpan={4} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!datasets.isLoading && (datasets.data?.datasets.length ?? 0) === 0 && (
              <TableRow>
                <TableCell colSpan={4} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No datasets in this project.
                </TableCell>
              </TableRow>
            )}
            {datasets.data?.datasets.map((dataset) => (
              <TableRow key={dataset.datasetId} hover>
                <TableCell>
                  <Link
                    component={RouterLink}
                    to={`/gcp/bigquery/datasets/${encodeURIComponent(dataset.datasetId)}`}
                  >
                    {dataset.datasetId}
                  </Link>
                </TableCell>
                <TableCell>{dataset.location || '—'}</TableCell>
                <TableCell>{dataset.friendlyName || '—'}</TableCell>
                <TableCell align="right">
                  <Tooltip title="Delete dataset">
                    <span>
                      <IconButton
                        size="small"
                        disabled={remove.isPending}
                        onClick={() => remove.mutate(dataset)}
                        aria-label={`Delete ${dataset.datasetId}`}
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

      <CreateDatasetDialog open={createOpen} onClose={() => setCreateOpen(false)} />
    </Box>
  )
}
