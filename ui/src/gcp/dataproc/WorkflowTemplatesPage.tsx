import { useQuery } from '@tanstack/react-query'
import {
  Alert,
  Box,
  CircularProgress,
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
import { Link as RouterLink } from 'react-router-dom'
import { listWorkflowTemplates } from '../../api/gcp/dataproc'
import { useAccount } from '../../context/AccountContext'
import { shortDate } from './util'

/** Dataproc workflow templates across every region. */
export function WorkflowTemplatesPage() {
  const { accountId } = useAccount()

  const templates = useQuery({
    queryKey: ['gcp', 'dataproc', 'workflow-templates', accountId],
    queryFn: listWorkflowTemplates,
  })

  return (
    <Box>
      <Stack sx={{ mb: 2 }}>
        <Typography variant="h5">Dataproc</Typography>
        <Typography variant="body2" color="text.secondary">
          Workflow templates · project {accountId || '—'}
        </Typography>
      </Stack>

      {templates.isError && <Alert severity="error">Failed to load workflow templates.</Alert>}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Template</TableCell>
              <TableCell>Region</TableCell>
              <TableCell>Version</TableCell>
              <TableCell>Created</TableCell>
              <TableCell>Updated</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {templates.isLoading && (
              <TableRow>
                <TableCell colSpan={5} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!templates.isLoading && (templates.data?.templates.length ?? 0) === 0 && (
              <TableRow>
                <TableCell colSpan={5} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No workflow templates in this project.
                </TableCell>
              </TableRow>
            )}
            {templates.data?.templates.map((template) => (
              <TableRow key={`${template.region}/${template.id}`} hover>
                <TableCell>
                  <Link
                    component={RouterLink}
                    to={`/gcp/dataproc/workflow-templates/${encodeURIComponent(template.region)}/${encodeURIComponent(template.id)}`}
                  >
                    {template.id}
                  </Link>
                </TableCell>
                <TableCell>{template.region || '—'}</TableCell>
                <TableCell>{template.version || '—'}</TableCell>
                <TableCell>{shortDate(template.createTime)}</TableCell>
                <TableCell>{shortDate(template.updateTime)}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  )
}
