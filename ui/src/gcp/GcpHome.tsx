import { Alert, Box, Card, CardActionArea, CardContent, Chip, Typography } from '@mui/material'
import CloudOutlinedIcon from '@mui/icons-material/CloudOutlined'
import StorageOutlinedIcon from '@mui/icons-material/StorageOutlined'
import CampaignOutlinedIcon from '@mui/icons-material/CampaignOutlined'
import ComputerOutlinedIcon from '@mui/icons-material/ComputerOutlined'
import { Link as RouterLink } from 'react-router-dom'
import { useAccount } from '../context/AccountContext'
import { useMeta } from '../hooks/useMeta'
import { useServices } from '../hooks/useServices'
import { tierLabel } from '../lib/tier'
import type { ServiceDescriptor } from '../api/services'

function ServiceIcon({ service }: { service: ServiceDescriptor }) {
  if (service.id === 'storage') return <StorageOutlinedIcon />
  if (service.id === 'pubsub') return <CampaignOutlinedIcon />
  if (service.id === 'compute') return <ComputerOutlinedIcon />
  return <CloudOutlinedIcon />
}

/** Google Cloud console overview: emulator identity and the wired services. */
export function GcpHome() {
  const { data: meta } = useMeta()
  const { accountId } = useAccount()
  const { data: servicesData } = useServices()
  const services = servicesData?.services ?? []

  return (
    <Box>
      <Typography variant="h4" sx={{ fontWeight: 400 }}>
        JaisCloud
      </Typography>
      <Typography variant="body1" color="text.secondary" sx={{ mt: 0.5 }}>
        Local GCP emulator · project {accountId || meta?.accountId || '—'} ·{' '}
        {meta?.region ?? 'global'}
      </Typography>

      <Typography variant="h6" sx={{ mt: 4, mb: 1.5 }}>
        Services
      </Typography>

      {services.length === 0 ? (
        <Alert severity="info">
          No service UIs are wired into this build yet. Use the gcloud CLI or the GCP SDKs
          against the emulator endpoints.
        </Alert>
      ) : (
        <Box
          sx={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, minmax(260px, 1fr))',
            gap: 2,
          }}
        >
          {services.map((service) => {
            const tier = tierLabel(service)
            return (
              <Card key={service.id} variant="outlined">
                <CardActionArea
                  component={RouterLink}
                  to={service.rootPath}
                  sx={{ height: '100%' }}
                >
                  <CardContent>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 1 }}>
                      <Box
                        sx={{
                          width: 40,
                          height: 40,
                          borderRadius: '50%',
                          bgcolor: 'primary.main',
                          color: '#fff',
                          display: 'flex',
                          alignItems: 'center',
                          justifyContent: 'center',
                        }}
                      >
                        <ServiceIcon service={service} />
                      </Box>
                      <Box>
                        <Typography variant="subtitle1">{service.label}</Typography>
                        <Typography variant="body2" color="text.secondary">
                          {service.category}
                        </Typography>
                      </Box>
                    </Box>
                    {tier && <Chip size="small" label={tier} variant="outlined" />}
                  </CardContent>
                </CardActionArea>
              </Card>
            )
          })}
        </Box>
      )}
    </Box>
  )
}
