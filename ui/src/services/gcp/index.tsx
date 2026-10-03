import {
  Box,
  ContentLayout,
  Header,
  Link,
  SpaceBetween,
  Table,
  TextContent,
} from '@cloudscape-design/components'
import { ConsoleHome } from '../../components/ConsoleHome'
import { useMeta } from '../../hooks/useMeta'
import { useServices } from '../../hooks/useServices'

export function GCPRoutes() {
  const { data: meta } = useMeta()
  const { data: servicesData } = useServices()
  const services = servicesData?.services ?? []

  // Once service backends ship, the generic Console Home is the GCP landing;
  // until then keep the shell-only placeholder.
  if (services.length > 0) return <ConsoleHome />

  return (
    <ContentLayout
      header={
        <Header
          variant="h1"
          description="Local Google Cloud emulator — GCP wire-protocol compatible"
        >
          Google Cloud
        </Header>
      }
    >
      <SpaceBetween size="l">
        <TextContent>
          <p>
            The JaisCloud GCP emulator is running and reachable on its wire ports. The console shell
            is live; service pages are added as each GCP service backend is wired into the UI.
          </p>
        </TextContent>
        <Table
          variant="container"
          columnDefinitions={[
            { id: 'identity', header: 'Cloud', cell: () => 'gcp' },
            { id: 'project', header: 'Project', cell: () => meta?.accountId || '—' },
            { id: 'region', header: 'Region', cell: () => meta?.region || '—' },
            { id: 'mode', header: 'Mode', cell: () => meta?.mode || '—' },
            { id: 'version', header: 'Version', cell: () => (meta?.version ? `v${meta.version}` : '—') },
          ]}
          items={[{ id: 'gcp' }]}
          empty="No cloud identity reported"
        />
        <Box color="text-body-secondary">
          Services such as Cloud Storage and Pub/Sub will appear in the navigation once their UI
          backends ship. Until then, use the <code>gcloud</code> CLI or the GCP SDKs against the
          emulator endpoints.
        </Box>
        <Link external href="https://cloud.google.com/docs">
          Google Cloud documentation
        </Link>
      </SpaceBetween>
    </ContentLayout>
  )
}
