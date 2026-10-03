import { Box, ContentLayout, Header } from '@cloudscape-design/components'

export function GCPRoutes() {
  return (
    <ContentLayout header={<Header variant="h1">GCP cloud emulation</Header>}>
      <Box color="text-body-secondary">
        Coming soon. Cloud Storage and Pub/Sub will appear here once the GCP provider ships
        non-501 responses.
      </Box>
    </ContentLayout>
  )
}
