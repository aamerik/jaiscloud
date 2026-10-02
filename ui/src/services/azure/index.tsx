import { Box, ContentLayout, Header } from '@cloudscape-design/components'

export function AzureRoutes() {
  return (
    <ContentLayout header={<Header variant="h1">Azure cloud emulation</Header>}>
      <Box color="text-body-secondary">
        Coming soon. Blob Storage and Service Bus will appear here once the Azure provider ships
        non-501 responses.
      </Box>
    </ContentLayout>
  )
}
