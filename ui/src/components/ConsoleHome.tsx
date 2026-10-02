import {
  Box,
  Cards,
  Container,
  ContentLayout,
  Header,
  Link,
  SpaceBetween,
} from '@cloudscape-design/components'
import { useNavigate } from 'react-router-dom'
import { useServices } from '../hooks/useServices'
import { groupByCategory, type NavSection } from './nav'

function recentIds(): string[] {
  try {
    return JSON.parse(localStorage.getItem('jaiscloud-recent') ?? '[]') as string[]
  } catch {
    return []
  }
}

/**
 * Console Home landing page: recently visited services plus all supported
 * services grouped by AWS category. The service list is provided by the
 * backend, so only services this build supports are shown.
 */
export function ConsoleHome() {
  const navigate = useNavigate()
  const { data } = useServices()
  const services = data?.services ?? []

  const recent = recentIds()
    .map((id) => services.find((service) => service.id === id))
    .filter((service): service is NavSection => service != null)

  const groups = groupByCategory(services)

  const renderLink = (service: NavSection) => (
    <Link
      href={`/ui${service.rootPath}`}
      onFollow={(event) => {
        event.preventDefault()
        navigate(service.rootPath)
      }}
    >
      {service.label}
    </Link>
  )

  return (
    <ContentLayout
      header={
        <Header variant="h1" description="JaisCloud local AWS emulator">
          Console Home
        </Header>
      }
    >
      <SpaceBetween size="l">
        {recent.length > 0 && (
          <Cards
            items={recent}
            cardDefinition={{
              header: (item) => renderLink(item),
              sections: [
                {
                  id: 'category',
                  header: 'Category',
                  content: (item) => item.category,
                },
              ],
            }}
            cardsPerRow={[
              { cards: 1 },
              { minWidth: 300, cards: 2 },
              { minWidth: 600, cards: 3 },
              { minWidth: 900, cards: 4 },
            ]}
            header={<Header variant="h2">Recently visited</Header>}
            empty={<Box color="inherit">Services you open appear here.</Box>}
          />
        )}

        {groups.map((group) => (
          <Container key={group.category} header={<Header variant="h2">{group.category}</Header>}>
            <div
              style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))',
                gap: '0.75rem',
              }}
            >
              {group.services.map((service) => (
                <span key={service.id}>{renderLink(service)}</span>
              ))}
            </div>
          </Container>
        ))}
      </SpaceBetween>
    </ContentLayout>
  )
}
