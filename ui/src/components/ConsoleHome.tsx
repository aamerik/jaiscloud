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
import { categoryOrder, navTree, serviceCategory, type NavSection } from './nav'

function toSection(id: string): NavSection | undefined {
  return navTree.find((section) => section.id === id)
}

/**
 * Console Home landing page: recently visited services plus all services
 * grouped by AWS category, mirroring the AWS Console home.
 */
export function ConsoleHome() {
  const navigate = useNavigate()

  let recent: NavSection[] = []
  try {
    const ids: string[] = JSON.parse(localStorage.getItem('jaiscloud-recent') ?? '[]')
    recent = ids.map(toSection).filter((section): section is NavSection => section != null)
  } catch {
    recent = []
  }

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
              header: (item) => (
                <Link
                  href={`/ui${item.rootPath}`}
                  onFollow={(event) => {
                    event.preventDefault()
                    navigate(item.rootPath)
                  }}
                >
                  {item.label}
                </Link>
              ),
              sections: [
                {
                  id: 'category',
                  header: 'Category',
                  content: (item) => serviceCategory[item.id] ?? '—',
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

        {categoryOrder.map((title) => {
          const services = navTree.filter((section) => serviceCategory[section.id] === title)
          if (services.length === 0) return null
          return (
            <Container key={title} header={<Header variant="h2">{title}</Header>}>
              <div
                style={{
                  display: 'grid',
                  gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))',
                  gap: '0.75rem',
                }}
              >
                {services.map((section) => (
                  <Link
                    key={section.id}
                    href={`/ui${section.rootPath}`}
                    onFollow={(event) => {
                      event.preventDefault()
                      navigate(section.rootPath)
                    }}
                  >
                    {section.label}
                  </Link>
                ))}
              </div>
            </Container>
          )
        })}
      </SpaceBetween>
    </ContentLayout>
  )
}
