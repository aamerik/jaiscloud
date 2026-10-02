import {
  Container,
  ContentLayout,
  Header,
  Link,
  SpaceBetween,
} from '@cloudscape-design/components'
import { useNavigate } from 'react-router-dom'
import { categoryOrder, navTree, serviceCategory } from './nav'

/**
 * Console Home landing page: services grouped by AWS category, mirroring the
 * AWS Console home. Reached at the app root instead of dropping straight into
 * an arbitrary service.
 */
export function ConsoleHome() {
  const navigate = useNavigate()

  return (
    <ContentLayout
      header={
        <Header variant="h1" description="JaisCloud local AWS emulator">
          Console Home
        </Header>
      }
    >
      <SpaceBetween size="l">
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
