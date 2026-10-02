import { useMemo, useState } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
import {
  AppLayout,
  BreadcrumbGroup,
  Input,
  SideNavigation,
  TopNavigation,
} from '@cloudscape-design/components'
import type {
  BreadcrumbGroupProps,
  SideNavigationProps,
  TopNavigationProps,
} from '@cloudscape-design/components'
import { AccountProvider, useAccount, useAccounts } from '../context/AccountContext'
import { useMeta } from '../hooks/useMeta'
import { useEventStream } from '../hooks/useEventStream'
import { categoryOrder, navTree, serviceCategory, type NavSection } from './nav'

/** Router basename; links must include it so they also work without JS. */
const BASE = '/ui'

const href = (path: string) => `${BASE}${path}`

interface Props {
  children: React.ReactNode
}

type NavItem = SideNavigationProps.Link | SideNavigationProps.ExpandableLinkGroup

function serviceItem(section: NavSection, expand: boolean): NavItem {
  if (section.children.length > 1) {
    return {
      type: 'expandable-link-group',
      text: section.label,
      href: href(section.rootPath),
      defaultExpanded: expand,
      items: section.children.map((child) => ({
        type: 'link',
        text: child.label,
        href: href(child.path),
      })),
    }
  }
  return { type: 'link', text: section.label, href: href(section.rootPath) }
}

function useConsoleNav() {
  const { pathname } = useLocation()
  const navigate = useNavigate()

  const onFollow = (event: { preventDefault: () => void; detail: { href: string } }) => {
    event.preventDefault()
    navigate(event.detail.href.replace(BASE, '') || '/')
  }

  const breadcrumbItems = useMemo<BreadcrumbGroupProps.Item[]>(() => {
    const items: BreadcrumbGroupProps.Item[] = [
      { text: 'JaisCloud', href: href('/') },
    ]
    const parts = pathname.split('/').filter(Boolean)
    if (parts[0] === 'aws' && parts[1]) {
      const section = navTree.find((s) => s.basePath === `/aws/${parts[1]}`)
      if (section) items.push({ text: section.label, href: href(section.rootPath) })
    } else if (parts[0] === 'admin') {
      items.push({ text: 'Admin', href: href('/admin') })
    }
    return items
  }, [pathname])

  return { breadcrumbItems, onFollow }
}

function Shell({ children }: Props) {
  const [navOpen, setNavOpen] = useState(true)
  const [search, setSearch] = useState('')
  const navigate = useNavigate()
  const { pathname } = useLocation()
  const { data: meta } = useMeta()
  const { accountId, setAccountId } = useAccount()
  const { data: accountsData, refetch: refetchAccounts } = useAccounts()
  const { connected } = useEventStream()
  const { breadcrumbItems, onFollow } = useConsoleNav()

  const accounts = useMemo(
    () => accountsData?.accounts ?? (accountId ? [accountId] : []),
    [accountsData, accountId],
  )

  const sideItems = useMemo<SideNavigationProps.Item[]>(() => {
    const query = search.trim().toLowerCase()
    const groups = categoryOrder
      .map((title) => {
        const items = navTree
          .filter((section) => serviceCategory[section.id] === title)
          .filter((section) => !query || section.label.toLowerCase().includes(query))
          .map((section) => serviceItem(section, query.length > 0 || pathname.startsWith(section.basePath)))
        return { type: 'section-group', title, items } as SideNavigationProps.SectionGroup
      })
      .filter((group) => group.items.length > 0)

    return [
      { type: 'link', text: 'Console home', href: href('/') },
      { type: 'divider' },
      ...groups,
      { type: 'divider' },
      { type: 'link', text: 'Admin', href: href('/admin') },
    ]
  }, [search, pathname])

  const utilities = useMemo<TopNavigationProps.Utility[]>(() => {
    const items: TopNavigationProps.Utility[] = [
      {
        type: 'button',
        text: meta?.region ?? '—',
        iconName: 'globe',
        disableUtilityCollapse: true,
      },
      {
        type: 'menu-dropdown',
        text: accountId || 'Account',
        iconName: 'user-profile',
        items: accounts.map((account) => ({ id: account, text: account })),
        onItemClick: (event) => {
          if (event.detail.id) setAccountId(event.detail.id)
        },
      },
      {
        type: 'button',
        iconName: 'refresh',
        ariaLabel: 'Refresh account list',
        onClick: () => void refetchAccounts(),
      },
      {
        type: 'button',
        text: connected ? 'Live' : 'Polling',
        iconName: connected ? 'status-positive' : 'status-pending',
        disableUtilityCollapse: true,
      },
    ]

    const version = [meta?.version ? `v${meta.version}` : '', meta?.mode ?? '']
      .filter(Boolean)
      .join(' · ')
    if (version) {
      items.push({ type: 'button', text: version, disableUtilityCollapse: true })
    }
    return items
  }, [meta, accountId, accounts, connected, refetchAccounts, setAccountId])

  return (
    <>
      <div className="console-topnav">
        <TopNavigation
          identity={{
            href: href('/'),
            title: 'JaisCloud',
            onFollow: (event) => {
              event.preventDefault()
              navigate('/')
            },
          }}
          utilities={utilities}
        />
      </div>
      <AppLayout
        navigation={
          <div className="console-sidenav">
            <div className="console-sidenav__search">
              <Input
                type="search"
                value={search}
                onChange={({ detail }) => setSearch(detail.value)}
                placeholder="Find services"
                ariaLabel="Find services"
              />
            </div>
            <SideNavigation
              key={search}
              header={{ href: href('/'), text: 'All services' }}
              items={sideItems}
              onFollow={onFollow}
              activeHref={href(pathname)}
            />
          </div>
        }
        navigationOpen={navOpen}
        onNavigationChange={({ detail }) => setNavOpen(detail.open)}
        breadcrumbs={
          <BreadcrumbGroup items={breadcrumbItems} onFollow={onFollow} ariaLabel="Breadcrumbs" />
        }
        content={children}
        toolsHide
        contentType="default"
      />
    </>
  )
}

export function Layout({ children }: Props) {
  return (
    <AccountProvider>
      <Shell>{children}</Shell>
    </AccountProvider>
  )
}
