import { useId } from 'react'
import { Link } from '@tanstack/react-router'
import { useTranslation } from 'react-i18next'
import { cn } from '@/lib/cn'
import { navigationItemsForSection, type AppShellNavItem } from './app-shell-navigation'

const linkClass =
  'group flex min-h-10 items-center gap-3 rounded-r-lg border-l-2 border-transparent px-3 py-2 text-ui text-muted-foreground transition-[background-color,color,transform] hover:bg-sidebar-hover hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring active:scale-[0.99] data-[status=active]:border-sidebar-active data-[status=active]:bg-ember-surface data-[status=active]:font-semibold data-[status=active]:text-foreground'

function NavigationEntry({
  item,
  projectId,
  onNavigate,
}: {
  item: AppShellNavItem
  projectId?: string
  onNavigate?: () => void
}) {
  const { t } = useTranslation()
  const Icon = item.icon
  const description =
    item.key === 'mainChat'
      ? t('appShell.navigation.mainAgentDescription')
      : item.key === 'agentWorkspace'
        ? t('appShell.navigation.projectAgentDescription')
        : null
  const content = (
    <>
      <Icon size={18} className="shrink-0" aria-hidden />
      <span className="min-w-0">
        <span className="block truncate">{t(`appShell.navigation.${item.key}`)}</span>
        {description ? (
          <span className="block truncate text-xs font-normal text-muted-foreground">
            {description}
          </span>
        ) : null}
      </span>
    </>
  )

  if (item.section === 'project') {
    if (!projectId) {
      return (
        <span
          aria-disabled="true"
          title="Select or create a Project to open this destination"
          className={cn(linkClass, 'cursor-not-allowed opacity-50')}
        >
          {content}
        </span>
      )
    }
    return (
      <Link
        to={item.to}
        params={{ projectId }}
        activeOptions={{ exact: true }}
        className={linkClass}
        onClick={onNavigate}
      >
        {content}
      </Link>
    )
  }

  return (
    <Link to={item.to} activeOptions={{ exact: true }} className={linkClass} onClick={onNavigate}>
      {content}
    </Link>
  )
}

export function AppShellSidebar({
  projectId,
  isAdmin,
  onNavigate,
}: {
  projectId?: string
  isAdmin: boolean
  onNavigate?: () => void
}) {
  const { t } = useTranslation()
  const accountId = useId()
  const projectSectionId = useId()
  const workspaceId = useId()
  const workspaceItems = navigationItemsForSection('workspace').filter(
    (item) =>
      isAdmin ||
      (item.key !== 'daemons' && item.key !== 'operations' && item.key !== 'forgeSettings'),
  )
  const sections = [
    {
      id: accountId,
      label: t('appShell.navigation.account'),
      items: navigationItemsForSection('account'),
    },
    {
      id: projectSectionId,
      label: t('appShell.navigation.project'),
      items: navigationItemsForSection('project'),
    },
    { id: workspaceId, label: t('appShell.navigation.workspace'), items: workspaceItems },
  ]

  return (
    <nav
      aria-label="Main navigation"
      className="flex h-full min-h-0 flex-col overflow-y-auto px-3 py-5"
    >
      {sections.map((section) => (
        <section key={section.id} aria-labelledby={section.id} className="mb-6 last:mb-0">
          <h2
            id={section.id}
            className="mb-2 px-3 font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground"
          >
            {section.label}
          </h2>
          <div className="space-y-1">
            {section.items.map((item) => (
              <NavigationEntry
                key={item.key}
                item={item}
                projectId={projectId}
                onNavigate={onNavigate}
              />
            ))}
          </div>
        </section>
      ))}
    </nav>
  )
}
