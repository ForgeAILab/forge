import { useEffect, useState } from 'react'
import type { Icon } from '@phosphor-icons/react'
import { Database, HardDrive, Robot, Warning } from '@phosphor-icons/react'
import { Link } from '@tanstack/react-router'
import { toast } from 'sonner'
import { useSettingsQuery, useUpdateSettings } from '@/api/hooks'
import { SettingsSection } from '@/components/settings/SettingsSection'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Skeleton } from '@/components/ui/skeleton'
import { Switch } from '@/components/ui/switch'
import { getApiErrorMessage } from '@/lib/api-error'
import { cn } from '@/lib/cn'
import type { ForgeSettingResponse } from '@/types/generated'

export type ForgeSettingsTab = 'server' | 'agent' | 'paths'

const TABS: Array<{ id: ForgeSettingsTab; label: string; icon: Icon }> = [
  { id: 'server', label: 'Server', icon: Database },
  { id: 'agent', label: 'Agent', icon: Robot },
  { id: 'paths', label: 'Paths', icon: HardDrive },
]

export function isForgeSettingsTab(value: string | undefined): value is ForgeSettingsTab {
  return TABS.some((tab) => tab.id === value)
}

function RestartBadge({ setting }: { setting: ForgeSettingResponse | undefined }) {
  if (!setting?.restart_required) return null
  return (
    <span className="ml-1.5 inline-flex items-center rounded-full bg-amber-100 px-1.5 py-0.5 font-mono text-[10px] font-medium text-amber-700 dark:bg-amber-900/30 dark:text-amber-400">
      restart required
    </span>
  )
}

function EffectiveHint({ setting }: { setting: ForgeSettingResponse | undefined }) {
  if (!setting?.restart_required) return null
  const val = String(setting.effective_value ?? '')
  if (!val) return null
  return <p className="mt-1 font-mono text-[11px] text-muted-foreground">Running: {val}</p>
}

export function ForgeSettingsPage({ initialTab = 'server' }: { initialTab?: ForgeSettingsTab }) {
  const settingsQuery = useSettingsQuery()
  const updateSettings = useUpdateSettings()

  const [bind, setBind] = useState('')
  const [mcpEnabled, setMcpEnabled] = useState(true)
  const [serverRunCap, setServerRunCap] = useState('')
  const [buildJobs, setBuildJobs] = useState('')
  const [runNice, setRunNice] = useState('10')
  const [usageIndexBudget, setUsageIndexBudget] = useState('')
  const [maxConcurrent, setMaxConcurrent] = useState('')
  const [heartbeatInterval, setHeartbeatInterval] = useState('')
  const [maxMissedHeartbeats, setMaxMissedHeartbeats] = useState('')
  const [dataDir, setDataDir] = useState('')

  const settings = settingsQuery.data?.settings ?? []
  const getSetting = (key: string) => settings.find((s) => s.key === key)

  useEffect(() => {
    if (!settings.length) return
    const get = (key: string) => getSetting(key)?.value
    setBind(String(get('server.bind') ?? ''))
    setMcpEnabled(get('server.mcp_enabled') !== false)
    setServerRunCap(String(get('server.max_concurrent_runs') ?? ''))
    setBuildJobs(String(get('server.build_jobs_per_run') ?? ''))
    setRunNice(String(get('server.run_nice') ?? 10))
    setUsageIndexBudget(String(get('server.usage_index_budget_mb') ?? ''))
    setMaxConcurrent(String(get('agent.max_concurrent_tasks') ?? ''))
    setHeartbeatInterval(String(get('agent.heartbeat_interval_seconds') ?? ''))
    setMaxMissedHeartbeats(String(get('agent.max_missed_heartbeats') ?? ''))
    setDataDir(String(get('forge.data_dir') ?? ''))
  }, [settings]) // eslint-disable-line react-hooks/exhaustive-deps

  const isSaving = updateSettings.isPending

  function saveServer() {
    const budget = Number(usageIndexBudget)
    if (
      usageIndexBudget.trim() &&
      (!Number.isInteger(budget) || budget < 0 || budget > 4294967295)
    ) {
      toast.error('Usage index budget must be a non-negative integer in MiB')
      return
    }
    const cap = Number(serverRunCap)
    if (serverRunCap.trim() && (!Number.isInteger(cap) || cap < 0 || cap > 4294967295)) {
      toast.error('Max concurrent runs must be a non-negative integer')
      return
    }
    const jobs = Number(buildJobs)
    const nice = Number(runNice)
    if (buildJobs.trim() && (!Number.isInteger(jobs) || jobs < 0 || jobs > 4294967295)) {
      toast.error('Build jobs per run must be a non-negative integer')
      return
    }
    if (!runNice.trim() || !Number.isInteger(nice) || nice < 0 || nice > 19) {
      toast.error('Run niceness must be an integer from 0 to 19')
      return
    }
    if (!bind.trim()) {
      toast.error('Bind address is required')
      return
    }
    updateSettings.mutate(
      {
        server: {
          bind: bind.trim(),
          mcp_enabled: mcpEnabled,
          max_concurrent_runs: serverRunCap.trim() ? cap : null,
          build_jobs_per_run: buildJobs.trim() ? jobs : null,
          run_nice: nice,
          usage_index_budget_mb: usageIndexBudget.trim() ? budget : null,
        },
      },
      {
        onSuccess: () => toast.success('Server settings saved'),
        onError: (err) => toast.error(getApiErrorMessage(err, 'Failed to save server settings')),
      },
    )
  }

  function saveAgent() {
    const concurrent = Number(maxConcurrent)
    const heartbeat = Number(heartbeatInterval)
    const missed = Number(maxMissedHeartbeats)
    if (maxConcurrent && (!Number.isInteger(concurrent) || concurrent < 1)) {
      toast.error('Max concurrent tasks must be a positive integer')
      return
    }
    if (heartbeatInterval && (!Number.isInteger(heartbeat) || heartbeat < 1)) {
      toast.error('Heartbeat interval must be a positive integer')
      return
    }
    if (maxMissedHeartbeats && (!Number.isInteger(missed) || missed < 1)) {
      toast.error('Max missed heartbeats must be a positive integer')
      return
    }
    updateSettings.mutate(
      {
        agent: {
          max_concurrent_tasks: maxConcurrent ? concurrent : null,
          heartbeat_interval_seconds: heartbeatInterval ? heartbeat : null,
          max_missed_heartbeats: maxMissedHeartbeats ? missed : null,
        },
      },
      {
        onSuccess: () => toast.success('Agent settings saved'),
        onError: (err) => toast.error(getApiErrorMessage(err, 'Failed to save agent settings')),
      },
    )
  }

  function savePaths() {
    if (!dataDir.trim()) {
      toast.error('Data directory is required')
      return
    }
    updateSettings.mutate(
      { forge: { data_dir: dataDir.trim() } },
      {
        onSuccess: () => toast.success('Path settings saved'),
        onError: (err) => toast.error(getApiErrorMessage(err, 'Failed to save path settings')),
      },
    )
  }

  const isLoading = settingsQuery.isLoading

  return (
    <div className="flex h-[calc(100vh-7rem)] gap-0 overflow-hidden rounded-xl border border-border-subtle bg-card shadow-card">
      {/* Settings sidebar */}
      <aside className="flex w-56 shrink-0 flex-col border-r bg-background">
        <div className="border-b px-4 py-3">
          <p className="font-mono text-micro font-semibold uppercase tracking-[1px] text-muted-foreground">
            System Settings
          </p>
          <p className="mt-0.5 text-sm font-semibold text-foreground">Forge</p>
          {settingsQuery.data?.config_path && (
            <p
              className="mt-0.5 truncate font-mono text-[11px] text-muted-foreground"
              title={settingsQuery.data.config_path}
            >
              {settingsQuery.data.config_path}
            </p>
          )}
        </div>
        <nav className="flex flex-1 flex-col gap-0.5 p-2">
          {TABS.map((t) => {
            const TabIcon = t.icon
            return (
              <Link
                key={t.id}
                to={t.id === 'server' ? '/settings' : '/settings/$tab'}
                params={{ tab: t.id }}
                className={cn(
                  'relative flex w-full cursor-pointer items-center gap-2.5 rounded-lg px-2.5 py-[7px] text-left text-[13px] leading-none font-medium transition-colors',
                  initialTab === t.id
                    ? 'bg-[var(--ember-surface)] text-sidebar-active-foreground before:absolute before:left-0 before:top-1/2 before:-translate-y-1/2 before:h-4 before:w-[3px] before:rounded-r-full before:bg-primary'
                    : 'text-sidebar-foreground hover:bg-accent/50 hover:text-foreground',
                )}
              >
                <TabIcon size={16} />
                {t.label}
              </Link>
            )
          })}
        </nav>
      </aside>

      {/* Content area */}
      <div className="flex-1 overflow-y-auto px-8 py-6">
        <div className="max-w-[760px]">
          {settingsQuery.data?.restart_required && (
            <div className="mb-6 flex items-center gap-2.5 rounded-lg border border-amber-300 bg-amber-50 px-4 py-3 text-sm text-amber-800 dark:border-amber-700 dark:bg-amber-900/20 dark:text-amber-300">
              <Warning size={16} weight="fill" className="shrink-0" />
              <span>Some settings have changed and will take effect after a server restart.</span>
            </div>
          )}

          {isLoading ? (
            <div className="space-y-4">
              <Skeleton className="h-8 w-48" />
              <Skeleton className="h-10 w-full" />
              <Skeleton className="h-10 w-full" />
            </div>
          ) : (
            <>
              {initialTab === 'server' && (
                <ServerTab
                  bind={bind}
                  serverRunCap={serverRunCap}
                  usageIndexBudget={usageIndexBudget}
                  budgetSetting={getSetting('server.usage_index_budget_mb')}
                  onBudgetChange={setUsageIndexBudget}
                  runCapSetting={getSetting('server.max_concurrent_runs')}
                  buildJobs={buildJobs}
                  runNice={runNice}
                  buildJobsSetting={getSetting('server.build_jobs_per_run')}
                  runNiceSetting={getSetting('server.run_nice')}
                  coresSetting={getSetting('server.logical_cores')}
                  onBuildJobsChange={setBuildJobs}
                  onRunNiceChange={setRunNice}
                  onRunCapChange={setServerRunCap}
                  mcpEnabled={mcpEnabled}
                  isSaving={isSaving}
                  bindSetting={getSetting('server.bind')}
                  mcpSetting={getSetting('server.mcp_enabled')}
                  onBindChange={setBind}
                  onMcpEnabledChange={setMcpEnabled}
                  onSave={saveServer}
                />
              )}
              {initialTab === 'agent' && (
                <AgentTab
                  maxConcurrent={maxConcurrent}
                  heartbeatInterval={heartbeatInterval}
                  maxMissedHeartbeats={maxMissedHeartbeats}
                  isSaving={isSaving}
                  maxConcurrentSetting={getSetting('agent.max_concurrent_tasks')}
                  heartbeatIntervalSetting={getSetting('agent.heartbeat_interval_seconds')}
                  maxMissedHeartbeatsSetting={getSetting('agent.max_missed_heartbeats')}
                  onMaxConcurrentChange={setMaxConcurrent}
                  onHeartbeatIntervalChange={setHeartbeatInterval}
                  onMaxMissedHeartbeatsChange={setMaxMissedHeartbeats}
                  onSave={saveAgent}
                />
              )}
              {initialTab === 'paths' && (
                <PathsTab
                  dataDir={dataDir}
                  isSaving={isSaving}
                  dataDirSetting={getSetting('forge.data_dir')}
                  onDataDirChange={setDataDir}
                  onSave={savePaths}
                />
              )}
            </>
          )}
        </div>
      </div>
    </div>
  )
}

function ServerTab({
  buildJobs,
  runNice,
  buildJobsSetting,
  runNiceSetting,
  coresSetting,
  onBuildJobsChange,
  onRunNiceChange,
  usageIndexBudget,
  budgetSetting,
  onBudgetChange,
  serverRunCap,
  runCapSetting,
  onRunCapChange,
  bind,
  mcpEnabled,
  isSaving,
  bindSetting,
  mcpSetting,
  onBindChange,
  onMcpEnabledChange,
  onSave,
}: {
  usageIndexBudget: string
  budgetSetting: ForgeSettingResponse | undefined
  onBudgetChange: (v: string) => void
  serverRunCap: string
  buildJobs: string
  runNice: string
  buildJobsSetting: ForgeSettingResponse | undefined
  runNiceSetting: ForgeSettingResponse | undefined
  coresSetting: ForgeSettingResponse | undefined
  onBuildJobsChange: (v: string) => void
  onRunNiceChange: (v: string) => void
  runCapSetting: ForgeSettingResponse | undefined
  onRunCapChange: (v: string) => void
  bind: string
  mcpEnabled: boolean
  isSaving: boolean
  bindSetting: ForgeSettingResponse | undefined
  mcpSetting: ForgeSettingResponse | undefined
  onBindChange: (v: string) => void
  onMcpEnabledChange: (v: boolean) => void
  onSave: () => void
}) {
  return (
    <>
      <div className="mb-8">
        <h2 className="text-page font-semibold tracking-tight">Server</h2>
        <p className="mt-1 text-sm text-muted-foreground">
          HTTP server bind address and feature flags.
        </p>
      </div>
      <SettingsSection
        title="Max concurrent runs"
        description="Limits agent runs on the server host. Blank uses half the logical cores (at least 2); 0 is unlimited. Applies immediately. CLI or environment overrides take effect again after a restart."
      >
        <Label htmlFor="server-run-cap" className="sr-only">
          Max concurrent runs
        </Label>
        <Input
          id="server-run-cap"
          type="number"
          min={0}
          step={1}
          className="w-24"
          placeholder="Auto"
          value={serverRunCap}
          onChange={(e) => onRunCapChange(e.target.value)}
        />
        <p className="mt-1 text-xs text-muted-foreground">
          {runCapSetting?.value == null
            ? `Automatic (${runCapSetting?.effective_value ?? '—'})`
            : runCapSetting.effective_value == null
              ? 'Unlimited'
              : `In effect: ${runCapSetting.effective_value}`}
        </p>
      </SettingsSection>
      <SettingsSection
        title="Build jobs per run"
        description="Blank divides logical cores by the run cap (at least 1 job). An unlimited cap uses the automatic cap for this calculation. 0 disables the budget. Project and operator environment values take precedence. Applies to new processes immediately."
      >
        <Label htmlFor="server-build-jobs" className="sr-only">
          Build jobs per run
        </Label>
        <Input
          id="server-build-jobs"
          type="number"
          min={0}
          step={1}
          className="w-24"
          placeholder="Auto"
          value={buildJobs}
          onChange={(e) => onBuildJobsChange(e.target.value)}
        />
        <p className="mt-1 text-xs text-muted-foreground">
          {String(coresSetting?.effective_value ?? '—')} logical cores ·{' '}
          {buildJobsSetting?.effective_value === 0
            ? 'Budget disabled'
            : `${buildJobsSetting?.value == null ? 'Automatic' : 'In effect'}: ${buildJobsSetting?.effective_value ?? '—'} jobs per run`}
        </p>
      </SettingsSection>
      <SettingsSection
        title="Run niceness"
        description="Lower CPU priority for run processes on Unix so Forge stays responsive. 0 turns it off; 10 is the default; maximum 19. Children inherit the priority. Windows ignores this setting. Applies to new processes immediately."
      >
        <Label htmlFor="server-run-nice" className="sr-only">
          Run niceness
        </Label>
        <Input
          id="server-run-nice"
          type="number"
          min={0}
          max={19}
          step={1}
          className="w-24"
          value={runNice}
          onChange={(e) => onRunNiceChange(e.target.value)}
        />
        <p className="mt-1 text-xs text-muted-foreground">
          Configured increment: {String(runNiceSetting?.effective_value ?? '—')}
        </p>
      </SettingsSection>
      <SettingsSection
        title="Usage index memory budget"
        description="Blank uses 128 MiB; 0 uses memoized full reads. On the next usage read, an oversized index is discarded; increasing the budget retries a rebuild. CLI or environment overrides take effect again after a restart."
      >
        <Label htmlFor="usage-index-budget" className="sr-only">
          Usage index memory budget
        </Label>
        <div className="flex items-center gap-2">
          <Input
            id="usage-index-budget"
            type="number"
            min={0}
            max={4294967295}
            step={1}
            className="w-24"
            placeholder="128"
            value={usageIndexBudget}
            onChange={(e) => onBudgetChange(e.target.value)}
          />
          <span className="text-sm text-muted-foreground">MiB</span>
        </div>
        <p className="mt-1 text-xs text-muted-foreground">
          {budgetSetting?.effective_value === 0
            ? 'In effect: memoized full reads'
            : `In effect: ${budgetSetting?.effective_value ?? 128} MiB`}
        </p>
      </SettingsSection>
      <SettingsSection
        title={
          <span className="inline-flex items-center">
            Bind address
            <RestartBadge setting={bindSetting} />
          </span>
        }
        description="The address and port the server listens on."
      >
        <div className="space-y-1">
          <Label htmlFor="server-bind" className="sr-only">
            Bind address
          </Label>
          <Input
            id="server-bind"
            className="max-w-xs font-mono"
            placeholder="127.0.0.1:8080"
            value={bind}
            onChange={(e) => onBindChange(e.target.value)}
          />
          <EffectiveHint setting={bindSetting} />
        </div>
      </SettingsSection>
      <SettingsSection
        title={
          <span className="inline-flex items-center">
            MCP endpoint
            <RestartBadge setting={mcpSetting} />
          </span>
        }
        description="Enable or disable the Model Context Protocol endpoint at /mcp."
      >
        <div className="flex items-center gap-3">
          <Switch
            id="server-mcp"
            checked={mcpEnabled}
            onChange={(e) => onMcpEnabledChange((e.target as HTMLInputElement).checked)}
          />
          <Label htmlFor="server-mcp" className="cursor-pointer text-sm">
            {mcpEnabled ? 'Enabled' : 'Disabled'}
          </Label>
        </div>
        <EffectiveHint setting={mcpSetting} />
      </SettingsSection>
      <div className="flex justify-end py-6">
        <Button disabled={isSaving} onClick={onSave}>
          {isSaving ? 'Saving...' : 'Save'}
        </Button>
      </div>
    </>
  )
}

function AgentTab({
  maxConcurrent,
  heartbeatInterval,
  maxMissedHeartbeats,
  isSaving,
  maxConcurrentSetting,
  heartbeatIntervalSetting,
  maxMissedHeartbeatsSetting,
  onMaxConcurrentChange,
  onHeartbeatIntervalChange,
  onMaxMissedHeartbeatsChange,
  onSave,
}: {
  maxConcurrent: string
  heartbeatInterval: string
  maxMissedHeartbeats: string
  isSaving: boolean
  maxConcurrentSetting: ForgeSettingResponse | undefined
  heartbeatIntervalSetting: ForgeSettingResponse | undefined
  maxMissedHeartbeatsSetting: ForgeSettingResponse | undefined
  onMaxConcurrentChange: (v: string) => void
  onHeartbeatIntervalChange: (v: string) => void
  onMaxMissedHeartbeatsChange: (v: string) => void
  onSave: () => void
}) {
  return (
    <>
      <div className="mb-8">
        <h2 className="text-page font-semibold tracking-tight">Agent</h2>
        <p className="mt-1 text-sm text-muted-foreground">
          Concurrency limits and heartbeat supervision for agents.
        </p>
      </div>
      <SettingsSection
        title={
          <span className="inline-flex items-center">
            Max concurrent tasks
            <RestartBadge setting={maxConcurrentSetting} />
          </span>
        }
        description="Maximum number of tasks an agent can work on simultaneously."
      >
        <div className="space-y-1">
          <Label htmlFor="agent-max-concurrent" className="sr-only">
            Max concurrent tasks
          </Label>
          <Input
            id="agent-max-concurrent"
            type="number"
            min={1}
            className="w-24"
            value={maxConcurrent}
            onChange={(e) => onMaxConcurrentChange(e.target.value)}
          />
          <EffectiveHint setting={maxConcurrentSetting} />
        </div>
      </SettingsSection>
      <SettingsSection
        title={
          <span className="inline-flex items-center">
            Heartbeat interval
            <RestartBadge setting={heartbeatIntervalSetting} />
          </span>
        }
        description="How often agents must send a heartbeat, in seconds."
      >
        <div className="space-y-1">
          <div className="flex items-center gap-2">
            <Label htmlFor="agent-heartbeat" className="sr-only">
              Heartbeat interval
            </Label>
            <Input
              id="agent-heartbeat"
              type="number"
              min={1}
              className="w-24"
              value={heartbeatInterval}
              onChange={(e) => onHeartbeatIntervalChange(e.target.value)}
            />
            <span className="text-sm text-muted-foreground">seconds</span>
          </div>
          <EffectiveHint setting={heartbeatIntervalSetting} />
        </div>
      </SettingsSection>
      <SettingsSection
        title={
          <span className="inline-flex items-center">
            Max missed heartbeats
            <RestartBadge setting={maxMissedHeartbeatsSetting} />
          </span>
        }
        description="Number of consecutive missed heartbeats before an agent is considered offline."
      >
        <div className="space-y-1">
          <Label htmlFor="agent-missed" className="sr-only">
            Max missed heartbeats
          </Label>
          <Input
            id="agent-missed"
            type="number"
            min={1}
            className="w-24"
            value={maxMissedHeartbeats}
            onChange={(e) => onMaxMissedHeartbeatsChange(e.target.value)}
          />
          <EffectiveHint setting={maxMissedHeartbeatsSetting} />
        </div>
      </SettingsSection>
      <div className="flex justify-end py-6">
        <Button disabled={isSaving} onClick={onSave}>
          {isSaving ? 'Saving...' : 'Save'}
        </Button>
      </div>
    </>
  )
}

function PathsTab({
  dataDir,
  isSaving,
  dataDirSetting,
  onDataDirChange,
  onSave,
}: {
  dataDir: string
  isSaving: boolean
  dataDirSetting: ForgeSettingResponse | undefined
  onDataDirChange: (v: string) => void
  onSave: () => void
}) {
  return (
    <>
      <div className="mb-8">
        <h2 className="text-page font-semibold tracking-tight">Paths</h2>
        <p className="mt-1 text-sm text-muted-foreground">
          Forge data directory and storage paths.
        </p>
      </div>
      <SettingsSection
        title={
          <span className="inline-flex items-center">
            Data directory
            <RestartBadge setting={dataDirSetting} />
          </span>
        }
        description="Root directory where Forge stores its database, config, and logs."
      >
        <div className="space-y-1">
          <Label htmlFor="forge-data-dir" className="sr-only">
            Data directory
          </Label>
          <Input
            id="forge-data-dir"
            className="max-w-sm font-mono"
            placeholder="~/.forge"
            value={dataDir}
            onChange={(e) => onDataDirChange(e.target.value)}
          />
          <EffectiveHint setting={dataDirSetting} />
        </div>
      </SettingsSection>
      <div className="flex justify-end py-6">
        <Button disabled={isSaving} onClick={onSave}>
          {isSaving ? 'Saving...' : 'Save'}
        </Button>
      </div>
    </>
  )
}
