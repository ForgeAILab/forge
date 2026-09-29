export const PROJECT_SETTINGS_TAB_IDS = [
  'general',
  'repos',
  'members',
  'mcp',
  'hooks',
  'environment',
  'analytics',
  'workflow',
  'danger',
] as const

export type ProjectSettingsTab = (typeof PROJECT_SETTINGS_TAB_IDS)[number]

const projectSettingsTabs = new Set<string>(PROJECT_SETTINGS_TAB_IDS)

export function isProjectSettingsTab(value: string | undefined): value is ProjectSettingsTab {
  return value !== undefined && projectSettingsTabs.has(value)
}
