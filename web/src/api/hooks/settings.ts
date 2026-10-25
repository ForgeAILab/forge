// Forge settings query and mutation hooks.

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { getSettings, updateSettings } from '@/api/client'
import { qk } from '@/api/query-keys'
import type { SettingsResponse, UpdateSettingsRequest } from '@/types/generated'

export function useSettingsQuery() {
  return useQuery({
    queryKey: qk.settings,
    queryFn: () => getSettings(),
  })
}

export function useUpdateSettings() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: UpdateSettingsRequest) => updateSettings(body),
    onSuccess: (data: SettingsResponse) => {
      queryClient.setQueryData(qk.settings, data)
    },
  })
}
