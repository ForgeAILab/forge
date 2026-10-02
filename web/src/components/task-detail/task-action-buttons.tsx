import { useState } from 'react'
import { toast } from 'sonner'
import { useTaskAction } from '@/api/hooks'
import { getApiErrorMessage } from '@/lib/api-error'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Label } from '@/components/ui/label'
import { Textarea } from '@/components/ui/textarea'
import type { Offer, TaskAction } from '@/types/generated'

/** The server owns validity, parameters and labels; this component only collects input. */
export function TaskActionButtons({ taskId, version, offers }: { taskId: string; version: number; offers: Offer[] }) {
  const command = useTaskAction()
  const [selected, setSelected] = useState<Offer | null>(null)
  const [action, setAction] = useState<TaskAction | null>(null)
  const apply = (value: TaskAction) => command.mutate({ taskId, version, action: value }, {
    onSuccess: () => { setSelected(null); setAction(null) },
    onError: (error) => toast.error(getApiErrorMessage(error, 'Task action failed')),
  })
  const choose = (offer: Offer) => {
    if (offer.parameters.length === 0) { apply(offer.action); return }
    setSelected(offer)
    setAction(offer.action)
  }
  return <>
    <div className="flex flex-wrap gap-2">
      {offers.map((offer) => <Button key={`${offer.action.verb}:${offer.reason}`} size="sm" variant="outline" disabled={command.isPending} onClick={() => choose(offer)}>{offer.label}</Button>)}
    </div>
    <Dialog open={selected != null} onOpenChange={(open) => { if (!open) setSelected(null) }}>
      <DialogContent>
        <DialogHeader><DialogTitle>{selected?.label}</DialogTitle></DialogHeader>
        <div className="space-y-4">
          {selected?.parameters.map((spec) => {
            const parameter = spec.name
            if (parameter === 'guidance' && action && (action.verb === 'retry' || action.verb === 'send_back')) {
              return <div className="space-y-2" key={parameter}><Label htmlFor="task-action-guidance">Guidance</Label><Textarea id="task-action-guidance" value={action.guidance ?? ''} onChange={(event) => setAction({ ...action, guidance: event.target.value })} /></div>
            }
            if (action?.verb === 'retry' && (parameter === 'fresh_session' || parameter === 'refresh_workspace' || parameter === 'reset_budget')) {
              return <label key={parameter} className="flex items-center gap-2 text-sm"><input type="checkbox" disabled={spec.boolean_values?.length === 1} checked={action[parameter] ?? false} onChange={(event) => setAction({ ...action, [parameter]: event.target.checked })} />{parameter.replaceAll('_', ' ')}</label>
            }
            if (parameter === 'override' && action?.verb === 'approve') {
              return <label key={parameter} className="flex items-center gap-2 text-sm"><input type="checkbox" disabled={spec.boolean_values?.length === 1} checked={action.override} onChange={(event) => setAction({ ...action, override: event.target.checked })} />Override checks</label>
            }
            return null
          })}
        </div>
        <DialogFooter><Button variant="outline" onClick={() => setSelected(null)}>Close</Button><Button disabled={command.isPending || !action} onClick={() => { if (action) apply(action) }}>Apply</Button></DialogFooter>
      </DialogContent>
    </Dialog>
  </>
}
