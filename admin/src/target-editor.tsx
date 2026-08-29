export interface SchedulerTargetEditorValue {
  targetType: string
  targetKey: string
  connectionKey: string
  operation: string
  dispatchMode: string
}

export interface SchedulerTargetEditorLabels {
  targetType: string
  targetKey: string
  targetPlaceholder: string
  targetLoading: string
  connectionKey: string
  operation: string
  dispatchMode: string
  runtimeCallback: string
  direct: string
}

export function SchedulerTargetEditor({value, targetTypes, targetValues, targetLoading, errors = {}, labels, onChange}: {
  value: SchedulerTargetEditorValue
  targetTypes: string[]
  targetValues: string[]
  targetLoading?: boolean
  errors?: Partial<Record<keyof SchedulerTargetEditorValue, string>>
  labels: SchedulerTargetEditorLabels
  onChange(next: SchedulerTargetEditorValue): void
}) {
  const update = <K extends keyof SchedulerTargetEditorValue>(key: K, next: SchedulerTargetEditorValue[K]) => onChange({...value, [key]: next})
  const field = 'grid gap-1.5'
  const control = 'h-9 w-full rounded-md border border-input bg-transparent px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring'
  const error = (key: keyof SchedulerTargetEditorValue) => errors[key] ? <p className='text-xs text-destructive'>{errors[key]}</p> : null
  return <div className='grid gap-4 sm:col-span-2 sm:grid-cols-2' data-scheduler-admin='target-editor'>
    <label className={field}><span className='text-sm font-medium'>{labels.targetType}</span><select className={control} value={value.targetType} onChange={(event) => update('targetType', event.target.value)}>{targetTypes.map((type) => <option key={type} value={type}>{type}</option>)}</select>{error('targetType')}</label>
    {value.targetType === 'http' ? <>
      <label className={field}><span className='text-sm font-medium'>{labels.connectionKey}</span><input className={control} value={value.connectionKey} onChange={(event) => update('connectionKey', event.target.value)} />{error('connectionKey')}</label>
      <label className={field}><span className='text-sm font-medium'>{labels.operation}</span><input className={control} value={value.operation} onChange={(event) => update('operation', event.target.value)} />{error('operation')}</label>
      <label className={field}><span className='text-sm font-medium'>{labels.dispatchMode}</span><select className={control} value={value.dispatchMode} onChange={(event) => update('dispatchMode', event.target.value)}><option value='runtime_callback'>{labels.runtimeCallback}</option><option value='direct'>{labels.direct}</option></select>{error('dispatchMode')}</label>
    </> : <label className={`${field} sm:col-span-2`}><span className='text-sm font-medium'>{labels.targetKey}</span><select className={control} value={value.targetKey} disabled={targetLoading || targetValues.length === 0} onChange={(event) => update('targetKey', event.target.value)}><option value=''>{targetLoading ? labels.targetLoading : labels.targetPlaceholder}</option>{targetValues.map((target) => <option key={target} value={target}>{target}</option>)}</select>{error('targetKey')}</label>}
  </div>
}
