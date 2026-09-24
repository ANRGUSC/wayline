import { clsx } from 'clsx'

type Phase =
  | 'Pending'
  | 'Scheduling'
  | 'Running'
  | 'Succeeded'
  | 'Failed'
  | 'Degraded'

// Tinted fill, matching ink, thin outline: the same box grammar as the
// paper's figures.
const styles: Record<Phase, string> = {
  Pending:    'bg-pending-tint text-pending border-pending',
  Scheduling: 'bg-ctrl-tint text-ctrl border-ctrl',
  Running:    'bg-run-tint text-run border-run',
  Succeeded:  'bg-ok-tint text-ok border-ok',
  Failed:     'bg-fail-tint text-fail border-fail',
  Degraded:   'bg-data-tint text-data border-data',
}

interface Props {
  phase: string
}

export default function StatusBadge({ phase }: Props) {
  const style = styles[phase as Phase] ?? styles.Pending
  return (
    <span className={clsx('inline-block px-2 py-0.5 rounded-sm border text-xs font-semibold', style)}>
      {phase}
    </span>
  )
}
