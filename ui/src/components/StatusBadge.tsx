import { clsx } from 'clsx'

type Phase =
  | 'Pending'
  | 'Scheduling'
  | 'Running'
  | 'Succeeded'
  | 'Failed'
  | 'Degraded'

const lightStyles: Record<Phase, string> = {
  Pending:    'bg-gray-200 text-gray-600 dark:bg-gray-700 dark:text-gray-300',
  Scheduling: 'bg-blue-100 text-blue-700 dark:bg-blue-900 dark:text-blue-300',
  Running:    'bg-yellow-100 text-yellow-700 dark:bg-yellow-900 dark:text-yellow-300',
  Succeeded:  'bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300',
  Failed:     'bg-red-100 text-red-700 dark:bg-red-900 dark:text-red-300',
  Degraded:   'bg-orange-100 text-orange-700 dark:bg-orange-900 dark:text-orange-300',
}

interface Props {
  phase: string
}

export default function StatusBadge({ phase }: Props) {
  const style = lightStyles[phase as Phase] ?? lightStyles.Pending
  return (
    <span className={clsx('inline-block px-2 py-0.5 rounded text-xs font-semibold', style)}>
      {phase}
    </span>
  )
}
