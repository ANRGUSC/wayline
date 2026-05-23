import type { ODAGDetail } from '@/api/client'
import type { ScheduleAlignment } from './scheduleAlignment'
import { computeAlignment } from './scheduleAlignment'

function isDark() { return document.documentElement.classList.contains('dark') }
function rowEven() { return isDark() ? '#0f172a' : '#f9fafb' }
function rowOdd() { return isDark() ? '#111827' : '#f3f4f6' }
function gridStroke() { return isDark() ? '#1f2937' : '#e5e7eb' }
function axisStroke() { return isDark() ? '#374151' : '#d1d5db' }
function labelFill() { return isDark() ? '#9ca3af' : '#6b7280' }
function tickFill() { return isDark() ? '#6b7280' : '#9ca3af' }
function barTextDark() { return isDark() ? '#0f172a' : '#ffffff' }
function legendFill() { return isDark() ? '#6b7280' : '#9ca3af' }
function dashStroke() { return isDark() ? '#374151' : '#d1d5db' }

const TASK_COLORS = [
  '#60a5fa', '#34d399', '#f59e0b', '#f87171',
  '#a78bfa', '#fb923c', '#e879f9', '#2dd4bf',
]

function taskColor(name: string, names: string[]): string {
  return TASK_COLORS[names.indexOf(name) % TASK_COLORS.length]
}

function assignSubRows(bars: Array<{ name: string; start: number; end: number }>): Record<string, number> {
  const sorted = [...bars].sort((a, b) => a.start - b.start)
  const rows: number[] = []
  const assignment: Record<string, number> = {}
  for (const bar of sorted) {
    let placed = false
    for (let r = 0; r < rows.length; r++) {
      if (bar.start >= rows[r]) {
        rows[r] = bar.end
        assignment[bar.name] = r
        placed = true
        break
      }
    }
    if (!placed) {
      assignment[bar.name] = rows.length
      rows.push(bar.end)
    }
  }
  return assignment
}

interface Props {
  dag: ODAGDetail
  align?: ScheduleAlignment
}

export default function GanttChart({ dag, align }: Props) {
  const a = align ?? computeAlignment(dag)
  const predicted = dag.predictedTasks ?? []
  const taskNames = dag.spec.tasks.map(t => t.name)

  const activeTasks = dag.tasks.filter(t => t.startTime && t.node)
  const refMs = activeTasks.length > 0
    ? Math.min(...activeTasks.map(t => new Date(t.startTime!).getTime()))
    : null
  const actualBars = activeTasks.map(t => ({
    name: t.name,
    node: t.node!,
    start: (new Date(t.startTime!).getTime() - refMs!) / 1000,
    end: t.completionTime
      ? (new Date(t.completionTime).getTime() - refMs!) / 1000
      : (Date.now() - refMs!) / 1000,
  }))

  if (a.nodes.length === 0) {
    return <p className="text-on-faint text-sm">No schedule data yet.</p>
  }

  const predSubRows: Record<string, Record<string, number>> = {}
  const actSubRows: Record<string, Record<string, number>> = {}
  for (const node of a.nodes) {
    predSubRows[node] = assignSubRows(
      predicted.filter(p => p.node === node).map(p => ({ name: p.name, start: p.estStart, end: p.estEnd }))
    )
    actSubRows[node] = assignSubRows(actualBars.filter(b => b.node === node))
  }

  return (
    <div className="overflow-x-auto">
      <svg viewBox={`0 0 ${a.W} ${a.totalH}`} width="100%" style={{ display: 'block', fontFamily: 'inherit' }}>
        {/* Row backgrounds */}
        {a.nodes.map((node, i) => (
          <rect
            key={node}
            x={a.ML} y={a.nodeYOffset[node]}
            width={a.innerW} height={a.nodeHeight[node]}
            fill={i % 2 === 0 ? rowEven() : rowOdd()}
          />
        ))}

        {/* Row separators */}
        {a.nodes.map((node, i) => {
          if (i === 0) return null
          return (
            <line
              key={`sep-${node}`}
              x1={a.ML} y1={a.nodeYOffset[node]}
              x2={a.ML + a.innerW} y2={a.nodeYOffset[node]}
              stroke={dashStroke()} strokeWidth={1} strokeDasharray="6 4"
            />
          )
        })}

        {/* Vertical grid */}
        {a.ticks.filter(t => t <= a.maxTime).map(t => (
          <line
            key={t}
            x1={a.xs(t)} y1={a.MT}
            x2={a.xs(t)} y2={a.MT + a.totalInnerH}
            stroke={gridStroke()} strokeWidth={1}
          />
        ))}

        {/* Node labels */}
        {a.nodes.map(node => (
          <text
            key={node}
            x={a.ML - 8}
            y={a.nodeYOffset[node] + a.nodeHeight[node] / 2}
            textAnchor="end"
            dominantBaseline="middle"
            fill={labelFill()}
            fontSize={11}
          >
            {node}
          </text>
        ))}

        {/* Predicted bars */}
        {predicted.map(p => {
          const color = taskColor(p.name, taskNames)
          const x = a.xs(p.estStart)
          const w = Math.max(a.xs(p.estEnd) - a.xs(p.estStart), 3)
          const subRow = predSubRows[p.node]?.[p.name] ?? 0
          const y = a.execPredY(p.node) + subRow * (a.BAR + a.BAR_GAP)
          return (
            <g key={`pred-${p.name}`}>
              <rect
                x={x} y={y} width={w} height={a.BAR}
                fill={color} fillOpacity={0.18}
                stroke={color} strokeWidth={1.5} strokeDasharray="5 3"
                rx={2}
              />
              <text x={x + 4} y={y + a.BAR / 2} dominantBaseline="middle" fill={color} fillOpacity={0.8} fontSize={9} style={{ pointerEvents: 'none' }}>
                {p.name}
              </text>
              <title>{`${p.name} predicted: ${p.estStart.toFixed(1)}s – ${p.estEnd.toFixed(1)}s (${p.node})`}</title>
            </g>
          )
        })}

        {/* Actual bars */}
        {actualBars.map(b => {
          const color = taskColor(b.name, taskNames)
          const x = a.xs(b.start)
          const w = Math.max(a.xs(b.end) - a.xs(b.start), 3)
          const subRow = actSubRows[b.node]?.[b.name] ?? 0
          const y = a.execActualY(b.node) + subRow * (a.BAR + a.BAR_GAP)
          return (
            <g key={`actual-${b.name}`}>
              <rect x={x} y={y} width={w} height={a.BAR} fill={color} fillOpacity={0.9} rx={2} />
              <text x={x + 4} y={y + a.BAR / 2} dominantBaseline="middle" fill={barTextDark()} fontWeight="bold" fontSize={9} style={{ pointerEvents: 'none' }}>
                {b.name}
              </text>
              <title>{`${b.name} actual: ${b.start.toFixed(1)}s – ${b.end.toFixed(1)}s (${b.node})`}</title>
            </g>
          )
        })}

        {/* X axis */}
        <line x1={a.ML} y1={a.MT + a.totalInnerH} x2={a.ML + a.innerW} y2={a.MT + a.totalInnerH} stroke={axisStroke()} strokeWidth={1} />
        {a.ticks.filter(t => t <= a.maxTime + 1e-6).map(t => (
          <g key={`xtick-${t}`}>
            <line x1={a.xs(t)} y1={a.MT + a.totalInnerH} x2={a.xs(t)} y2={a.MT + a.totalInnerH + 5} stroke={axisStroke()} />
            <text x={a.xs(t)} y={a.MT + a.totalInnerH + 16} textAnchor="middle" fill={tickFill()} fontSize={10}>{t}s</text>
          </g>
        ))}

        {/* Legend */}
        <g transform={`translate(${a.ML}, ${a.MT + a.totalInnerH + 30})`}>
          <rect x={0} y={0} width={14} height={10} fill="#9ca3af" fillOpacity={0.18} stroke="#9ca3af" strokeDasharray="5 3" strokeWidth={1.5} rx={1} />
          <text x={20} y={5} dominantBaseline="middle" fill={legendFill()} fontSize={11}>Predicted</text>
          <rect x={90} y={0} width={14} height={10} fill="#9ca3af" fillOpacity={0.9} rx={1} />
          <text x={110} y={5} dominantBaseline="middle" fill={legendFill()} fontSize={11}>Actual</text>
        </g>
      </svg>
    </div>
  )
}
