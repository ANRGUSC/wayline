import type { ODAGDetail, PredictedNetworkFlow, ActualNetworkFlow } from '@/api/client'
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

const TASK_COLORS = [
  '#60a5fa', '#34d399', '#f59e0b', '#f87171',
  '#a78bfa', '#fb923c', '#e879f9', '#2dd4bf',
]

function taskColor(name: string, names: string[]): string {
  return TASK_COLORS[names.indexOf(name) % TASK_COLORS.length]
}

function fmtBytes(bytes: number): string {
  if (bytes >= 1_000_000_000) return `${(bytes / 1_000_000_000).toFixed(1)} GB`
  if (bytes >= 1_000_000) return `${(bytes / 1_000_000).toFixed(0)} MB`
  if (bytes >= 1_000) return `${(bytes / 1_000).toFixed(0)} KB`
  return `${bytes} B`
}

type Kind = 'predicted' | 'actual'
type Role = 'src' | 'dst'

interface Bar {
  kind: Kind
  role: Role
  node: string
  start: number
  end: number
  fromTask: string
  toTask: string
  srcNode: string
  dstNode: string
  dataSize: number
  ok: boolean
  id: string
}

function assignSubRows(items: Array<{ key: string; start: number; end: number }>): Record<string, number> {
  const sorted = [...items].sort((a, b) => a.start - b.start)
  const rowEnds: number[] = []
  const out: Record<string, number> = {}
  for (const it of sorted) {
    let placed = false
    for (let r = 0; r < rowEnds.length; r++) {
      if (it.start >= rowEnds[r]) {
        rowEnds[r] = it.end
        out[it.key] = r
        placed = true
        break
      }
    }
    if (!placed) {
      out[it.key] = rowEnds.length
      rowEnds.push(it.end)
    }
  }
  return out
}

interface Props {
  dag: ODAGDetail
  align?: ScheduleAlignment
}

export default function NetworkGantt({ dag, align }: Props) {
  const base = align ?? computeAlignment(dag)
  // NetworkGantt has a 4-item vertically-stacked legend; it needs more bottom
  // margin than the shared alignment (tuned for GanttChart's single-row legend).
  const extraMB = 56
  const a = { ...base, MB: base.MB + extraMB, totalH: base.totalH + extraMB }
  const predicted: PredictedNetworkFlow[] = dag.predictedNetworkFlows ?? []
  const actual: ActualNetworkFlow[] = dag.actualNetworkFlows ?? []
  const taskNames = dag.spec.tasks.map(t => t.name)

  if (predicted.length === 0 && actual.length === 0) {
    return <p className="text-on-faint text-sm">No network flows yet (same-node placement, or tasks haven't sent yet).</p>
  }

  const bars: Bar[] = []
  predicted.forEach((f, i) => {
    const id = `p${i}-${f.fromTask}-${f.toTask}-${f.start.toFixed(3)}`
    bars.push({ kind: 'predicted', role: 'src', node: f.srcNode, start: f.start, end: f.end,
      fromTask: f.fromTask, toTask: f.toTask, srcNode: f.srcNode, dstNode: f.dstNode,
      dataSize: f.dataSize, ok: true, id })
    bars.push({ kind: 'predicted', role: 'dst', node: f.dstNode, start: f.start, end: f.end,
      fromTask: f.fromTask, toTask: f.toTask, srcNode: f.srcNode, dstNode: f.dstNode,
      dataSize: f.dataSize, ok: true, id })
  })
  actual.forEach((f, i) => {
    const id = `a${i}-${f.fromTask}-${f.toTask}-${f.start.toFixed(3)}`
    bars.push({ kind: 'actual', role: 'src', node: f.srcNode, start: f.start, end: f.end,
      fromTask: f.fromTask, toTask: f.toTask, srcNode: f.srcNode, dstNode: f.dstNode,
      dataSize: f.dataSize, ok: f.ok, id })
    bars.push({ kind: 'actual', role: 'dst', node: f.dstNode, start: f.start, end: f.end,
      fromTask: f.fromTask, toTask: f.toTask, srcNode: f.srcNode, dstNode: f.dstNode,
      dataSize: f.dataSize, ok: f.ok, id })
  })

  const predSubRows: Record<string, Record<string, number>> = {}
  const actSubRows: Record<string, Record<string, number>> = {}
  for (const node of a.nodes) {
    predSubRows[node] = assignSubRows(
      bars.filter(b => b.node === node && b.kind === 'predicted')
        .map(b => ({ key: `${b.id}/${b.role}`, start: b.start, end: b.end }))
    )
    actSubRows[node] = assignSubRows(
      bars.filter(b => b.node === node && b.kind === 'actual')
        .map(b => ({ key: `${b.id}/${b.role}`, start: b.start, end: b.end }))
    )
  }

  function barY(b: Bar): number {
    if (b.kind === 'predicted') {
      const sub = predSubRows[b.node][`${b.id}/${b.role}`] ?? 0
      return a.netPredY(b.node) + sub * (a.BAR + a.BAR_GAP)
    }
    const sub = actSubRows[b.node][`${b.id}/${b.role}`] ?? 0
    return a.netActualY(b.node) + sub * (a.BAR + a.BAR_GAP)
  }

  const peakActualPerNode = Math.max(0, ...a.nodes.map(n => a.netActualSlots[n]))

  return (
    <div className="overflow-x-auto">
      <div className="text-xs text-on-faint mb-2">
        {predicted.length} predicted, {actual.length} actual flow{actual.length === 1 ? '' : 's'}
        {peakActualPerNode > 1 && (
          <span className="ml-2 text-amber-500 dark:text-amber-400">
            ⚠ observed peak NIC concurrency: {peakActualPerNode}
          </span>
        )}
      </div>
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
              stroke={isDark() ? '#f3f4f6' : '#000000'} strokeWidth={1}
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

        {/* Bars */}
        {bars.map(b => {
          const color = b.ok ? taskColor(b.fromTask, taskNames) : '#ef4444'
          const x = a.xs(b.start)
          const w = Math.max(a.xs(b.end) - a.xs(b.start), 3)
          const y = barY(b)
          const isSrc = b.role === 'src'
          const isPred = b.kind === 'predicted'
          const label = `${b.srcNode}→${b.dstNode} · ${b.fromTask}→${b.toTask}`
          const showInline = w > 80
          const fillOpacity = isPred ? (isSrc ? 0.18 : 0) : (isSrc ? 0.9 : 0.35)
          const strokeWidth = isPred ? 1.5 : (isSrc ? 0 : 1)
          const strokeDash = isPred ? '5 3' : undefined
          return (
            <g key={`${b.kind}-${b.id}-${b.role}-${b.node}`}>
              <rect
                x={x} y={y} width={w} height={a.BAR}
                fill={color}
                fillOpacity={fillOpacity}
                stroke={color}
                strokeWidth={strokeWidth}
                strokeDasharray={strokeDash}
                rx={2}
              />
              {showInline && (
                <text x={x + 4} y={y + a.BAR / 2} dominantBaseline="middle" fill={isPred ? color : (isSrc ? barTextDark() : color)} fillOpacity={isPred ? 0.85 : 1} fontWeight={!isPred && isSrc ? 'bold' : 'normal'} fontSize={9} style={{ pointerEvents: 'none' }}>
                  {label}
                </text>
              )}
              <title>{`${label}\n${fmtBytes(b.dataSize)}\n${b.kind} ${b.role === 'src' ? 'egress' : 'ingress'} on ${b.node}\n${b.start.toFixed(2)}s – ${b.end.toFixed(2)}s (${(b.end - b.start).toFixed(2)}s)${b.ok ? '' : '\nFAILED'}`}</title>
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

        {/* Legend — stacked vertically */}
        <g transform={`translate(${a.ML}, ${a.MT + a.totalInnerH + 28})`}>
          <g transform="translate(0, 0)">
            <rect x={0} y={0} width={14} height={10} fill="#9ca3af" fillOpacity={0.18} stroke="#9ca3af" strokeWidth={1.5} strokeDasharray="5 3" rx={1} />
            <text x={20} y={5} dominantBaseline="middle" fill={legendFill()} fontSize={11}>Predicted egress</text>
          </g>
          <g transform="translate(0, 15)">
            <rect x={0} y={0} width={14} height={10} fill="transparent" stroke="#9ca3af" strokeWidth={1.5} strokeDasharray="5 3" rx={1} />
            <text x={20} y={5} dominantBaseline="middle" fill={legendFill()} fontSize={11}>Predicted ingress</text>
          </g>
          <g transform="translate(0, 30)">
            <rect x={0} y={0} width={14} height={10} fill="#9ca3af" fillOpacity={0.9} rx={1} />
            <text x={20} y={5} dominantBaseline="middle" fill={legendFill()} fontSize={11}>Actual egress</text>
          </g>
          <g transform="translate(0, 45)">
            <rect x={0} y={0} width={14} height={10} fill="#9ca3af" fillOpacity={0.35} stroke="#9ca3af" strokeWidth={1} rx={1} />
            <text x={20} y={5} dominantBaseline="middle" fill={legendFill()} fontSize={11}>Actual ingress</text>
          </g>
        </g>
      </svg>
    </div>
  )
}
