import type { ODAGDetail } from '@/api/client'
import { ODAG_COLORS, BATCH_PRESETS } from '@/data/batchPresets'

function isDark() { return document.documentElement.classList.contains('dark') }
function rowEven() { return isDark() ? '#0f172a' : '#f9fafb' }
function rowOdd() { return isDark() ? '#111827' : '#f3f4f6' }
function gridStroke() { return isDark() ? '#1f2937' : '#e5e7eb' }
function axisStroke() { return isDark() ? '#374151' : '#d1d5db' }
function labelFill() { return isDark() ? '#9ca3af' : '#6b7280' }
function tickFill() { return isDark() ? '#6b7280' : '#9ca3af' }
function barTextDark() { return isDark() ? '#0f172a' : '#ffffff' }
function legendFill() { return isDark() ? '#9ca3af' : '#6b7280' }
function legendSmallFill() { return isDark() ? '#6b7280' : '#9ca3af' }

interface Props {
  odags: ODAGDetail[]
}

interface Bar {
  odagName: string
  odagIdx: number
  taskName: string
  node: string
  start: number
  end: number
  color: string
  kind: 'predicted' | 'actual'
}

export default function BatchGanttChart({ odags }: Props) {
  if (!odags || odags.length === 0) {
    return <p className="text-on-faint text-sm">No schedule data yet.</p>
  }

  const nameIndex = new Map(BATCH_PRESETS.map((p, i) => [p.name, i]))

  // Compute a shared reference time: earliest createdAt across all ODAGs.
  const timestamps = odags.map(o => new Date(o.createdAt).getTime()).filter(t => !isNaN(t))
  if (timestamps.length === 0) {
    return <p className="text-on-faint text-sm">Waiting for schedule data...</p>
  }
  const refMs = Math.min(...timestamps)

  // Build bars for all ODAGs.
  const bars: Bar[] = []

  for (const odag of odags) {
    const idx = nameIndex.get(odag.name) ?? 0
    const color = ODAG_COLORS[idx % ODAG_COLORS.length]
    const odagRefMs = new Date(odag.createdAt).getTime()
    if (isNaN(odagRefMs)) continue
    const odagOffset = (odagRefMs - refMs) / 1000

    // Predicted bars: offset by when this ODAG was submitted.
    for (const p of odag.predictedTasks ?? []) {
      if (!p.node || p.estStart == null || p.estEnd == null) continue
      bars.push({
        odagName: odag.name,
        odagIdx: idx,
        taskName: p.name,
        node: p.node,
        start: odagOffset + p.estStart,
        end: odagOffset + p.estEnd,
        color,
        kind: 'predicted',
      })
    }

    // Actual bars: use absolute timestamps.
    const tasks = odag.tasks ?? []
    for (const t of tasks) {
      if (!t.startTime || !t.node) continue
      const startMs = new Date(t.startTime).getTime()
      if (isNaN(startMs)) continue
      const endMs = t.completionTime
        ? new Date(t.completionTime).getTime()
        : Date.now()
      bars.push({
        odagName: odag.name,
        odagIdx: idx,
        taskName: t.name,
        node: t.node,
        start: (startMs - refMs) / 1000,
        end: (endMs - refMs) / 1000,
        color,
        kind: 'actual',
      })
    }
  }

  if (bars.length === 0) {
    return <p className="text-on-faint text-sm">Waiting for schedule data...</p>
  }

  // Group bars by node, then by odagIdx within each node to create lanes.
  const nodeMap = new Map<string, Map<number, Bar[]>>()
  for (const b of bars) {
    if (!nodeMap.has(b.node)) nodeMap.set(b.node, new Map())
    const odagMap = nodeMap.get(b.node)!
    if (!odagMap.has(b.odagIdx)) odagMap.set(b.odagIdx, [])
    odagMap.get(b.odagIdx)!.push(b)
  }

  const nodes = Array.from(nodeMap.keys()).sort()
  const maxTime = Math.max(...bars.map(b => b.end), 1)

  // Layout constants.
  const ML = 90
  const MR = 20
  const MT = 16
  const MB = 60
  const BAR_H = 10
  const LANE_GAP = 2
  const LANE_H = BAR_H * 2 + LANE_GAP
  const NODE_PAD = 6
  const W = 1000

  // Compute per-node row heights and y-offsets.
  const nodeY: Record<string, number> = {}
  const nodeHeight: Record<string, number> = {}
  let curY = MT
  for (const node of nodes) {
    const laneCount = nodeMap.get(node)!.size
    const h = laneCount * LANE_H + (laneCount - 1) * LANE_GAP + NODE_PAD * 2
    nodeY[node] = curY
    nodeHeight[node] = h
    curY += h
  }
  const innerH = curY - MT
  const totalH = innerH + MT + MB
  const innerW = W - ML - MR

  const xs = (t: number) => (t / maxTime) * innerW

  // Tick values.
  const tickCount = 8
  const step = Math.max(Math.ceil(maxTime / tickCount), 1)
  const ticks: number[] = []
  for (let t = 0; t <= maxTime + step; t += step) ticks.push(Math.round(t))

  function laneY(node: string, odagIdx: number): number {
    const odagMap = nodeMap.get(node)
    if (!odagMap) return MT
    const sortedIdxs = Array.from(odagMap.keys()).sort((a, b) => a - b)
    const laneIndex = sortedIdxs.indexOf(odagIdx)
    if (laneIndex < 0) return nodeY[node] + NODE_PAD
    return nodeY[node] + NODE_PAD + laneIndex * (LANE_H + LANE_GAP)
  }

  return (
    <div className="overflow-x-auto">
      <svg
        viewBox={`0 0 ${W} ${totalH}`}
        width="100%"
        style={{ display: 'block', fontFamily: 'inherit' }}
      >
        {/* Row backgrounds */}
        {nodes.map((node, i) => (
          <rect
            key={node}
            x={ML} y={nodeY[node]}
            width={innerW} height={nodeHeight[node]}
            fill={i % 2 === 0 ? rowEven() : rowOdd()}
          />
        ))}

        {/* Vertical grid lines */}
        {ticks.filter(t => t <= maxTime).map(t => (
          <line
            key={t}
            x1={ML + xs(t)} y1={MT}
            x2={ML + xs(t)} y2={MT + innerH}
            stroke={gridStroke()} strokeWidth={1}
          />
        ))}

        {/* Node labels */}
        {nodes.map(node => (
          <text
            key={node}
            x={ML - 8}
            y={nodeY[node] + nodeHeight[node] / 2}
            textAnchor="end"
            dominantBaseline="middle"
            fill={labelFill()}
            fontSize={11}
          >
            {node}
          </text>
        ))}

        {/* Draw bars grouped by node > odag > kind */}
        {nodes.flatMap(node => {
          const odagMap = nodeMap.get(node)!
          return Array.from(odagMap.entries()).flatMap(([odagIdx, nodeBars]) => {
            const baseY = laneY(node, odagIdx)
            return nodeBars.map(b => {
              const x = ML + xs(b.start)
              const w = Math.max(xs(b.end - b.start), 3)
              const y = b.kind === 'predicted' ? baseY : baseY + BAR_H + LANE_GAP
              const isPred = b.kind === 'predicted'
              return (
                <g key={`${b.kind}-${b.odagName}-${b.taskName}`}>
                  <rect
                    x={x} y={y} width={w} height={BAR_H}
                    fill={b.color}
                    fillOpacity={isPred ? 0.18 : 0.85}
                    stroke={isPred ? b.color : 'none'}
                    strokeWidth={isPred ? 1 : 0}
                    strokeDasharray={isPred ? '4 2' : undefined}
                    rx={2}
                  />
                  {w > 30 && (
                    <text
                      x={x + 3} y={y + BAR_H / 2}
                      dominantBaseline="middle"
                      fill={isPred ? b.color : barTextDark()}
                      fillOpacity={isPred ? 0.7 : 1}
                      fontWeight={isPred ? 'normal' : 'bold'}
                      fontSize={8}
                      style={{ pointerEvents: 'none' }}
                    >
                      {b.taskName}
                    </text>
                  )}
                  <title>
                    {`${b.odagName}/${b.taskName} (${b.kind}): ${b.start.toFixed(1)}s - ${b.end.toFixed(1)}s`}
                  </title>
                </g>
              )
            })
          })
        })}

        {/* X axis */}
        <line
          x1={ML} y1={MT + innerH}
          x2={ML + innerW} y2={MT + innerH}
          stroke={axisStroke()} strokeWidth={1}
        />
        {ticks.filter(t => t <= maxTime + step).map(t => (
          <g key={`xtick-${t}`}>
            <line
              x1={ML + xs(t)} y1={MT + innerH}
              x2={ML + xs(t)} y2={MT + innerH + 5}
              stroke={axisStroke()}
            />
            <text
              x={ML + xs(t)} y={MT + innerH + 16}
              textAnchor="middle"
              fill={tickFill()}
              fontSize={10}
            >
              {t}s
            </text>
          </g>
        ))}

        {/* Legend */}
        <g transform={`translate(${ML}, ${MT + innerH + 28})`}>
          {BATCH_PRESETS.map((p, i) => {
            const x = i * 160
            return (
              <g key={p.name} transform={`translate(${x}, 0)`}>
                <rect x={0} y={0} width={12} height={10} fill={ODAG_COLORS[i]} fillOpacity={0.85} rx={2} />
                <text x={16} y={5} dominantBaseline="middle" fill={legendFill()} fontSize={10}>
                  {p.name}
                </text>
              </g>
            )
          })}
          <g transform="translate(0, 16)">
            <rect x={0} y={0} width={12} height={8} fill="#9ca3af" fillOpacity={0.18}
              stroke="#9ca3af" strokeDasharray="4 2" strokeWidth={1} rx={1} />
            <text x={16} y={4} dominantBaseline="middle" fill={legendSmallFill()} fontSize={10}>Predicted</text>
            <rect x={90} y={0} width={12} height={8} fill="#9ca3af" fillOpacity={0.85} rx={1} />
            <text x={106} y={4} dominantBaseline="middle" fill={legendSmallFill()} fontSize={10}>Actual</text>
          </g>
        </g>
      </svg>
    </div>
  )
}
