// Shared layout math so the execution Gantt and network Gantt paint the same
// node rows at identical y-positions and share a single time axis.

import type { ODAGDetail } from '@/api/client'

export interface ScheduleAlignment {
  nodes: string[]
  maxTime: number
  W: number
  ML: number
  MR: number
  MT: number
  MB: number
  BAR: number
  BAR_GAP: number
  NODE_PAD: number
  HALF_GAP: number
  innerW: number
  totalInnerH: number
  totalH: number
  // Vertical slot counts per node for each of the four band types.
  execPredSlots: Record<string, number>
  execActualSlots: Record<string, number>
  netPredSlots: Record<string, number>
  netActualSlots: Record<string, number>
  // Starting y of each node row (before NODE_PAD).
  nodeYOffset: Record<string, number>
  // Total height reserved for one node's row.
  nodeHeight: Record<string, number>
  // Sub-y helpers: y of the first bar in each band for a given node.
  execPredY: (node: string) => number
  execActualY: (node: string) => number
  netPredY: (node: string) => number
  netActualY: (node: string) => number
  // X axis scale: seconds → pixel x.
  xs: (t: number) => number
  ticks: number[]
}

function maxOverlap(rows: Array<{ s: number; e: number }>): number {
  if (rows.length <= 1) return rows.length
  const events: Array<{ t: number; d: number }> = []
  for (const r of rows) {
    events.push({ t: r.s, d: 1 })
    events.push({ t: r.e, d: -1 })
  }
  events.sort((a, b) => a.t - b.t || a.d - b.d)
  let cur = 0, mx = 0
  for (const e of events) {
    cur += e.d
    mx = Math.max(mx, cur)
  }
  return mx
}

export function computeAlignment(dag: ODAGDetail): ScheduleAlignment {
  const predicted = dag.predictedTasks ?? []
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
  const predictedFlows = dag.predictedNetworkFlows ?? []
  const actualFlows = dag.actualNetworkFlows ?? []

  // Union of all nodes.
  const nodeSet = new Set<string>()
  predicted.forEach(p => nodeSet.add(p.node))
  actualBars.forEach(b => nodeSet.add(b.node))
  predictedFlows.forEach(f => { nodeSet.add(f.srcNode); nodeSet.add(f.dstNode) })
  actualFlows.forEach(f => { nodeSet.add(f.srcNode); nodeSet.add(f.dstNode) })
  const nodes = Array.from(nodeSet).sort()

  // Per-node slot counts for each band.
  const execPredSlots: Record<string, number> = {}
  const execActualSlots: Record<string, number> = {}
  const netPredSlots: Record<string, number> = {}
  const netActualSlots: Record<string, number> = {}
  for (const node of nodes) {
    execPredSlots[node] = Math.max(
      maxOverlap(predicted.filter(p => p.node === node).map(p => ({ s: p.estStart, e: p.estEnd }))),
      1,
    )
    execActualSlots[node] = Math.max(
      maxOverlap(actualBars.filter(b => b.node === node).map(b => ({ s: b.start, e: b.end }))),
      1,
    )
    // Network bars live on both src and dst rows, so take both ends into account.
    const netPredOnNode = predictedFlows.flatMap(f => {
      const out = [] as Array<{ s: number; e: number }>
      if (f.srcNode === node) out.push({ s: f.start, e: f.end })
      if (f.dstNode === node) out.push({ s: f.start, e: f.end })
      return out
    })
    const netActualOnNode = actualFlows.flatMap(f => {
      const out = [] as Array<{ s: number; e: number }>
      if (f.srcNode === node) out.push({ s: f.start, e: f.end })
      if (f.dstNode === node) out.push({ s: f.start, e: f.end })
      return out
    })
    netPredSlots[node] = Math.max(maxOverlap(netPredOnNode), netPredOnNode.length > 0 ? 1 : 0)
    netActualSlots[node] = Math.max(maxOverlap(netActualOnNode), netActualOnNode.length > 0 ? 1 : 0)
  }

  // Time axis — include every kind of bar end we know about.
  const maxTime = Math.max(
    1,
    ...actualBars.map(b => b.end),
    ...predicted.map(p => p.estEnd),
    ...predictedFlows.map(f => f.end),
    ...actualFlows.map(f => f.end),
  )

  // Layout constants identical to the chart components.
  const ML = 90, MR = 20, MT = 16, MB = 44
  const BAR = 14, BAR_GAP = 2, NODE_PAD = 8, HALF_GAP = 4
  const W = 960
  const innerW = W - ML - MR

  // Per-node total height.
  const nodeHeight: Record<string, number> = {}
  const nodeYOffset: Record<string, number> = {}
  let running = 0
  for (const node of nodes) {
    const ep = execPredSlots[node]
    const ea = execActualSlots[node]
    const np = netPredSlots[node]
    const na = netActualSlots[node]
    const execH = ep * (BAR + BAR_GAP) + HALF_GAP + ea * (BAR + BAR_GAP)
    const netH = (np + na > 0)
      ? (np > 0 ? np * (BAR + BAR_GAP) : 0) + ((np > 0 && na > 0) ? HALF_GAP : 0) + (na > 0 ? na * (BAR + BAR_GAP) : 0)
      : 0
    nodeHeight[node] = NODE_PAD * 2 + execH + (netH > 0 ? HALF_GAP * 2 : 0) + netH
    nodeYOffset[node] = MT + running
    running += nodeHeight[node]
  }
  const totalInnerH = running
  const totalH = totalInnerH + MT + MB

  const xs = (t: number) => ML + (t / maxTime) * innerW
  const tickCount = 7
  const step = Math.ceil(maxTime / tickCount)
  const ticks: number[] = []
  for (let t = 0; t <= maxTime + step; t += step) ticks.push(Math.round(t))

  // Y of first bar in each band.
  function execPredY(node: string): number {
    return nodeYOffset[node] + NODE_PAD
  }
  function execActualY(node: string): number {
    return execPredY(node) + execPredSlots[node] * (BAR + BAR_GAP) + HALF_GAP
  }
  function netPredY(node: string): number {
    const execBottom = execActualY(node) + execActualSlots[node] * (BAR + BAR_GAP)
    return execBottom + HALF_GAP * 2
  }
  function netActualY(node: string): number {
    return netPredY(node) + netPredSlots[node] * (BAR + BAR_GAP) + (netPredSlots[node] > 0 ? HALF_GAP : 0)
  }

  return {
    nodes, maxTime, W, ML, MR, MT, MB, BAR, BAR_GAP, NODE_PAD, HALF_GAP,
    innerW, totalInnerH, totalH,
    execPredSlots, execActualSlots, netPredSlots, netActualSlots,
    nodeYOffset, nodeHeight,
    execPredY, execActualY, netPredY, netActualY,
    xs, ticks,
  }
}
