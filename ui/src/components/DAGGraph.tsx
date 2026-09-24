/**
 * DAGGraph: interactive DAG visualization using React Flow with custom nodes.
 *
 * Each node shows: task name, phase badge, assigned node.
 * On hover: a tooltip with start time, end time, and runtime duration.
 */

import { useMemo, useCallback } from 'react'
import {
  ReactFlow,
  Node,
  Edge,
  Background,
  Controls,
  Position,
  NodeProps,
  Handle,
  useReactFlow,
  ReactFlowProvider,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import type { TaskStatus, ODAGDetail } from '@/api/client'

// ─── colour palette (light / dark) ──────────────────────────────────────────

import * as T from '@/theme'

// Box grammar from the paper's figures: a tinted box with an ink outline;
// the tint says what phase the task is in, the outline stays the same.
function phaseBgColor(phase: string): string { return T.phaseTint(phase) }
function phaseBorderColor(): string { return T.ink() }
function phaseTextColor(phase: string): string { return T.phaseColor(phase) }
function stateTextColor(state: string): string { return T.stateColor(state) }

function cardTextColor(): string { return T.textPrimary() }
function labelColor(): string { return T.textMuted() }
function valueColor(): string { return T.textSecondary() }
function tooltipBg(): string { return T.surface() }
function tooltipShadow(): string { return T.isDark() ? '0 8px 24px rgba(0,0,0,0.6)' : '0 8px 24px rgba(0,0,0,0.12)' }
function chipBg(): string { return T.surfaceCard() }
function chipBorder(): string { return T.line() }
function chipText(): string { return T.textSecondary() }
function gridColor(): string { return T.lineSoft() }
function controlsBg(): string { return T.surface() }
function controlsBorder(): string { return T.line() }
function labelBgFill(): string { return T.surface() }
function constraintColor(): string { return T.textFaint() }
function edgeDefaultColor(): string { return T.ink() }

// If the data-agent state is Succeeded, treat the node as visually Succeeded
// even if the pod phase hasn't caught up yet (brief k8s lag after container exit).
function effectivePhase(phase: string, state?: string) {
  return state === 'Done' ? 'Succeeded' : phase
}
function bg(phase: string)     { return phaseBgColor(phase) }
function border(_phase: string) { return phaseBorderColor() }
function txt(phase: string)    { return phaseTextColor(phase) }
function stateTxt(state: string) { return stateTextColor(state) }

// ─── helpers ─────────────────────────────────────────────────────────────────

function fmtTime(iso?: string): string {
  if (!iso) return '—'
  return new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
}

function fmtDate(iso?: string): string {
  if (!iso) return ''
  return new Date(iso).toLocaleDateString([], { month: 'short', day: 'numeric' })
}

function runtimeStr(startIso?: string, endIso?: string): string {
  if (!startIso || !endIso) return '—'
  const s = Math.round((new Date(endIso).getTime() - new Date(startIso).getTime()) / 1000)
  if (s < 60) return `${s}s`
  return `${Math.floor(s / 60)}m ${s % 60}s`
}

function fmtBytes(b?: string): string {
  if (!b) return ''
  const n = parseInt(b, 10)
  if (isNaN(n) || n === 0) return ''
  if (n >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(1)} GB`
  if (n >= 1_000_000)     return `${(n / 1_000_000).toFixed(0)} MB`
  if (n >= 1_000)         return `${(n / 1_000).toFixed(0)} KB`
  return `${n} B`
}

// ─── custom node ─────────────────────────────────────────────────────────────

interface TaskNodeData {
  taskName: string
  phase: string
  state?: string
  sending?: boolean
  nodeName?: string
  startTime?: string
  completionTime?: string
  dataSize?: string
  specRuntime?: number
  image?: string
  constraints?: string[]
  hasDeps: boolean
  hasDownstream: boolean
  [key: string]: unknown  // satisfy React Flow's NodeProps constraint
}

function TaskNode({ data }: NodeProps) {
  const d = data as TaskNodeData
  const phase = effectivePhase(d.phase ?? 'Pending', d.state)
  const state = d.state
  const runtime = runtimeStr(d.startTime, d.completionTime)
  const startDate = fmtDate(d.startTime)
  const dataSizeFmt = fmtBytes(d.dataSize)


  return (
    <div
      className="relative group"
      style={{
        background: bg(phase),
        border: `1.5px solid ${border(phase)}`,
        borderRadius: 3,
        minWidth: 175,
        padding: '10px 14px',
        color: cardTextColor(),
        cursor: 'default',
      }}
    >
      {/* React Flow connection handles */}
      {d.hasDeps       && <Handle type="target" position={Position.Left}  style={{ background: border(phase), border: 'none', width: 10, height: 10 }} />}
      {d.hasDownstream && <Handle type="source" position={Position.Right} style={{ background: border(phase), border: 'none', width: 10, height: 10 }} />}

      {/* Main card content */}
      <div className="font-semibold text-sm text-on mb-1 truncate" style={{ maxWidth: 195 }}>
        {d.taskName}
      </div>

      {/* Phase pill + state badge + data size */}
      <div className="flex items-center justify-between gap-2 mb-1">
        <div className="flex items-center gap-1.5">
          {/* Phase pill */}
          <span
            className="text-xs font-medium px-1.5 py-0.5 rounded"
            style={{ background: T.surface(), color: txt(phase), border: `1px solid ${txt(phase)}` }}
          >
            {phase}
          </span>
          {/* State badge — always shown */}
          {state && (
            <span className="text-xs font-medium" style={{ color: stateTxt(state) }}>
              · {state}
            </span>
          )}
        </div>
        {dataSizeFmt && (
          <div className="text-xs" style={{ color: T.data(), fontVariantNumeric: 'tabular-nums' }}>
            {dataSizeFmt}
          </div>
        )}
      </div>

      {/* Assigned node */}
      {d.nodeName && (
        <div className="text-xs" style={{ color: labelColor() }}>
          {d.nodeName}
          {d.startTime && d.completionTime && (
            <span style={{ color: T.run(), marginLeft: 6 }}>{runtime}</span>
          )}
        </div>
      )}

      {d.constraints && d.constraints.length > 0 && (
        <div className="text-xs mt-1" style={{ color: constraintColor() }}>
          {'↦ '}{(d.constraints as string[]).join(', ')}
        </div>
      )}

      {/* Hover tooltip */}
      <div
        className="absolute left-1/2 z-50 pointer-events-none opacity-0 group-hover:opacity-100 transition-opacity duration-150"
        style={{
          bottom: 'calc(100% + 10px)',
          transform: 'translateX(-50%)',
          minWidth: 240,
          background: tooltipBg(),
          border: `1px solid ${border(phase)}`,
          borderRadius: 8,
          padding: '10px 14px',
          boxShadow: tooltipShadow(),
        }}
      >
        {/* Arrow */}
        <div
          style={{
            position: 'absolute',
            bottom: -6,
            left: '50%',
            transform: 'translateX(-50%)',
            width: 12,
            height: 6,
            overflow: 'hidden',
          }}
        >
          <div style={{
            width: 12, height: 12, background: border(phase),
            transform: 'rotate(45deg)', transformOrigin: 'top left',
            marginTop: -6,
          }} />
        </div>

        <div className="text-xs space-y-1.5">
          <div className="font-semibold text-on text-sm mb-2">{d.taskName}</div>

          <Row label="Phase"  value={phase}        color={txt(phase)} />
          {state && <Row label="State" value={state} color={stateTxt(state)} />}
          {d.nodeName && <Row label="Node" value={d.nodeName} />}

          {dataSizeFmt && (
            <Row label="Output size" value={dataSizeFmt} color={T.data()} />
          )}
          {d.specRuntime != null && (
            <Row label="Spec runtime" value={`${d.specRuntime}s`} color={T.textMuted()} />
          )}

          {(d.startTime || d.completionTime) && (
            <div className="border-t border-line pt-1.5 mt-1.5 space-y-1.5">
              {d.startTime && (
                <Row label="Start" value={`${startDate} ${fmtTime(d.startTime)}`} />
              )}
              {d.completionTime && (
                <Row label="End" value={`${fmtDate(d.completionTime)} ${fmtTime(d.completionTime)}`} />
              )}
              {d.startTime && d.completionTime && (
                <Row label="Duration" value={runtime} color={T.run()} />
              )}
            </div>
          )}

          {d.image && (
            <div className="border-t border-line pt-1.5 mt-1.5">
              <span className="text-on-faint">image: </span>
              <span className="text-on-secondary break-all">{d.image.split('/').pop()}</span>
            </div>
          )}

          {d.constraints && d.constraints.length > 0 && (
            <div className="border-t border-line pt-1.5 mt-1.5">
              <div className="text-on-faint mb-1">allowed nodes:</div>
              <div className="flex flex-wrap gap-1">
                {d.constraints.map((n: string) => (
                  <span key={n} style={{ background: chipBg(), border: `1px solid ${chipBorder()}`, borderRadius: 4, padding: '1px 6px', color: chipText(), fontSize: 11 }}>{n}</span>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

function Row({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div className="flex justify-between gap-3">
      <span style={{ color: labelColor() }}>{label}</span>
      <span style={{ color: color ?? valueColor(), fontVariantNumeric: 'tabular-nums' }}>{value}</span>
    </div>
  )
}

// ─── layer layout ─────────────────────────────────────────────────────────────

function computeLayers(tasks: Array<{ name: string; dependencies: string[] }>): Record<string, number> {
  const layers: Record<string, number> = {}
  const deps: Record<string, string[]> = {}
  for (const t of tasks) deps[t.name] = t.dependencies

  function layer(name: string): number {
    if (name in layers) return layers[name]
    const d = deps[name] ?? []
    layers[name] = d.length === 0 ? 0 : Math.max(...d.map(layer)) + 1
    return layers[name]
  }

  for (const t of tasks) layer(t.name)
  return layers
}

// node type registry (stable reference — defined outside component)
const nodeTypes = { task: TaskNode }

// ─── main component ───────────────────────────────────────────────────────────

interface Props { dag: ODAGDetail }

function DAGGraphInner({ dag }: Props) {
  const statusMap = useMemo(() => {
    const m: Record<string, TaskStatus> = {}
    for (const t of dag.tasks ?? []) m[t.name] = t
    return m
  }, [dag.tasks])

  const specMap = useMemo(() => {
    const m: Record<string, { dataSize?: string; runtime?: number }> = {}
    for (const t of dag.spec.tasks) m[t.name] = { dataSize: t.dataSize, runtime: t.runtime }
    return m
  }, [dag.spec.tasks])

  // Which tasks have downstream dependents?
  const hasDownstream = useMemo(() => {
    const s = new Set<string>()
    for (const t of dag.spec.tasks) for (const d of t.dependencies) s.add(d)
    return s
  }, [dag.spec.tasks])

  const layers = useMemo(() => computeLayers(dag.spec.tasks), [dag.spec.tasks])

  // Group tasks by layer to space them vertically
  const layerGroups = useMemo(() => {
    const g: Record<number, string[]> = {}
    for (const t of dag.spec.tasks) {
      const l = layers[t.name] ?? 0
      ;(g[l] ??= []).push(t.name)
    }
    return g
  }, [dag.spec.tasks, layers])

  const NODE_W = 220
  const NODE_H = 100
  const COL_GAP = 120
  const ROW_GAP = 30

  const nodes: Node[] = useMemo(() =>
    dag.spec.tasks.map((task) => {
      const layer = layers[task.name] ?? 0
      const group = layerGroups[layer] ?? []
      const posInLayer = group.indexOf(task.name)
      const totalInLayer = group.length
      const status = statusMap[task.name]
      const spec = specMap[task.name]
      const phase = status?.phase ?? 'Pending'

      const x = layer * (NODE_W + COL_GAP)
      const totalHeight = totalInLayer * NODE_H + (totalInLayer - 1) * ROW_GAP
      const y = posInLayer * (NODE_H + ROW_GAP) - totalHeight / 2

      return {
        id: task.name,
        type: 'task',
        position: { x, y },
        data: {
          taskName: task.name,
          phase,
          state: status?.state,
          sending: status?.sending,
          nodeName: status?.node,
          startTime: status?.startTime,
          completionTime: status?.completionTime,
          dataSize: status?.dataSize,
          specRuntime: spec?.runtime,
          image: task.image,
          constraints: task.constraints?.nodeNames,
          hasDeps: task.dependencies.length > 0,
          hasDownstream: hasDownstream.has(task.name),
        } satisfies TaskNodeData,
      }
    }),
  [dag.spec.tasks, statusMap, specMap, layers, layerGroups, hasDownstream])

  const edges: Edge[] = useMemo(() =>
    dag.spec.tasks.flatMap(task =>
      task.dependencies.map(dep => {
        const depStatus  = statusMap[dep]
        const taskStatus = statusMap[task.name]
        const depPhase   = depStatus?.phase ?? 'Pending'
        const depState   = depStatus?.state

        // Transfer type: same-node if both assigned and equal
        const srcNode = depStatus?.node
        const dstNode = taskStatus?.node
        const transferType = srcNode && dstNode
          ? srcNode === dstNode ? 'same-node' : 'cross-node'
          : undefined

        // Data size from dep's output
        const sizeFmt = fmtBytes(depStatus?.dataSize)

        // Build edge label
        const labelParts = []
        if (sizeFmt) labelParts.push(sizeFmt)
        if (transferType) labelParts.push(transferType)
        const label = labelParts.join(' · ')

        // Edge colour follows the data: orange while bytes move (data plane),
        // green once installed at the consumer, red on failure, ink otherwise.
        const depSending = depStatus?.sending
        const edgeColor = depSending              ? T.data()
                        : depState === 'DataReady' ? T.ok()
                        : depPhase === 'Failed'    ? T.fail()
                        : edgeDefaultColor()

        return {
          id: `${dep}->${task.name}`,
          source: dep,
          target: task.name,
          animated: depPhase === 'Running' || !!depSending,
          label: label || undefined,
          labelStyle: {
            fill: transferType === 'cross-node' ? T.data() : T.textMuted(),
            fontSize: 10,
            fontWeight: 500,
          },
          labelBgStyle: { fill: labelBgFill(), fillOpacity: 0.85 },
          style: {
            stroke: edgeColor,
            strokeWidth: 1.5,
          },
          markerEnd: { type: 'arrowclosed' as const, color: edgeColor, width: 16, height: 16 },
        }
      })
    ),
  [dag.spec.tasks, statusMap])

  const { fitView } = useReactFlow()
  const onInit = useCallback(() => { fitView({ padding: 0.2 }) }, [fitView])

  return (
    <div style={{ height: 480 }} className="rounded overflow-hidden border border-line bg-surface">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onInit={onInit}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        minZoom={0.3}
        maxZoom={2}
        proOptions={{ hideAttribution: true }}
      >
        <Background color={gridColor()} gap={20} size={1} />
        <Controls
          showInteractive={false}
          style={{ background: controlsBg(), border: `1px solid ${controlsBorder()}` }}
        />
      </ReactFlow>
    </div>
  )
}

// Wrap with provider so useReactFlow() works inside DAGGraphInner
export default function DAGGraph({ dag }: Props) {
  return (
    <ReactFlowProvider>
      <DAGGraphInner dag={dag} />
    </ReactFlowProvider>
  )
}
