/**
 * TemplateGraph: static DAG visualization for ODAG templates.
 *
 * Shows the task dependency graph from the template spec. No animation,
 * no phase coloring — just the structure with hover details.
 */

import { useMemo } from 'react'
import {
  ReactFlow,
  Node,
  Edge,
  Background,
  Controls,
  Position,
  NodeProps,
  Handle,
  ReactFlowProvider,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'

// ─── types ──────────────────────────────────────────────────────────────────

export interface TemplateTask {
  name: string
  image: string
  dependencies: string[]
  dataSize?: string
  runtime?: number
  resources?: { cpu?: string; memory?: string }
  constraints?: { nodeNames?: string[] }
}

export interface TemplateGraphProps {
  tasks: TemplateTask[]
}

// ─── colour helpers ─────────────────────────────────────────────────────────

function isDark() {
  return document.documentElement.classList.contains('dark')
}

const neutral = {
  border: () => isDark() ? '#6b7280' : '#9ca3af',
  bg: () => isDark() ? '#1f2937' : '#f9fafb',
  text: () => isDark() ? '#e5e7eb' : '#1f2937',
  label: () => isDark() ? '#9ca3af' : '#6b7280',
  value: () => isDark() ? '#d1d5db' : '#374151',
  tooltipBg: () => isDark() ? '#111827' : '#ffffff',
  tooltipShadow: () => isDark() ? '0 4px 24px rgba(0,0,0,0.6)' : '0 4px 24px rgba(0,0,0,0.15)',
  chipBg: () => isDark() ? '#374151' : '#e5e7eb',
  chipBorder: () => isDark() ? '#4b5563' : '#d1d5db',
  chipText: () => isDark() ? '#d1d5db' : '#374151',
  edgeColor: () => isDark() ? '#4b5563' : '#d1d5db',
  accent: () => isDark() ? '#60a5fa' : '#3b82f6',
}

// ─── custom node ────────────────────────────────────────────────────────────

interface TemplateNodeData {
  taskName: string
  image?: string
  dataSize?: string
  runtime?: number
  resources?: { cpu?: string; memory?: string }
  constraints?: string[]
  hasDeps: boolean
  hasDownstream: boolean
  [key: string]: unknown
}

function TemplateNode({ data }: NodeProps) {
  const d = data as TemplateNodeData

  return (
    <div
      className="relative group"
      style={{
        background: neutral.bg(),
        border: `2px solid ${neutral.border()}`,
        borderRadius: 10,
        minWidth: 175,
        padding: '10px 14px',
        color: neutral.text(),
        cursor: 'default',
      }}
    >
      {d.hasDeps       && <Handle type="target" position={Position.Left}  style={{ background: neutral.border(), border: 'none', width: 10, height: 10 }} />}
      {d.hasDownstream && <Handle type="source" position={Position.Right} style={{ background: neutral.border(), border: 'none', width: 10, height: 10 }} />}

      {/* Task name */}
      <div className="font-semibold text-sm mb-1 truncate" style={{ maxWidth: 195, color: neutral.text() }}>
        {d.taskName}
      </div>

      {/* Meta line */}
      <div className="flex items-center gap-2 text-xs" style={{ color: neutral.label() }}>
        {d.runtime != null && (
          <span>{d.runtime}s</span>
        )}
        {d.dataSize && d.dataSize !== '0' && (
          <span style={{ color: '#67e8f9' }}>{d.dataSize}</span>
        )}
      </div>

      {/* Constraints */}
      {d.constraints && d.constraints.length > 0 && (
        <div className="text-xs mt-1" style={{ color: neutral.accent() }}>
          {'↦ '}{d.constraints.join(', ')}
        </div>
      )}

      {/* Hover tooltip */}
      <div
        className="absolute left-1/2 z-50 pointer-events-none opacity-0 group-hover:opacity-100 transition-opacity duration-150"
        style={{
          bottom: 'calc(100% + 10px)',
          transform: 'translateX(-50%)',
          minWidth: 220,
          background: neutral.tooltipBg(),
          border: `1px solid ${neutral.border()}`,
          borderRadius: 8,
          padding: '10px 14px',
          boxShadow: neutral.tooltipShadow(),
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
            width: 12, height: 12, background: neutral.border(),
            transform: 'rotate(45deg)', transformOrigin: 'top left',
            marginTop: -6,
          }} />
        </div>

        <div className="text-xs space-y-1.5">
          <div className="font-semibold text-sm mb-2" style={{ color: neutral.text() }}>{d.taskName}</div>

          {d.image && (
            <TipRow label="Image" value={d.image.split('/').pop() ?? d.image} />
          )}

          {d.runtime != null && (
            <TipRow label="Runtime hint" value={`${d.runtime}s`} />
          )}
          {d.dataSize && d.dataSize !== '0' && (
            <TipRow label="Data size" value={d.dataSize} color="#67e8f9" />
          )}

          {d.resources && (d.resources.cpu || d.resources.memory) && (
            <div className="border-t border-line pt-1.5 mt-1.5 space-y-1">
              {d.resources.cpu && <TipRow label="CPU" value={d.resources.cpu} />}
              {d.resources.memory && <TipRow label="Memory" value={d.resources.memory} />}
            </div>
          )}

          {d.constraints && d.constraints.length > 0 && (
            <div className="border-t border-line pt-1.5 mt-1.5">
              <div style={{ color: neutral.label() }} className="mb-1">allowed nodes:</div>
              <div className="flex flex-wrap gap-1">
                {d.constraints.map(n => (
                  <span key={n} style={{
                    background: neutral.chipBg(),
                    border: `1px solid ${neutral.chipBorder()}`,
                    borderRadius: 4,
                    padding: '1px 6px',
                    color: neutral.chipText(),
                    fontSize: 11,
                  }}>{n}</span>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

function TipRow({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div className="flex justify-between gap-3">
      <span style={{ color: neutral.label() }}>{label}</span>
      <span style={{ color: color ?? neutral.value(), fontVariantNumeric: 'tabular-nums' }}>{value}</span>
    </div>
  )
}

// ─── layer layout ───────────────────────────────────────────────────────────

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

// ─── node type registry ─────────────────────────────────────────────────────
const nodeTypes = { task: TemplateNode }

// ─── main component ─────────────────────────────────────────────────────────

function TemplateGraphInner({ tasks }: TemplateGraphProps) {
  const hasDownstream = useMemo(() => {
    const s = new Set<string>()
    for (const t of tasks) for (const d of t.dependencies) s.add(d)
    return s
  }, [tasks])

  const layers = useMemo(() => computeLayers(tasks), [tasks])

  const layerGroups = useMemo(() => {
    const g: Record<number, string[]> = {}
    for (const t of tasks) {
      const l = layers[t.name] ?? 0
      ;(g[l] ??= []).push(t.name)
    }
    return g
  }, [tasks, layers])

  const NODE_W = 220
  const NODE_H = 100
  const COL_GAP = 120
  const ROW_GAP = 30

  const nodes: Node[] = useMemo(() =>
    tasks.map((task) => {
      const layer = layers[task.name] ?? 0
      const group = layerGroups[layer] ?? []
      const posInLayer = group.indexOf(task.name)
      const totalInLayer = group.length
      const x = layer * (NODE_W + COL_GAP)
      const totalHeight = totalInLayer * NODE_H + (totalInLayer - 1) * ROW_GAP
      const y = posInLayer * (NODE_H + ROW_GAP) - totalHeight / 2

      return {
        id: task.name,
        type: 'task',
        position: { x, y },
        data: {
          taskName: task.name,
          image: task.image,
          dataSize: task.dataSize,
          runtime: task.runtime,
          resources: task.resources,
          constraints: task.constraints?.nodeNames,
          hasDeps: task.dependencies.length > 0,
          hasDownstream: hasDownstream.has(task.name),
        } satisfies TemplateNodeData,
      }
    }),
  [tasks, layers, layerGroups, hasDownstream])

  const edges: Edge[] = useMemo(() =>
    tasks.flatMap(task =>
      task.dependencies.map(dep => ({
        id: `${dep}->${task.name}`,
        source: dep,
        target: task.name,
        animated: false,
        style: { stroke: neutral.edgeColor(), strokeWidth: 2 },
      }))
    ),
  [tasks])

  return (
    <div style={{ height: 480, border: `1px solid ${neutral.border()}`, borderRadius: 8 }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        fitView
        minZoom={0.3}
        maxZoom={2}
        proOptions={{ hideAttribution: true }}
        nodesDraggable={false}
        nodesConnectable={false}
      >
        <Background gap={20} size={1} />
        <Controls showInteractive={false} />
      </ReactFlow>
    </div>
  )
}

export default function TemplateGraph(props: TemplateGraphProps) {
  return (
    <ReactFlowProvider>
      <TemplateGraphInner {...props} />
    </ReactFlowProvider>
  )
}
