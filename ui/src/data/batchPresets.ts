import type { BatchODAGEntry } from '@/api/client'

const IMAGE = '192.168.1.163:5000/multi-odag-task:latest'
const CMD = ['python', 'task.py']

interface T {
  name: string
  deps: string[]
  runtime: number
  dataSize: string
  cpu: string
  mem: string
  nodes?: string[]
}

function task(t: T) {
  const base: Record<string, unknown> = {
    name: t.name,
    image: IMAGE,
    command: CMD,
    dependencies: t.deps,
    resources: { cpu: t.cpu, memory: t.mem },
    dataSize: t.dataSize,
    runtime: t.runtime,
  }
  if (t.nodes) base.constraints = { nodeNames: t.nodes }
  return base
}

export interface BatchPreset {
  name: string
  delay: number
  description: string
  topology: string
  taskCount: number
  spec: Record<string, unknown>
}

export const BATCH_PRESETS: BatchPreset[] = [
  {
    name: 'video-transcode',
    delay: 0,
    description: 'Video processing pipeline',
    topology: 'Linear (4)',
    taskCount: 4,
    spec: {
      scheduler: 'heft',
      retryPolicy: { maxRetries: 2 },
      tasks: [
        task({ name: 'ingest',  deps: [],          runtime: 5,  dataSize: '200MB', cpu: '200m', mem: '512Mi', nodes: ['anrg-3','anrg-4'] }),
        task({ name: 'decode',  deps: ['ingest'],   runtime: 8,  dataSize: '500MB', cpu: '400m', mem: '1Gi',   nodes: ['anrg-3','anrg-5'] }),
        task({ name: 'encode',  deps: ['decode'],   runtime: 15, dataSize: '300MB', cpu: '500m', mem: '1Gi',   nodes: ['anrg-4','anrg-6'] }),
        task({ name: 'package', deps: ['encode'],   runtime: 3,  dataSize: '50MB',  cpu: '100m', mem: '512Mi' }),
      ],
    },
  },
  {
    name: 'ml-training',
    delay: 15,
    description: 'ML training workflow',
    topology: 'Diamond (5)',
    taskCount: 5,
    spec: {
      scheduler: 'heft',
      retryPolicy: { maxRetries: 2 },
      tasks: [
        task({ name: 'fetch-data',  deps: [],                          runtime: 4,  dataSize: '100MB', cpu: '200m', mem: '256Mi', nodes: ['anrg-3','anrg-4'] }),
        task({ name: 'preprocess',  deps: ['fetch-data'],              runtime: 6,  dataSize: '200MB', cpu: '300m', mem: '512Mi', nodes: ['anrg-3','anrg-5'] }),
        task({ name: 'augment',     deps: ['fetch-data'],              runtime: 8,  dataSize: '150MB', cpu: '300m', mem: '512Mi', nodes: ['anrg-4','anrg-6'] }),
        task({ name: 'train',       deps: ['preprocess','augment'],    runtime: 20, dataSize: '50MB',  cpu: '500m', mem: '640Mi', nodes: ['anrg-5','anrg-6'] }),
        task({ name: 'evaluate',    deps: ['train'],                   runtime: 5,  dataSize: '10MB',  cpu: '200m', mem: '256Mi', nodes: ['anrg-5','anrg-6'] }),
      ],
    },
  },
  {
    name: 'etl-wide',
    delay: 15,
    description: 'ETL fan-out / fan-in',
    topology: 'Wide (5)',
    taskCount: 5,
    spec: {
      scheduler: 'heft',
      retryPolicy: { maxRetries: 2 },
      tasks: [
        task({ name: 'extract', deps: [],                                runtime: 3, dataSize: '300MB', cpu: '200m', mem: '512Mi', nodes: ['anrg-3'] }),
        task({ name: 'clean-a', deps: ['extract'],                      runtime: 5, dataSize: '100MB', cpu: '200m', mem: '640Mi', nodes: ['anrg-3','anrg-4'] }),
        task({ name: 'clean-b', deps: ['extract'],                      runtime: 7, dataSize: '100MB', cpu: '200m', mem: '640Mi', nodes: ['anrg-5','anrg-6'] }),
        task({ name: 'clean-c', deps: ['extract'],                      runtime: 4, dataSize: '100MB', cpu: '200m', mem: '640Mi', nodes: ['anrg-4','anrg-6'] }),
        task({ name: 'load',    deps: ['clean-a','clean-b','clean-c'],  runtime: 6, dataSize: '10MB',  cpu: '300m', mem: '512Mi', nodes: ['anrg-3','anrg-5'] }),
      ],
    },
  },
  {
    name: 'sensor-fusion',
    delay: 15,
    description: 'Multi-sensor fusion pipeline',
    topology: 'Complex (7)',
    taskCount: 7,
    spec: {
      scheduler: 'heft',
      retryPolicy: { maxRetries: 2 },
      tasks: [
        task({ name: 'sensor-1', deps: [],                       runtime: 3,  dataSize: '50MB',  cpu: '200m', mem: '256Mi', nodes: ['anrg-3'] }),
        task({ name: 'sensor-2', deps: [],                       runtime: 4,  dataSize: '80MB',  cpu: '200m', mem: '256Mi', nodes: ['anrg-4'] }),
        task({ name: 'sensor-3', deps: [],                       runtime: 2,  dataSize: '60MB',  cpu: '100m', mem: '256Mi', nodes: ['anrg-5'] }),
        task({ name: 'fuse-ab',  deps: ['sensor-1','sensor-2'],  runtime: 10, dataSize: '200MB', cpu: '400m', mem: '512Mi', nodes: ['anrg-3','anrg-4'] }),
        task({ name: 'fuse-bc',  deps: ['sensor-2','sensor-3'],  runtime: 8,  dataSize: '150MB', cpu: '400m', mem: '512Mi', nodes: ['anrg-4','anrg-5'] }),
        task({ name: 'analyze',  deps: ['fuse-ab','fuse-bc'],    runtime: 12, dataSize: '100MB', cpu: '500m', mem: '640Mi', nodes: ['anrg-5','anrg-6'] }),
        task({ name: 'report',   deps: ['analyze'],              runtime: 3,  dataSize: '5MB',   cpu: '100m', mem: '256Mi', nodes: ['anrg-6'] }),
      ],
    },
  },
  {
    name: 'image-batch',
    delay: 15,
    description: 'Batch image processing',
    topology: 'Deep chain (5)',
    taskCount: 5,
    spec: {
      scheduler: 'heft',
      retryPolicy: { maxRetries: 2 },
      tasks: [
        task({ name: 'download',  deps: [],              runtime: 4, dataSize: '500MB', cpu: '200m', mem: '768Mi', nodes: ['anrg-3','anrg-4'] }),
        task({ name: 'resize',    deps: ['download'],    runtime: 6, dataSize: '200MB', cpu: '300m', mem: '1Gi',   nodes: ['anrg-4','anrg-5'] }),
        task({ name: 'filter',    deps: ['resize'],      runtime: 8, dataSize: '200MB', cpu: '400m', mem: '640Mi', nodes: ['anrg-5','anrg-6'] }),
        task({ name: 'watermark', deps: ['filter'],      runtime: 5, dataSize: '200MB', cpu: '200m', mem: '640Mi', nodes: ['anrg-5','anrg-6'] }),
        task({ name: 'upload',    deps: ['watermark'],   runtime: 3, dataSize: '0',     cpu: '100m', mem: '384Mi', nodes: ['anrg-6'] }),
      ],
    },
  },
]

export function toBatchEntries(): BatchODAGEntry[] {
  return BATCH_PRESETS.map(p => ({
    name: p.name,
    delay: p.delay,
    spec: p.spec,
  }))
}

export const ODAG_COLORS = [
  '#60a5fa', // blue   - video-transcode
  '#34d399', // green  - ml-training
  '#f59e0b', // amber  - etl-wide
  '#f87171', // red    - sensor-fusion
  '#a78bfa', // purple - image-batch
]
