/**
 * Colour access for SVG and inline styles, resolved from the CSS tokens in
 * index.css so light and dark stay in one place. Components call these at
 * render time; the theme toggle re-renders the tree, so the values follow.
 */

export function token(name: string): string {
  if (typeof window === 'undefined') return '#000000'
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim() || '#000000'
}

export function isDark(): boolean {
  return document.documentElement.classList.contains('dark')
}

// ─── neutrals ────────────────────────────────────────────────────────────────
export const ink = () => token('--ink')
export const surface = () => token('--surface')
export const surfaceAlt = () => token('--surface-alt')
export const surfaceCard = () => token('--surface-card')
export const textPrimary = () => token('--on-surface')
export const textSecondary = () => token('--on-surface-secondary')
export const textMuted = () => token('--on-surface-muted')
export const textFaint = () => token('--on-surface-faint')
export const line = () => token('--line')
export const lineSoft = () => token('--line-soft')

// ─── planes and statuses ─────────────────────────────────────────────────────
export const ctrl = () => token('--ctrl')
export const ctrlTint = () => token('--ctrl-tint')
export const data = () => token('--data')
export const dataTint = () => token('--data-tint')
export const run = () => token('--run')
export const runTint = () => token('--run-tint')
export const ok = () => token('--ok')
export const okTint = () => token('--ok-tint')
export const fail = () => token('--fail')
export const failTint = () => token('--fail-tint')
export const alt = () => token('--alt')
export const pending = () => token('--pending')
export const pendingTint = () => token('--pending-tint')

// ─── charts (Gantt, histogram, line charts) ──────────────────────────────────
export const rowEven = () => token('--chart-row-even')
export const rowOdd = () => token('--chart-row-odd')
export const gridStroke = () => token('--chart-grid')
export const axisStroke = () => token('--chart-axis')
export const labelFill = () => token('--chart-label')
export const tickFill = () => token('--chart-tick')
export const barText = () => token('--chart-bar-text')
export const legendFill = () => token('--chart-label')
export const dashStroke = () => token('--chart-dash')
export const sepStroke = () => token('--chart-axis')

/**
 * Categorical series colours, assigned in fixed order. Navy and orange are
 * the two planes; the rest is the paper's plot palette and a few
 * complements chosen to stay apart under colour-vision deficiency.
 */
export const SERIES = [
  '#1f4e79', // navy (control plane)
  '#c55a11', // orange (data plane)
  '#1a9850', // green
  '#762a83', // purple
  '#2166ac', // blue
  '#b8860b', // gold
  '#2a9d8f', // teal
  '#b2182b', // red
]

export function seriesColor(index: number): string {
  return SERIES[((index % SERIES.length) + SERIES.length) % SERIES.length]
}

export function taskColor(name: string, names: string[]): string {
  return seriesColor(Math.max(names.indexOf(name), 0))
}

// ─── task phase and agent state ──────────────────────────────────────────────
export function phaseColor(phase: string): string {
  switch (phase) {
    case 'Running':    return run()
    case 'Scheduling': return ctrl()
    case 'Succeeded':  return ok()
    case 'Failed':     return fail()
    case 'Degraded':   return data()
    default:           return pending()
  }
}

export function phaseTint(phase: string): string {
  switch (phase) {
    case 'Running':    return runTint()
    case 'Scheduling': return ctrlTint()
    case 'Succeeded':  return okTint()
    case 'Failed':     return failTint()
    case 'Degraded':   return dataTint()
    default:           return surface()
  }
}

/** Data-agent state: executing is control-plane blue, moving bytes is data-plane orange. */
export function stateColor(state: string): string {
  switch (state) {
    case 'Executing': return run()
    case 'Sending':   return data()
    case 'DataReady': return ok()
    case 'Done':      return ok()
    case 'Failed':    return fail()
    default:          return textMuted()
  }
}
