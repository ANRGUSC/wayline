interface Props {
  values: number[]
  bins?: number
}

export default function MakespanHistogram({ values, bins = 12 }: Props) {
  if (values.length === 0) return null

  const min = Math.min(...values)
  const max = Math.max(...values)
  // Avoid 0-width bins when all values equal.
  const span = max - min < 1e-9 ? 1 : max - min
  const binWidth = span / bins
  const counts = new Array(bins).fill(0) as number[]
  const edges = Array.from({ length: bins + 1 }, (_, i) => min + i * binWidth)
  for (const v of values) {
    let idx = Math.floor((v - min) / binWidth)
    if (idx >= bins) idx = bins - 1
    if (idx < 0) idx = 0
    counts[idx]++
  }
  const maxCount = Math.max(...counts)

  const W = 720, H = 180, ML = 40, MR = 10, MT = 10, MB = 32
  const innerW = W - ML - MR
  const innerH = H - MT - MB
  const barW = innerW / bins

  return (
    <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ display: 'block' }}>
      {/* Y gridlines / labels */}
      <line x1={ML} y1={MT + innerH} x2={ML + innerW} y2={MT + innerH} stroke="#9ca3af" strokeOpacity={0.4} />
      <line x1={ML} y1={MT} x2={ML} y2={MT + innerH} stroke="#9ca3af" strokeOpacity={0.4} />
      <text x={ML - 4} y={MT + 4} textAnchor="end" fontSize={10} fill="#9ca3af">{maxCount}</text>
      <text x={ML - 4} y={MT + innerH + 4} textAnchor="end" fontSize={10} fill="#9ca3af">0</text>

      {counts.map((c, i) => {
        const h = maxCount === 0 ? 0 : (c / maxCount) * innerH
        const x = ML + i * barW
        const y = MT + innerH - h
        return (
          <g key={i}>
            <rect x={x + 1} y={y} width={barW - 2} height={h} fill="#60a5fa" rx={1} />
            {c > 0 && (
              <text x={x + barW / 2} y={y - 3} textAnchor="middle" fontSize={9} fill="#9ca3af">{c}</text>
            )}
            <title>{`${edges[i].toFixed(1)}s – ${edges[i + 1].toFixed(1)}s: ${c} run${c === 1 ? '' : 's'}`}</title>
          </g>
        )
      })}

      {/* X labels — min, median, max */}
      <text x={ML} y={H - 10} fontSize={10} fill="#9ca3af">{min.toFixed(1)}s</text>
      <text x={ML + innerW / 2} y={H - 10} textAnchor="middle" fontSize={10} fill="#9ca3af">{((min + max) / 2).toFixed(1)}s</text>
      <text x={ML + innerW} y={H - 10} textAnchor="end" fontSize={10} fill="#9ca3af">{max.toFixed(1)}s</text>
    </svg>
  )
}
