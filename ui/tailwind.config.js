/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  darkMode: 'class',
  theme: {
    extend: {
      fontFamily: {
        sans: ['Arial', 'Helvetica', 'Liberation Sans', 'sans-serif'],
        mono: ['Menlo', 'DejaVu Sans Mono', 'Consolas', 'monospace'],
      },
      colors: {
        surface: {
          DEFAULT: 'var(--surface)',
          alt: 'var(--surface-alt)',
          card: 'var(--surface-card)',
        },
        on: {
          DEFAULT: 'var(--on-surface)',
          secondary: 'var(--on-surface-secondary)',
          muted: 'var(--on-surface-muted)',
          faint: 'var(--on-surface-faint)',
        },
        line: {
          DEFAULT: 'var(--line)',
          soft: 'var(--line-soft)',
        },
        ink: 'var(--ink)',
        accent: {
          DEFAULT: 'var(--accent)',
          hover: 'var(--accent-hover)',
        },
        // the two planes
        ctrl: { DEFAULT: 'var(--ctrl)', hover: 'var(--ctrl-hover)', tint: 'var(--ctrl-tint)' },
        data: { DEFAULT: 'var(--data)', hover: 'var(--data-hover)', tint: 'var(--data-tint)' },
        // statuses
        run:     { DEFAULT: 'var(--run)',     tint: 'var(--run-tint)' },
        ok:      { DEFAULT: 'var(--ok)',      tint: 'var(--ok-tint)' },
        fail:    { DEFAULT: 'var(--fail)',    tint: 'var(--fail-tint)' },
        alt:     { DEFAULT: 'var(--alt)',     tint: 'var(--alt-tint)' },
        pending: { DEFAULT: 'var(--pending)', tint: 'var(--pending-tint)' },
      },
    },
  },
  plugins: [],
}
