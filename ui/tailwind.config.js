/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  darkMode: 'class',
  theme: {
    extend: {
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
        accent: {
          DEFAULT: 'var(--accent)',
          hover: 'var(--accent-hover)',
        },
      },
    },
  },
  plugins: [],
}
