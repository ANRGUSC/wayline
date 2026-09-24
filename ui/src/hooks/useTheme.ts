import { useState, useEffect } from 'react'

type Theme = 'light' | 'dark'

const STORAGE_KEY = 'wayline-theme'

function getInitial(): Theme {
  if (typeof window === 'undefined') return 'light'
  // ?theme=dark|light overrides the stored choice (useful for links and captures)
  const fromUrl = new URLSearchParams(window.location.search).get('theme') as Theme | null
  if (fromUrl === 'light' || fromUrl === 'dark') return fromUrl
  const stored = localStorage.getItem(STORAGE_KEY) as Theme | null
  if (stored === 'light' || stored === 'dark') return stored
  return 'light'
}

export function useTheme() {
  const [theme, setTheme] = useState<Theme>(getInitial)

  useEffect(() => {
    const root = document.documentElement
    if (theme === 'dark') {
      root.classList.add('dark')
    } else {
      root.classList.remove('dark')
    }
    localStorage.setItem(STORAGE_KEY, theme)
  }, [theme])

  const toggle = () => setTheme(t => (t === 'dark' ? 'light' : 'dark'))

  return { theme, toggle }
}
