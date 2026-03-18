/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      colors: {
        pivot: {
          blue: '#1e3a5f',
          teal: '#0e9aaa',
          dark: '#0f172a',
          surface: '#1e293b',
          surfaceHover: '#334155',
          border: '#334155',
          textPrimary: '#f1f5f9',
          textSecondary: '#94a3b8',
          textMuted: '#64748b',
        },
      },
    },
  },
  plugins: [],
}
