/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      colors: {
        pivot: {
          blue: '#1e3a5f',
          teal: '#0e9aaa',
        },
      },
    },
  },
  plugins: [],
}
