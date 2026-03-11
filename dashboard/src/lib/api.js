const BASE = '/api'

async function request(path, options = {}) {
  const res = await fetch(`${BASE}${path}`, options)
  if (!res.ok) {
    const text = await res.text().catch(() => '')
    throw new Error(`API error ${res.status}: ${text}`)
  }
  return res.json()
}

export const api = {
  listClients: () => request('/clients'),
  getFindings: (client) => request(`/clients/${encodeURIComponent(client)}`),
  downloadReport: (client) =>
    `${BASE}/clients/${encodeURIComponent(client)}/report`,
  startRun: (body) =>
    request('/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    }),
}
