const BASE = '/api'

async function request(path, options = {}, responseType = 'json') {
  const res = await fetch(`${BASE}${path}`, options)
  if (!res.ok) {
    const text = await res.text().catch(() => '')
    throw new Error(`API error ${res.status}: ${text}`)
  }
  return responseType === 'text' ? res.text() : res.json()
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

  // SQL generation
  getSqlgenDefaults: (client) =>
    request(`/sqlgen/defaults/${encodeURIComponent(client)}`),
  generateSql: (body) =>
    request('/sqlgen/generate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    }),
  downloadSqlFile: (client, filename) =>
    `${BASE}/sqlgen/download/${encodeURIComponent(client)}/${encodeURIComponent(filename)}`,
  previewSqlFile: (client, filename) =>
    request(`/sqlgen/preview/${encodeURIComponent(client)}/${encodeURIComponent(filename)}`, {}, 'text'),
}
