const BASE_URL = 'http://localhost:8000'

async function request(method, path, body) {
  const opts = { method, headers: {} }
  if (body !== undefined) {
    opts.headers['Content-Type'] = 'application/json'
    opts.body = JSON.stringify(body)
  }
  const res = await fetch(`${BASE_URL}${path}`, opts)
  if (res.status === 204) return null
  const data = await res.json().catch(() => null)
  if (!res.ok) {
    throw new Error(data?.detail || `HTTP ${res.status}`)
  }
  return data
}

export const getPins = () => request('GET', '/api/pins')
export const addPin = (url) => request('POST', '/api/pins', { url })
export const deletePin = (id) => request('DELETE', `/api/pins/${id}`)

export const getMetricsSummary = () => request('GET', '/api/metrics/summary')
export const getPinMetrics = (id) => request('GET', `/api/pins/${id}/metrics`)

export const getKeywords = () => request('GET', '/api/keywords')

export const getAB = () => request('GET', '/api/ab')
export const createAB = (data) => request('POST', '/api/ab', data)

export const triggerRun = () => request('POST', '/api/run')
export const getRunStatus = () => request('GET', '/api/run/status')
