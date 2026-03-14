import { useState, useEffect, useCallback, useRef } from 'react'
import {
  LineChart, Line,
  BarChart, Bar,
  XAxis, YAxis, CartesianGrid,
  Tooltip, Legend,
  ResponsiveContainer, Cell, LabelList,
} from 'recharts'
import * as api from './api.js'

const CHART_COLORS = ['#e60023', '#4f8ef7', '#00c896', '#f5a623', '#a78bfa']

// ── Skeleton ──────────────────────────────────────────────────────────────────
function Skeleton({ className = '' }) {
  return <div className={`animate-pulse rounded bg-[#2a2a2a] ${className}`} />
}

// ── Toast ─────────────────────────────────────────────────────────────────────
function Toast({ message, type, onClose }) {
  useEffect(() => {
    const t = setTimeout(onClose, 3500)
    return () => clearTimeout(t)
  }, [onClose])
  const bg = type === 'error' ? 'bg-red-900 border-red-700' : 'bg-green-900 border-green-700'
  return (
    <div className={`fixed bottom-5 right-5 z-50 px-4 py-3 rounded border text-[#f5f5f5] text-sm shadow-xl ${bg}`}>
      {message}
    </div>
  )
}

// ── Header ────────────────────────────────────────────────────────────────────
function Header({ runStatus, onRunNow }) {
  const isRunning = runStatus?.status === 'running'
  const isError   = runStatus?.status === 'error'
  const lastRun   = runStatus?.last_run
    ? new Date(runStatus.last_run).toLocaleTimeString()
    : null

  return (
    <header className="sticky top-0 z-40 bg-[#0f0f0f] border-b border-[#2a2a2a] px-6 py-3 flex items-center justify-between">
      <span className="text-white font-bold text-lg">Pinterest SEO</span>

      <div className="flex items-center gap-4">
        <div className="flex items-center gap-2 text-sm">
          {isRunning ? (
            <>
              <span className="w-2.5 h-2.5 rounded-full bg-yellow-400 animate-pulse inline-block" />
              <span className="text-[#888]">Running…</span>
            </>
          ) : isError ? (
            <>
              <span className="w-2.5 h-2.5 rounded-full bg-red-500 inline-block" />
              <span className="text-red-400">Failed</span>
            </>
          ) : lastRun ? (
            <>
              <span className="w-2.5 h-2.5 rounded-full bg-green-500 inline-block" />
              <span className="text-[#888]">Last run: {lastRun}</span>
            </>
          ) : (
            <>
              <span className="w-2.5 h-2.5 rounded-full bg-[#444] inline-block" />
              <span className="text-[#888]">Never run</span>
            </>
          )}
        </div>

        <button
          onClick={onRunNow}
          disabled={isRunning}
          className="bg-[#e60023] text-white px-4 py-1.5 rounded text-sm font-medium
                     hover:bg-[#c4001f] disabled:opacity-50 disabled:cursor-not-allowed
                     transition-colors flex items-center gap-2"
        >
          {isRunning && (
            <svg className="animate-spin w-4 h-4" viewBox="0 0 24 24" fill="none">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
            </svg>
          )}
          Run Now
        </button>
      </div>
    </header>
  )
}

// ── Stat Cards ────────────────────────────────────────────────────────────────
function StatCards({ summary, loading }) {
  const cards = [
    { label: 'Total Pins',         value: summary?.total_pins ?? '—' },
    { label: 'Avg Engagement Rate',
      value: summary?.avg_engagement_rate != null
        ? `${(summary.avg_engagement_rate * 100).toFixed(2)}%`
        : '—' },
    { label: 'Total Impressions',  value: summary?.total_impressions?.toLocaleString() ?? '—' },
    { label: 'Total Saves',        value: summary?.total_saves?.toLocaleString() ?? '—' },
  ]

  return (
    <div className="mb-6 space-y-3">
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        {cards.map(({ label, value }) => (
          <div key={label} className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4">
            <div className="text-[#888] text-xs mb-1">{label}</div>
            {loading
              ? <Skeleton className="h-8 w-20 mt-1" />
              : <div className="text-[#f5f5f5] text-2xl font-bold">{value}</div>}
          </div>
        ))}
      </div>

      {summary?.best_pin && (
        <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4">
          <div className="text-[#888] text-xs mb-1">Best Pin</div>
          <div className="text-[#f5f5f5] font-semibold truncate">
            {(summary.best_pin.title || summary.best_pin.pin_id).slice(0, 30)}
          </div>
        </div>
      )}
    </div>
  )
}

// ── Pin Manager ───────────────────────────────────────────────────────────────
function PinManager({ pins, loading, onRefresh, showToast }) {
  const [open, setOpen]       = useState(false)
  const [url, setUrl]         = useState('')
  const [adding, setAdding]   = useState(false)

  const handleAdd = async () => {
    if (!url.trim()) return
    setAdding(true)
    try {
      await api.addPin(url.trim())
      setUrl('')
      onRefresh()
      showToast('Pin added', 'success')
    } catch (e) {
      showToast(e.message, 'error')
    } finally {
      setAdding(false)
    }
  }

  const handleDelete = async (pinId) => {
    try {
      await api.deletePin(pinId)
      onRefresh()
      showToast('Pin removed', 'success')
    } catch (e) {
      showToast(e.message, 'error')
    }
  }

  return (
    <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg mb-6 overflow-hidden">
      <button
        onClick={() => setOpen(o => !o)}
        className="w-full flex items-center justify-between px-4 py-3 text-[#f5f5f5] hover:bg-[#222] transition-colors"
      >
        <span className="font-semibold">
          Pin Manager {pins != null ? `(${pins.length})` : ''}
        </span>
        <svg
          className={`w-5 h-5 transition-transform ${open ? 'rotate-180' : ''}`}
          viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"
        >
          <path d="M19 9l-7 7-7-7" />
        </svg>
      </button>

      {open && (
        <div className="border-t border-[#2a2a2a] p-4">
          {loading ? (
            <Skeleton className="h-20 w-full mb-4" />
          ) : (
            <div className="space-y-1 mb-4 max-h-72 overflow-y-auto">
              {pins?.length === 0 && (
                <p className="text-[#888] text-sm text-center py-6">No pins tracked yet</p>
              )}
              {pins?.map(pin => (
                <div
                  key={pin.pin_id}
                  className="flex items-center gap-3 py-2 border-b border-[#2a2a2a] last:border-0"
                >
                  {pin.image_url ? (
                    <img
                      src={pin.image_url}
                      alt=""
                      className="w-10 h-10 rounded object-cover shrink-0"
                      onError={e => { e.target.style.display = 'none' }}
                    />
                  ) : (
                    <div className="w-10 h-10 rounded bg-[#2a2a2a] shrink-0" />
                  )}
                  <div className="flex-1 min-w-0">
                    <div className="text-[#f5f5f5] text-sm font-medium truncate">
                      {pin.title || pin.pin_id}
                    </div>
                    <div className="text-[#888] text-xs truncate">{pin.url}</div>
                  </div>
                  <button
                    onClick={() => handleDelete(pin.pin_id)}
                    className="text-[#e60023] hover:text-red-400 px-2 py-1 text-lg leading-none shrink-0"
                    title="Remove pin"
                  >
                    ×
                  </button>
                </div>
              ))}
            </div>
          )}

          <div className="flex gap-2">
            <input
              type="text"
              value={url}
              onChange={e => setUrl(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && handleAdd()}
              placeholder="https://www.pinterest.com/pin/..."
              className="flex-1 bg-[#0f0f0f] border border-[#2a2a2a] rounded px-3 py-2
                         text-[#f5f5f5] text-sm placeholder-[#555]
                         focus:outline-none focus:border-[#e60023]"
            />
            <button
              onClick={handleAdd}
              disabled={adding}
              className="bg-[#e60023] text-white px-4 py-2 rounded text-sm font-medium
                         hover:bg-[#c4001f] disabled:opacity-50 transition-colors"
            >
              {adding ? '…' : 'Add Pin'}
            </button>
          </div>
        </div>
      )}
    </div>
  )
}

// ── Engagement Chart ──────────────────────────────────────────────────────────
function EngagementChart({ pins, allMetrics, loading }) {
  const dateMap = {}
  pins.forEach(pin => {
    const metrics = allMetrics[pin.pin_id] || []
    metrics.forEach(m => {
      const date = (m.scraped_at || '').slice(0, 10)
      if (!date) return
      if (!dateMap[date]) dateMap[date] = { date }
      dateMap[date][pin.pin_id] = m.engagement_rate
    })
  })

  const data = Object.values(dateMap)
    .sort((a, b) => a.date.localeCompare(b.date))
    .map(d => ({
      ...d,
      label: new Date(d.date + 'T12:00:00Z').toLocaleDateString('en-US', {
        month: 'short', day: '2-digit',
      }),
    }))

  return (
    <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4 mb-6">
      <h2 className="text-[#f5f5f5] font-semibold mb-4">Engagement Rate Over Time</h2>
      {loading ? (
        <Skeleton className="h-64 w-full" />
      ) : data.length === 0 ? (
        <p className="text-[#888] text-sm text-center py-16">No metrics data yet — run the pipeline first</p>
      ) : (
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={data}>
            <CartesianGrid stroke="#2a2a2a" strokeDasharray="3 3" />
            <XAxis dataKey="label" tick={{ fill: '#888', fontSize: 11 }} />
            <YAxis
              tickFormatter={v => `${(v * 100).toFixed(1)}%`}
              tick={{ fill: '#888', fontSize: 11 }}
            />
            <Tooltip
              contentStyle={{ background: '#1a1a1a', border: '1px solid #2a2a2a', borderRadius: 4 }}
              labelStyle={{ color: '#f5f5f5', marginBottom: 4 }}
              formatter={(value, name) => {
                const pin = pins.find(p => p.pin_id === name)
                return [`${(value * 100).toFixed(2)}%`, pin?.title || name]
              }}
            />
            <Legend
              formatter={name => {
                const p = pins.find(p => p.pin_id === name)
                return p?.title || name
              }}
              wrapperStyle={{ color: '#888', fontSize: 12, paddingTop: 12 }}
            />
            {pins.map((pin, i) => (
              <Line
                key={pin.pin_id}
                type="monotone"
                dataKey={pin.pin_id}
                stroke={CHART_COLORS[i % CHART_COLORS.length]}
                dot={false}
                strokeWidth={2}
                connectNulls
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      )}
    </div>
  )
}

// ── Keyword Chart ─────────────────────────────────────────────────────────────
function KeywordChart({ keywords, pins, loading }) {
  const pinColorMap = {}
  pins.forEach((p, i) => { pinColorMap[p.pin_id] = CHART_COLORS[i % CHART_COLORS.length] })

  const data = (keywords || []).map(k => ({
    keyword: k.keyword,
    health: k.health,
    pin_title: k.pin_title || k.pin_id,
    pin_id: k.pin_id,
  }))

  const chartHeight = Math.max(260, data.length * 28)

  return (
    <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4 mb-6">
      <h2 className="text-[#f5f5f5] font-semibold mb-4">Keyword Health — Top 15</h2>
      {loading ? (
        <Skeleton className="h-64 w-full" />
      ) : data.length === 0 ? (
        <p className="text-[#888] text-sm text-center py-16">No keyword data yet</p>
      ) : (
        <ResponsiveContainer width="100%" height={chartHeight}>
          <BarChart data={data} layout="vertical" margin={{ left: 10, right: 30, bottom: 20 }}>
            <CartesianGrid stroke="#2a2a2a" strokeDasharray="3 3" horizontal={false} />
            <XAxis
              type="number"
              tick={{ fill: '#888', fontSize: 11 }}
              label={{
                value: 'Health score (TF-IDF × trend volume)',
                position: 'insideBottom',
                offset: -12,
                fill: '#888',
                fontSize: 11,
              }}
            />
            <YAxis
              dataKey="keyword"
              type="category"
              tick={{ fill: '#888', fontSize: 11 }}
              width={110}
            />
            <Tooltip
              contentStyle={{ background: '#1a1a1a', border: '1px solid #2a2a2a', borderRadius: 4 }}
              labelStyle={{ color: '#f5f5f5', marginBottom: 4 }}
              formatter={(value, _name, props) => [
                value.toFixed(4),
                props.payload.pin_title,
              ]}
            />
            <Bar dataKey="health">
              {data.map((entry, i) => (
                <Cell key={i} fill={pinColorMap[entry.pin_id] || '#e60023'} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      )}
    </div>
  )
}

// ── A/B Results ───────────────────────────────────────────────────────────────
function ABResults({ abData, loading }) {
  if (loading) {
    return (
      <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4 mb-6">
        <h2 className="text-[#f5f5f5] font-semibold mb-4">A/B Test Results</h2>
        <Skeleton className="h-64 w-full" />
      </div>
    )
  }

  if (!abData?.length) {
    return (
      <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4 mb-6">
        <h2 className="text-[#f5f5f5] font-semibold mb-4">A/B Test Results</h2>
        <p className="text-[#888] text-sm text-center py-16">No A/B tests yet</p>
      </div>
    )
  }

  const chartData = abData.map(group => {
    const varA = group.variants.find(v => v.variant === 'A')
    const varB = group.variants.find(v => v.variant === 'B')
    return {
      group: group.variant_group,
      A: varA?.mean_engagement_rate ?? 0,
      B: varB?.mean_engagement_rate ?? 0,
      winnerA: varA?.winner === 1,
      winnerB: varB?.winner === 1,
    }
  })

  return (
    <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4 mb-6">
      <h2 className="text-[#f5f5f5] font-semibold mb-4">A/B Test Results</h2>
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={chartData}>
          <CartesianGrid stroke="#2a2a2a" strokeDasharray="3 3" />
          <XAxis dataKey="group" tick={{ fill: '#888', fontSize: 11 }} />
          <YAxis
            tickFormatter={v => `${(v * 100).toFixed(1)}%`}
            tick={{ fill: '#888', fontSize: 11 }}
          />
          <Tooltip
            contentStyle={{ background: '#1a1a1a', border: '1px solid #2a2a2a', borderRadius: 4 }}
            labelStyle={{ color: '#f5f5f5', marginBottom: 4 }}
            formatter={v => `${(v * 100).toFixed(2)}%`}
          />
          <Legend wrapperStyle={{ color: '#888', fontSize: 12, paddingTop: 12 }} />

          <Bar dataKey="A" name="Variant A">
            <LabelList
              dataKey="A"
              position="top"
              formatter={v => `${(v * 100).toFixed(2)}`}
              style={{ fill: '#888', fontSize: 10 }}
            />
            {chartData.map((entry, i) => (
              <Cell
                key={i}
                fill="#e60023"
                stroke={entry.winnerA ? '#DAA520' : 'none'}
                strokeWidth={entry.winnerA ? 2 : 0}
              />
            ))}
          </Bar>

          <Bar dataKey="B" name="Variant B">
            <LabelList
              dataKey="B"
              position="top"
              formatter={v => `${(v * 100).toFixed(2)}`}
              style={{ fill: '#888', fontSize: 10 }}
            />
            {chartData.map((entry, i) => (
              <Cell
                key={i}
                fill="#4f8ef7"
                stroke={entry.winnerB ? '#DAA520' : 'none'}
                strokeWidth={entry.winnerB ? 2 : 0}
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

// ── A/B Form ──────────────────────────────────────────────────────────────────
function ABForm({ pins, onRefresh, showToast }) {
  const [form, setForm] = useState({
    pin_id: '', variant_group: '', variant: 'A', title: '', description: '',
  })
  const [submitting, setSubmitting] = useState(false)
  const [error, setError]           = useState(null)

  const set = (key) => (e) => setForm(f => ({ ...f, [key]: e.target.value }))

  const handleSubmit = async (e) => {
    e.preventDefault()
    setError(null)
    setSubmitting(true)
    try {
      await api.createAB(form)
      setForm({ pin_id: '', variant_group: '', variant: 'A', title: '', description: '' })
      onRefresh()
      showToast('A/B variant created', 'success')
    } catch (e) {
      setError(e.message)
    } finally {
      setSubmitting(false)
    }
  }

  const inputCls = `w-full bg-[#0f0f0f] border border-[#2a2a2a] rounded px-3 py-2
                    text-[#f5f5f5] text-sm placeholder-[#555]
                    focus:outline-none focus:border-[#e60023]`

  return (
    <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4 mb-6">
      <h2 className="text-[#f5f5f5] font-semibold mb-4">A/B Test Setup</h2>
      <form onSubmit={handleSubmit} className="space-y-3">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          <div>
            <label className="block text-[#888] text-xs mb-1">Pin</label>
            <select
              value={form.pin_id}
              onChange={set('pin_id')}
              required
              className={inputCls}
            >
              <option value="">Select a pin…</option>
              {pins?.map(p => (
                <option key={p.pin_id} value={p.pin_id}>
                  {p.title || p.pin_id}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-[#888] text-xs mb-1">Variant Group Name</label>
            <input
              type="text"
              value={form.variant_group}
              onChange={set('variant_group')}
              required
              placeholder="e.g. test-2024-q1"
              className={inputCls}
            />
          </div>
        </div>

        <div>
          <label className="block text-[#888] text-xs mb-1">Variant</label>
          <div className="flex gap-2">
            {['A', 'B'].map(v => (
              <button
                key={v}
                type="button"
                onClick={() => setForm(f => ({ ...f, variant: v }))}
                className={`px-8 py-2 rounded text-sm font-semibold border transition-colors
                  ${form.variant === v
                    ? 'bg-[#e60023] border-[#e60023] text-white'
                    : 'border-[#2a2a2a] text-[#888] hover:border-[#555]'}`}
              >
                {v}
              </button>
            ))}
          </div>
        </div>

        <div>
          <label className="block text-[#888] text-xs mb-1">Title</label>
          <input
            type="text"
            value={form.title}
            onChange={set('title')}
            placeholder="Pin title for this variant"
            className={inputCls}
          />
        </div>

        <div>
          <label className="block text-[#888] text-xs mb-1">Description</label>
          <textarea
            value={form.description}
            onChange={set('description')}
            placeholder="Pin description for this variant"
            rows={3}
            className={`${inputCls} resize-y`}
          />
        </div>

        {error && <p className="text-red-400 text-sm">{error}</p>}

        <button
          type="submit"
          disabled={submitting}
          className="bg-[#e60023] text-white px-6 py-2 rounded text-sm font-medium
                     hover:bg-[#c4001f] disabled:opacity-50 transition-colors"
        >
          {submitting ? 'Creating…' : 'Create Variant'}
        </button>
      </form>
    </div>
  )
}

// ── App (root) ────────────────────────────────────────────────────────────────
export default function App() {
  const [pins,       setPins]       = useState(null)
  const [summary,    setSummary]    = useState(null)
  const [allMetrics, setAllMetrics] = useState({})
  const [keywords,   setKeywords]   = useState(null)
  const [abData,     setAbData]     = useState(null)
  const [runStatus,  setRunStatus]  = useState(null)
  const [loading,    setLoading]    = useState(true)
  const [toast,      setToast]      = useState(null)

  const prevStatusRef = useRef(null)

  const showToast = useCallback((message, type = 'success') => {
    setToast({ message, type, key: Date.now() })
  }, [])

  // Fetch everything from the API
  const fetchAllData = useCallback(async () => {
    try {
      const [pinsData, summaryData, kwData, abRes] = await Promise.all([
        api.getPins(),
        api.getMetricsSummary(),
        api.getKeywords(),
        api.getAB(),
      ])
      setPins(pinsData)
      setSummary(summaryData)
      setKeywords(kwData)
      setAbData(abRes)

      // Per-pin metrics
      const metricsMap = {}
      await Promise.all(pinsData.map(async pin => {
        try {
          metricsMap[pin.pin_id] = await api.getPinMetrics(pin.pin_id)
        } catch {
          metricsMap[pin.pin_id] = []
        }
      }))
      setAllMetrics(metricsMap)
    } catch (e) {
      showToast(`Failed to load data: ${e.message}`, 'error')
    } finally {
      setLoading(false)
    }
  }, [showToast])

  // Initial load
  useEffect(() => {
    fetchAllData()
    api.getRunStatus().then(setRunStatus).catch(() => {})
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  // Poll run status every 3 s while pipeline is running
  useEffect(() => {
    if (runStatus?.status !== 'running') {
      if (prevStatusRef.current === 'running') {
        // Just finished — refetch all data
        fetchAllData()
      }
      prevStatusRef.current = runStatus?.status ?? null
      return
    }
    prevStatusRef.current = 'running'

    const interval = setInterval(async () => {
      try {
        const status = await api.getRunStatus()
        setRunStatus(status)
      } catch {}
    }, 3000)
    return () => clearInterval(interval)
  }, [runStatus?.status, fetchAllData])

  const handleRunNow = async () => {
    try {
      await api.triggerRun()
      setRunStatus(s => ({ ...s, status: 'running', error: null }))
    } catch (e) {
      showToast(e.message, 'error')
    }
  }

  const handleRefresh = useCallback(() => {
    setLoading(true)
    fetchAllData()
  }, [fetchAllData])

  return (
    <div className="min-h-screen bg-[#0f0f0f] text-[#f5f5f5]">
      <Header runStatus={runStatus} onRunNow={handleRunNow} />

      <main className="max-w-7xl mx-auto px-4 sm:px-6 py-6">
        <StatCards summary={summary} loading={loading} />
        <PinManager pins={pins} loading={loading} onRefresh={handleRefresh} showToast={showToast} />
        <EngagementChart pins={pins || []} allMetrics={allMetrics} loading={loading} />
        <KeywordChart keywords={keywords} pins={pins || []} loading={loading} />
        <ABResults abData={abData} loading={loading} />
        <ABForm pins={pins} onRefresh={handleRefresh} showToast={showToast} />
      </main>

      {toast && (
        <Toast
          key={toast.key}
          message={toast.message}
          type={toast.type}
          onClose={() => setToast(null)}
        />
      )}
    </div>
  )
}
