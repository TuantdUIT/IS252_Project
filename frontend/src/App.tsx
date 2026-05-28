import { useState, useEffect, useCallback } from 'react'
import axios from 'axios'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from 'recharts'

const BASE = '/api'

// =============================================================================
// Types
// =============================================================================

type Tab = 'jobs' | 'stats' | 'cluster'

interface PaginatedJobs {
  items: Job[]
  total: number
  page: number
  limit: number
  pages: number
}

interface Job {
  job_id: number
  job_title: string | null
  category: string | null
  location: string | null
  salary_min: number | null
  salary_max: number | null
  salary_avg: string | null
  experience_required: string | null
  contract_type: string | null
}

interface JobDetail extends Job {
  working_hours: string | null
  description: string | null
  requirements_text: string | null
  benefits: string | null
  technical_skills: string | null
  soft_skills: string | null
  qualifications: string | null
  languages_required: string | null
}

interface SalaryStats {
  category_group: number
  group_name: string
  location: string
  job_count: number
  avg_salary: string | null
}

interface ExperienceSalary {
  experience_required: string
  job_count: number
  avg_salary: string | null
  min_salary: number | null
  max_salary: number | null
}

interface TopKJob {
  rank_num: number
  job_id: number
  job_title: string | null
  category: string | null
  location: string | null
  salary_avg: string | null
  experience_required: string | null
}

interface SalaryBracket {
  bracket_label: string
  job_count: number
  pct_total: string | null
}

interface ClusterNode {
  nodeid: number
  nodename: string
  nodeport: number
  isactive: boolean
  noderole: string
  nodecluster: string
}

interface DataDistribution {
  category_group: number
  group_name: string
  location: string
  job_count: number
}

// =============================================================================
// Helpers
// =============================================================================

const fmt = (v: string | number | null): string =>
  v == null ? '—' : `${parseFloat(String(v)).toFixed(1)}M`

const GROUP: Record<string, string> = { '1': 'Commerce', '2': 'Tech', '3': 'Creative', '4': 'People' }

// =============================================================================
// Inline styles
// =============================================================================

const s: Record<string, React.CSSProperties> = {
  app:       { fontFamily: "'Segoe UI', system-ui, sans-serif", minHeight: '100vh', background: '#f0f2f5', margin: 0 },
  header:    { background: 'linear-gradient(135deg, #1a3c5e 0%, #2c6fad 100%)', color: '#fff', padding: '18px 32px', display: 'flex', alignItems: 'center', gap: 14, boxShadow: '0 2px 8px rgba(0,0,0,0.2)' },
  htitle:    { margin: 0, fontSize: 20, fontWeight: 700, letterSpacing: 0.3 },
  hsub:      { margin: 0, fontSize: 12, opacity: 0.75, marginTop: 2 },
  badge:     { background: 'rgba(255,255,255,0.18)', borderRadius: 6, padding: '4px 10px', fontSize: 11, fontWeight: 600 },
  nav:       { background: '#fff', borderBottom: '1px solid #e4e7ed', padding: '0 28px', display: 'flex', gap: 0 },
  main:      { maxWidth: 1180, margin: '0 auto', padding: 24 },
  card:      { background: '#fff', borderRadius: 10, padding: 24, marginBottom: 20, boxShadow: '0 1px 6px rgba(0,0,0,0.07)' },
  ctitle:    { margin: '0 0 14px', fontSize: 15, fontWeight: 700, color: '#1a3c5e', display: 'flex', alignItems: 'center', gap: 8 },
  row:       { display: 'flex', gap: 10, alignItems: 'center', flexWrap: 'wrap', marginBottom: 16 },
  input:     { border: '1px solid #d4d9e2', borderRadius: 7, padding: '8px 12px', fontSize: 13, outline: 'none', minWidth: 220 },
  select:    { border: '1px solid #d4d9e2', borderRadius: 7, padding: '8px 12px', fontSize: 13, background: '#fff', cursor: 'pointer', color: '#333' },
  btn:       { background: '#1a3c5e', color: '#fff', border: 'none', borderRadius: 7, padding: '8px 18px', fontSize: 13, cursor: 'pointer', fontWeight: 600 },
  btnSm:     { background: '#e8f0fb', color: '#1a3c5e', border: 'none', borderRadius: 6, padding: '5px 12px', fontSize: 12, cursor: 'pointer', fontWeight: 600 },
  btnDanger: { background: '#fee', color: '#c00', border: '1px solid #fcc', borderRadius: 6, padding: '5px 12px', fontSize: 12, cursor: 'pointer' },
  table:     { width: '100%', borderCollapse: 'collapse', fontSize: 13 },
  th:        { background: '#f5f7fa', padding: '10px 14px', textAlign: 'left', fontWeight: 600, color: '#555', borderBottom: '2px solid #e4e7ed', whiteSpace: 'nowrap' },
  td:        { padding: '9px 14px', borderBottom: '1px solid #f0f2f6', color: '#333', verticalAlign: 'top' },
  loading:   { textAlign: 'center', padding: 48, color: '#aaa', fontSize: 14 },
  err:       { background: '#fff3f3', border: '1px solid #fcc', borderRadius: 7, padding: 12, color: '#c00', fontSize: 13, marginBottom: 12 },
  empty:     { textAlign: 'center', padding: 36, color: '#bbb', fontSize: 13 },
  // modal
  overlay:   { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', zIndex: 100, display: 'flex', alignItems: 'center', justifyContent: 'center' },
  modal:     { background: '#fff', borderRadius: 12, padding: 28, maxWidth: 680, width: '92%', maxHeight: '82vh', overflowY: 'auto', boxShadow: '0 8px 32px rgba(0,0,0,0.2)' },
  modalHead: { margin: '0 0 18px', fontSize: 17, fontWeight: 700, color: '#1a3c5e', lineHeight: 1.3 },
  grid2:     { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '6px 20px', marginBottom: 14 },
  label:     { fontSize: 11, color: '#888', fontWeight: 600, textTransform: 'uppercase', letterSpacing: 0.5 },
  value:     { fontSize: 13, color: '#333', marginTop: 2 },
  divider:   { border: 'none', borderTop: '1px solid #eee', margin: '14px 0' },
}

function navBtn(active: boolean): React.CSSProperties {
  return {
    padding: '14px 22px', border: 'none', background: 'none', cursor: 'pointer',
    fontSize: 14, fontWeight: active ? 700 : 400,
    color: active ? '#1a3c5e' : '#777',
    borderBottom: active ? '3px solid #1a3c5e' : '3px solid transparent',
  }
}

function salaryColor(avg: string | null): React.CSSProperties {
  const v = parseFloat(avg ?? '0')
  const color = v >= 40 ? '#27ae60' : v >= 20 ? '#e67e22' : '#555'
  return { color, fontWeight: 700 }
}

// =============================================================================
// Pagination
// =============================================================================

function Pagination({ page, pages, total, onChange }: {
  page: number
  pages: number
  total: number
  onChange: (p: number) => void
}) {
  if (pages <= 1) return null

  const nums: number[] = []
  for (let i = Math.max(1, page - 2); i <= Math.min(pages, page + 2); i++) nums.push(i)

  const btnPage = (n: number): React.CSSProperties => ({
    minWidth: 32, padding: '5px 10px', border: '1px solid #d4d9e2',
    borderRadius: 6, background: n === page ? '#1a3c5e' : '#fff',
    color: n === page ? '#fff' : '#333', cursor: 'pointer', fontSize: 13,
  })
  const btnNav = (disabled: boolean): React.CSSProperties => ({
    padding: '5px 12px', border: '1px solid #d4d9e2', borderRadius: 6,
    background: disabled ? '#f5f5f5' : '#fff', color: disabled ? '#bbb' : '#333',
    cursor: disabled ? 'not-allowed' : 'pointer', fontSize: 13,
  })

  return (
    <div style={{ display: 'flex', justifyContent: 'flex-end', alignItems: 'center', gap: 6, marginTop: 16 }}>
      <span style={{ fontSize: 12, color: '#888', marginRight: 8 }}>
        Trang {page}/{pages} &nbsp;·&nbsp; {total.toLocaleString()} kết quả
      </span>
      <button style={btnNav(page === 1)} disabled={page === 1} onClick={() => onChange(page - 1)}>← Trước</button>
      {nums[0] > 1 && <span style={{ color: '#aaa', fontSize: 13 }}>…</span>}
      {nums.map(n => <button key={n} style={btnPage(n)} onClick={() => onChange(n)}>{n}</button>)}
      {nums[nums.length - 1] < pages && <span style={{ color: '#aaa', fontSize: 13 }}>…</span>}
      <button style={btnNav(page === pages)} disabled={page === pages} onClick={() => onChange(page + 1)}>Tiếp →</button>
    </div>
  )
}

// =============================================================================
// JobDetailModal
// =============================================================================

function JobDetailModal({ job, onClose }: { job: JobDetail; onClose: () => void }) {
  const field = (label: string, val: string | null) => (
    <div key={label}>
      <div style={s.label}>{label}</div>
      <div style={s.value}>{val || '—'}</div>
    </div>
  )
  return (
    <div style={s.overlay} onClick={onClose}>
      <div style={s.modal} onClick={e => e.stopPropagation()}>
        <h2 style={s.modalHead}>{job.job_title || 'Chi tiết việc làm'}</h2>
        <div style={s.grid2}>
          {field('Ngành', job.category)}
          {field('Địa điểm', job.location)}
          {field('Lương', job.salary_min != null && job.salary_max != null
            ? `${job.salary_min}–${job.salary_max}M (TB: ${fmt(job.salary_avg)})` : fmt(job.salary_avg))}
          {field('Kinh nghiệm', job.experience_required)}
          {field('Hợp đồng', job.contract_type)}
          {field('Giờ làm', job.working_hours)}
        </div>
        <hr style={s.divider} />
        {job.description && (
          <>
            <div style={s.label}>Mô tả công việc</div>
            <div style={{ ...s.value, marginTop: 4, lineHeight: 1.6 }}>{job.description}</div>
            <hr style={s.divider} />
          </>
        )}
        <div style={s.grid2}>
          {field('Kỹ năng kỹ thuật', job.technical_skills)}
          {field('Kỹ năng mềm', job.soft_skills)}
          {field('Bằng cấp', job.qualifications)}
          {field('Ngôn ngữ', job.languages_required)}
        </div>
        {job.requirements_text && (
          <>
            <hr style={s.divider} />
            <div style={s.label}>Yêu cầu</div>
            <div style={{ ...s.value, marginTop: 4, lineHeight: 1.6 }}>{job.requirements_text}</div>
          </>
        )}
        {job.benefits && (
          <>
            <hr style={s.divider} />
            <div style={s.label}>Phúc lợi</div>
            <div style={{ ...s.value, marginTop: 4, lineHeight: 1.6 }}>{job.benefits}</div>
          </>
        )}
        <div style={{ textAlign: 'right', marginTop: 20 }}>
          <button style={s.btn} onClick={onClose}>Đóng</button>
        </div>
      </div>
    </div>
  )
}

// =============================================================================
// JobsTab
// =============================================================================

function JobsTab() {
  const [mode, setMode] = useState<'list' | 'skill'>('list')
  const [keyword, setKeyword] = useState('')
  const [skill, setSkill] = useState('')
  const [catGroup, setCatGroup] = useState('')
  const [location, setLocation] = useState('')
  const [jobs, setJobs] = useState<Job[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [detail, setDetail] = useState<JobDetail | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [page, setPage] = useState(1)
  const [totalPages, setTotalPages] = useState(1)
  const [total, setTotal] = useState(0)

  const doSearch = useCallback(async (p: number) => {
    setLoading(true)
    setError('')
    try {
      const params: Record<string, string | number> = {}
      if (location) params.location = location
      if (catGroup) params.category_group = Number(catGroup)

      if (mode === 'skill') {
        if (!skill.trim()) { setJobs([]); setTotal(0); setTotalPages(1); setLoading(false); return }
        const res = await axios.get<Job[]>(`${BASE}/jobs/skill`, { params: { ...params, q: skill } })
        setJobs(res.data); setTotal(res.data.length); setTotalPages(1)
      } else if (keyword.trim()) {
        const res = await axios.get<Job[]>(`${BASE}/jobs/search`, { params: { ...params, q: keyword } })
        setJobs(res.data); setTotal(res.data.length); setTotalPages(1)
      } else {
        const res = await axios.get<PaginatedJobs>(`${BASE}/jobs`, { params: { ...params, page: p, limit: 50 } })
        setJobs(res.data.items); setTotal(res.data.total); setTotalPages(res.data.pages)
      }
    } catch {
      setError('Không thể tải danh sách việc làm. Đảm bảo backend đang chạy.')
    } finally {
      setLoading(false)
    }
  }, [mode, keyword, skill, catGroup, location])

  // Reset về trang 1 và fetch lại khi filter thay đổi
  useEffect(() => { setPage(1); doSearch(1) }, [doSearch])

  const search = () => { setPage(1); doSearch(1) }
  const handlePageChange = (p: number) => { setPage(p); doSearch(p) }

  const openDetail = async (job: Job) => {
    if (!catGroup) return
    setDetailLoading(true)
    try {
      const res = await axios.get<JobDetail>(`${BASE}/jobs/${catGroup}/${job.job_id}`)
      setDetail(res.data)
    } catch {
      setError('Không tải được chi tiết việc làm.')
    } finally {
      setDetailLoading(false)
    }
  }

  return (
    <div>
      {detail && <JobDetailModal job={detail} onClose={() => setDetail(null)} />}

      <div style={s.card}>
        <h3 style={s.ctitle}>Tìm kiếm việc làm</h3>
        <div style={s.row}>
          <button style={mode === 'list' ? s.btn : s.btnSm} onClick={() => setMode('list')}>Từ khoá</button>
          <button style={mode === 'skill' ? s.btn : s.btnSm} onClick={() => setMode('skill')}>Kỹ năng</button>
        </div>
        <div style={s.row}>
          {mode === 'list'
            ? <input style={s.input} placeholder="Tìm tên công việc…" value={keyword} onChange={e => setKeyword(e.target.value)} onKeyDown={e => e.key === 'Enter' && search()} />
            : <input style={s.input} placeholder="Kỹ năng (vd: Python, Java, Excel)…" value={skill} onChange={e => setSkill(e.target.value)} onKeyDown={e => e.key === 'Enter' && search()} />
          }
          <select style={s.select} value={catGroup} onChange={e => setCatGroup(e.target.value)}>
            <option value="">Tất cả nhóm ngành</option>
            <option value="1">1 — Commerce</option>
            <option value="2">2 — Tech</option>
            <option value="3">3 — Creative</option>
            <option value="4">4 — People</option>
          </select>
          <select style={s.select} value={location} onChange={e => setLocation(e.target.value)}>
            <option value="">Tất cả địa điểm</option>
            <option value="Hà Nội">Hà Nội</option>
            <option value="TPHCM">TPHCM</option>
          </select>
          <button style={s.btn} onClick={search} disabled={loading}>Tìm</button>
        </div>
        {catGroup
          ? <div style={{ fontSize: 12, color: '#27ae60' }}>Shard pruning ON — Task Count: 1 (chỉ 1 worker)</div>
          : <div style={{ fontSize: 12, color: '#e67e22' }}>Scatter-gather — Task Count: 4 (4 workers song song)</div>
        }
      </div>

      <div style={s.card}>
        {error && <div style={s.err}>{error}</div>}
        {loading ? (
          <div style={s.loading}>Đang tải…</div>
        ) : jobs.length === 0 ? (
          <div style={s.empty}>Không có kết quả</div>
        ) : (
          <>
            <div style={{ ...s.ctitle, justifyContent: 'space-between' }}>
              <span>Kết quả — {total.toLocaleString()} việc làm</span>
              {!catGroup && <span style={{ fontSize: 12, color: '#aaa', fontWeight: 400 }}>Chọn nhóm ngành để xem chi tiết (Tier 2)</span>}
            </div>
            <table style={{ ...s.table, marginBottom: 0 }}>
              <thead>
                <tr>
                  <th style={s.th}>Tên công việc</th>
                  <th style={s.th}>Ngành</th>
                  <th style={s.th}>Địa điểm</th>
                  <th style={s.th}>Lương TB</th>
                  <th style={s.th}>Kinh nghiệm</th>
                  <th style={s.th}>Hợp đồng</th>
                  {catGroup && <th style={s.th}></th>}
                </tr>
              </thead>
              <tbody>
                {jobs.map(j => (
                  <tr key={j.job_id}>
                    <td style={s.td}>{j.job_title || '—'}</td>
                    <td style={s.td}>{j.category || '—'}</td>
                    <td style={s.td}>{j.location || '—'}</td>
                    <td style={{ ...s.td, ...salaryColor(j.salary_avg) }}>{fmt(j.salary_avg)}</td>
                    <td style={s.td}>{j.experience_required || '—'}</td>
                    <td style={s.td}>{j.contract_type || '—'}</td>
                    {catGroup && (
                      <td style={s.td}>
                        <button style={s.btnSm} onClick={() => openDetail(j)} disabled={detailLoading}>
                          Chi tiết
                        </button>
                      </td>
                    )}
                  </tr>
                ))}
              </tbody>
            </table>
            <Pagination page={page} pages={totalPages} total={total} onChange={handlePageChange} />
          </>
        )}
      </div>
    </div>
  )
}

// =============================================================================
// StatsTab
// =============================================================================

type StatView = 'salary' | 'experience' | 'topk' | 'histogram'

interface ChartRow { name: string; 'Hà Nội': number; TPHCM: number }

function StatsTab() {
  const [view, setView] = useState<StatView>('salary')
  const [salaryData, setSalaryData] = useState<ChartRow[]>([])
  const [experience, setExperience] = useState<ExperienceSalary[]>([])
  const [topk, setTopk] = useState<TopKJob[]>([])
  const [histogram, setHistogram] = useState<SalaryBracket[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const load = useCallback(async (v: StatView) => {
    setLoading(true)
    setError('')
    try {
      if (v === 'salary') {
        const res = await axios.get<SalaryStats[]>(`${BASE}/stats/salary`)
        const map: Record<string, ChartRow> = {}
        for (const r of res.data) {
          if (!map[r.group_name]) map[r.group_name] = { name: r.group_name, 'Hà Nội': 0, TPHCM: 0 }
          const key = r.location === 'Hà Nội' ? 'Hà Nội' : 'TPHCM'
          map[r.group_name][key] = parseFloat(r.avg_salary ?? '0')
        }
        setSalaryData(Object.values(map))
      } else if (v === 'experience') {
        const res = await axios.get<ExperienceSalary[]>(`${BASE}/stats/experience`)
        setExperience(res.data)
      } else if (v === 'topk') {
        const res = await axios.get<TopKJob[]>(`${BASE}/stats/topk`, { params: { k: 10 } })
        setTopk(res.data)
      } else {
        const res = await axios.get<SalaryBracket[]>(`${BASE}/stats/histogram`, { params: { bucket: 10 } })
        setHistogram(res.data)
      }
    } catch {
      setError('Không tải được dữ liệu thống kê.')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { load(view) }, [view, load])

  const tabBtn = (v: StatView, label: string) => (
    <button key={v} style={view === v ? s.btn : s.btnSm} onClick={() => setView(v)}>{label}</button>
  )

  return (
    <div>
      <div style={s.card}>
        <h3 style={s.ctitle}>Thống kê & Phân tích</h3>
        <div style={s.row}>
          {tabBtn('salary', 'Lương theo nhóm')}
          {tabBtn('experience', 'Kinh nghiệm')}
          {tabBtn('topk', 'Top-10 lương cao')}
          {tabBtn('histogram', 'Phân bố lương')}
        </div>
      </div>

      {error && <div style={{ ...s.err, marginBottom: 16 }}>{error}</div>}

      {loading ? (
        <div style={{ ...s.card, ...s.loading }}>Đang tải…</div>
      ) : view === 'salary' ? (
        <div style={s.card}>
          <h3 style={s.ctitle}>Lương trung bình theo nhóm ngành (MapReduce 2-phase)</h3>
          <div style={{ fontSize: 12, color: '#888', marginBottom: 16 }}>
            MAP: mỗi worker tính partial SUM/COUNT → REDUCE: coordinator tổng hợp AVG. Đơn vị: triệu VND/tháng.
          </div>
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={salaryData} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis dataKey="name" tick={{ fontSize: 12 }} />
              <YAxis tick={{ fontSize: 11 }} tickFormatter={v => `${v}M`} />
              <Tooltip formatter={(v: number) => [`${v.toFixed(1)}M`, '']} />
              <Legend />
              <Bar dataKey="Hà Nội" fill="#4e79a7" radius={[4, 4, 0, 0]} />
              <Bar dataKey="TPHCM" fill="#f28e2b" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      ) : view === 'experience' ? (
        <div style={s.card}>
          <h3 style={s.ctitle}>Lương theo yêu cầu kinh nghiệm (MapReduce)</h3>
          <table style={s.table}>
            <thead>
              <tr>
                <th style={s.th}>Kinh nghiệm</th>
                <th style={s.th}>Số việc</th>
                <th style={s.th}>Lương TB</th>
                <th style={s.th}>Min</th>
                <th style={s.th}>Max</th>
              </tr>
            </thead>
            <tbody>
              {experience.map((r, i) => (
                <tr key={i}>
                  <td style={s.td}>{r.experience_required}</td>
                  <td style={s.td}>{r.job_count.toLocaleString()}</td>
                  <td style={{ ...s.td, ...salaryColor(r.avg_salary) }}>{fmt(r.avg_salary)}</td>
                  <td style={s.td}>{r.min_salary ?? '—'}M</td>
                  <td style={s.td}>{r.max_salary ?? '—'}M</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : view === 'topk' ? (
        <div style={s.card}>
          <h3 style={s.ctitle}>Top-10 việc làm lương cao nhất (Parallel Top-K)</h3>
          <div style={{ fontSize: 12, color: '#888', marginBottom: 16 }}>
            Phase 1: mỗi worker trả về top-10 cục bộ (LIMIT pushdown). Phase 2: coordinator sort 40 rows → top-10 cuối.
          </div>
          <table style={s.table}>
            <thead>
              <tr>
                <th style={s.th}>#</th>
                <th style={s.th}>Tên công việc</th>
                <th style={s.th}>Ngành</th>
                <th style={s.th}>Địa điểm</th>
                <th style={s.th}>Lương TB</th>
                <th style={s.th}>Kinh nghiệm</th>
              </tr>
            </thead>
            <tbody>
              {topk.map(r => (
                <tr key={r.rank_num}>
                  <td style={{ ...s.td, fontWeight: 700, color: '#1a3c5e', width: 36 }}>{r.rank_num}</td>
                  <td style={s.td}>{r.job_title || '—'}</td>
                  <td style={s.td}>{r.category || '—'}</td>
                  <td style={s.td}>{r.location || '—'}</td>
                  <td style={{ ...s.td, ...salaryColor(r.salary_avg) }}>{fmt(r.salary_avg)}</td>
                  <td style={s.td}>{r.experience_required || '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div style={s.card}>
          <h3 style={s.ctitle}>Phân bố mức lương (Histogram — bucket 10M)</h3>
          <div style={{ fontSize: 12, color: '#888', marginBottom: 16 }}>
            MAP: mỗi worker đếm jobs per bucket cục bộ. REDUCE: coordinator SUM counts, tính %.
          </div>
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={histogram} margin={{ top: 10, right: 30, left: 0, bottom: 30 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis dataKey="bracket_label" tick={{ fontSize: 10 }} angle={-40} textAnchor="end" />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip formatter={(v: number) => [v.toLocaleString(), 'Số việc làm']} />
              <Bar dataKey="job_count" fill="#4e79a7" radius={[4, 4, 0, 0]} name="Số việc làm" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  )
}

// =============================================================================
// ClusterTab
// =============================================================================

function ClusterTab() {
  const [nodes, setNodes] = useState<ClusterNode[]>([])
  const [dist, setDist] = useState<DataDistribution[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  useEffect(() => {
    setLoading(true)
    Promise.all([
      axios.get<ClusterNode[]>(`${BASE}/cluster/nodes`),
      axios.get<DataDistribution[]>(`${BASE}/cluster/distribution`),
    ])
      .then(([n, d]) => { setNodes(n.data); setDist(d.data) })
      .catch(() => setError('Không tải được thông tin cluster.'))
      .finally(() => setLoading(false))
  }, [])

  const roleColor: Record<string, string> = { primary: '#1a3c5e', secondary: '#27ae60' }

  return (
    <div>
      {error && <div style={s.err}>{error}</div>}
      {loading ? (
        <div style={{ ...s.card, ...s.loading }}>Đang tải…</div>
      ) : (
        <>
          <div style={s.card}>
            <h3 style={s.ctitle}>Citus Cluster Nodes</h3>
            <div style={{ fontSize: 12, color: '#888', marginBottom: 14 }}>
              1 coordinator nhận query từ client → routing đến đúng worker qua pg_dist_node / pg_dist_shard metadata.
            </div>
            <table style={s.table}>
              <thead>
                <tr>
                  <th style={s.th}>ID</th>
                  <th style={s.th}>Host</th>
                  <th style={s.th}>Port</th>
                  <th style={s.th}>Role</th>
                  <th style={s.th}>Active</th>
                  <th style={s.th}>Cluster</th>
                </tr>
              </thead>
              <tbody>
                {nodes.map(n => (
                  <tr key={n.nodeid}>
                    <td style={s.td}>{n.nodeid}</td>
                    <td style={s.td}>{n.nodename}</td>
                    <td style={s.td}>{n.nodeport}</td>
                    <td style={s.td}>
                      <span style={{ color: roleColor[n.noderole] ?? '#555', fontWeight: 600, fontSize: 12 }}>
                        {n.noderole.toUpperCase()}
                      </span>
                    </td>
                    <td style={s.td}>{n.isactive ? '✓' : '✗'}</td>
                    <td style={s.td}>{n.nodecluster}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div style={s.card}>
            <h3 style={s.ctitle}>Phân bố dữ liệu theo Shard</h3>
            <div style={{ fontSize: 12, color: '#888', marginBottom: 14 }}>
              8 phân mảnh: 4 category_group × 2 location. Distribution key: hash(category_group) → shard → worker.
            </div>
            <table style={s.table}>
              <thead>
                <tr>
                  <th style={s.th}>Group</th>
                  <th style={s.th}>Tên nhóm</th>
                  <th style={s.th}>Địa điểm</th>
                  <th style={s.th}>Số bản ghi</th>
                  <th style={s.th}>Tỷ lệ</th>
                </tr>
              </thead>
              <tbody>
                {dist.map((r, i) => {
                  const total = dist.reduce((a, x) => a + x.job_count, 0)
                  const pct = total ? ((r.job_count / total) * 100).toFixed(1) : '0'
                  return (
                    <tr key={i}>
                      <td style={s.td}>{r.category_group}</td>
                      <td style={s.td}>{GROUP[String(r.category_group)] ?? r.group_name}</td>
                      <td style={s.td}>{r.location}</td>
                      <td style={{ ...s.td, fontWeight: 600 }}>{r.job_count.toLocaleString()}</td>
                      <td style={s.td}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                          <div style={{ background: '#e8f0fb', borderRadius: 4, height: 10, width: 120, overflow: 'hidden' }}>
                            <div style={{ background: '#4e79a7', height: '100%', width: `${pct}%` }} />
                          </div>
                          <span style={{ fontSize: 12, color: '#666' }}>{pct}%</span>
                        </div>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  )
}

// =============================================================================
// App (root)
// =============================================================================

export default function App() {
  const [tab, setTab] = useState<Tab>('jobs')

  return (
    <div style={s.app}>
      <header style={s.header}>
        <div>
          <h1 style={s.htitle}>VietJobs Distributed DB Dashboard</h1>
          <p style={s.hsub}>PostgreSQL + Citus 12.1 &nbsp;|&nbsp; 1 Coordinator + 4 Workers &nbsp;|&nbsp; 24,281 bản ghi</p>
        </div>
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 8 }}>
          <span style={s.badge}>Hash Join</span>
          <span style={s.badge}>MapReduce</span>
          <span style={s.badge}>Top-K</span>
          <span style={s.badge}>Semi-Join</span>
        </div>
      </header>

      <nav style={s.nav}>
        <button style={navBtn(tab === 'jobs')}    onClick={() => setTab('jobs')}>Việc làm</button>
        <button style={navBtn(tab === 'stats')}   onClick={() => setTab('stats')}>Thống kê</button>
        <button style={navBtn(tab === 'cluster')} onClick={() => setTab('cluster')}>Cluster</button>
      </nav>

      <main style={s.main}>
        {tab === 'jobs'    && <JobsTab />}
        {tab === 'stats'   && <StatsTab />}
        {tab === 'cluster' && <ClusterTab />}
      </main>
    </div>
  )
}
