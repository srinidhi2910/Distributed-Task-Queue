import { useState, useEffect, useCallback } from "react";
import axios from "axios";

// ─── Stat Card ────────────────────────────────────────────────────────────────
function StatCard({ label, value, color }) {
  return (
    <div style={{
      background: "#1e1e2e", borderRadius: 12, padding: "20px 28px",
      borderLeft: `4px solid ${color}`, minWidth: 140,
    }}>
      <div style={{ color: "#888", fontSize: 13, marginBottom: 6 }}>{label}</div>
      <div style={{ color, fontSize: 32, fontWeight: 700 }}>{value}</div>
    </div>
  );
}

// ─── Status Badge ─────────────────────────────────────────────────────────────
function Badge({ status }) {
  const colors = {
    pending:   { bg: "#2a2a1a", text: "#f0c040" },
    active:    { bg: "#1a2a2a", text: "#40c0f0" },
    completed: { bg: "#1a2a1a", text: "#40c080" },
    failed:    { bg: "#2a1a1a", text: "#f06040" },
    dead:      { bg: "#2a1a1a", text: "#cc3333" },
    cancelled: { bg: "#222",    text: "#888"     },
  };
  const c = colors[status] || colors.cancelled;
  return (
    <span style={{
      background: c.bg, color: c.text,
      padding: "2px 10px", borderRadius: 20, fontSize: 12, fontWeight: 600,
    }}>
      {status}
    </span>
  );
}

// ─── Enqueue Form ─────────────────────────────────────────────────────────────
function EnqueueForm({ onJobEnqueued }) {
  const [form, setForm] = useState({
    jobType: "send_email", to: "", subject: "",
    priority: 0, delayMs: 0,
  });
  const [loading, setLoading] = useState(false);
  const [msg, setMsg] = useState(null);

  const handleSubmit = async () => {
    if (!form.to || !form.subject) {
      setMsg({ type: "error", text: "Fill in all fields" });
      return;
    }
    setLoading(true);
    try {
      await axios.post("/api/jobs", {
        jobType: form.jobType,
        payload: { to: form.to, subject: form.subject },
        priority: parseInt(form.priority),
        delayMs:  parseInt(form.delayMs),
      });
      setMsg({ type: "success", text: "Job enqueued!" });
      onJobEnqueued();
      setTimeout(() => setMsg(null), 3000);
    } catch (e) {
      setMsg({ type: "error", text: e.response?.data?.error || "Failed" });
    } finally {
      setLoading(false);
    }
  };

  const inputStyle = {
    width: "100%", background: "#13131f", border: "1px solid #333",
    borderRadius: 8, padding: "8px 12px", color: "#eee",
    fontSize: 14, boxSizing: "border-box", marginBottom: 10,
  };

  return (
    <div style={{ background: "#1e1e2e", borderRadius: 12, padding: 24 }}>
      <h3 style={{ margin: "0 0 16px", color: "#ccc", fontSize: 15 }}>
        Enqueue New Job
      </h3>

      <select value={form.jobType}
        onChange={e => setForm({ ...form, jobType: e.target.value })}
        style={inputStyle}>
        <option value="send_email">send_email</option>
        <option value="resize_image">resize_image</option>
        <option value="generate_report">generate_report</option>
      </select>

      <input placeholder="To (email)" value={form.to}
        onChange={e => setForm({ ...form, to: e.target.value })}
        style={inputStyle} />

      <input placeholder="Subject" value={form.subject}
        onChange={e => setForm({ ...form, subject: e.target.value })}
        style={inputStyle} />

      <div style={{ display: "flex", gap: 10 }}>
        <input type="number" placeholder="Priority (0-10)" value={form.priority}
          onChange={e => setForm({ ...form, priority: e.target.value })}
          style={{ ...inputStyle, flex: 1, marginBottom: 0 }} />
        <input type="number" placeholder="Delay (ms)" value={form.delayMs}
          onChange={e => setForm({ ...form, delayMs: e.target.value })}
          style={{ ...inputStyle, flex: 1, marginBottom: 0 }} />
      </div>

      {msg && (
        <div style={{
          marginTop: 10, padding: "8px 12px", borderRadius: 8, fontSize: 13,
          background: msg.type === "error" ? "#2a1a1a" : "#1a2a1a",
          color:      msg.type === "error" ? "#f06040" : "#40c080",
        }}>
          {msg.text}
        </div>
      )}

      <button onClick={handleSubmit} disabled={loading}
        style={{
          marginTop: 12, width: "100%", padding: "10px 0",
          background: loading ? "#333" : "#5865f2", color: "#fff",
          border: "none", borderRadius: 8, fontSize: 14,
          fontWeight: 600, cursor: loading ? "not-allowed" : "pointer",
        }}>
        {loading ? "Enqueueing..." : "Enqueue Job"}
      </button>
    </div>
  );
}

// ─── Main App ─────────────────────────────────────────────────────────────────
export default function App() {
  const [stats,  setStats]  = useState(null);
  const [jobs,   setJobs]   = useState([]);
  const [filter, setFilter] = useState("all");
  const [connected, setConnected] = useState(false);

  // Fetch recent jobs from REST API
  const fetchJobs = useCallback(async () => {
    const params = filter !== "all" ? `?status=${filter}&limit=15` : "?limit=15";
    const res = await axios.get(`/api/jobs${params}`);
    setJobs(res.data.jobs);
  }, [filter]);

  useEffect(() => { fetchJobs(); }, [fetchJobs]);

  // Connect to SSE stream for live stats
  useEffect(() => {
    const es = new EventSource("/api/queues/stream");

    es.onopen = () => setConnected(true);

    es.onmessage = (e) => {
      const data = JSON.parse(e.data);
      if (data.type === "stats") {
        setStats(data);
        // Refresh job list every time stats update
        fetchJobs();
      }
    };

    es.onerror = () => setConnected(false);

    return () => es.close();
  }, [fetchJobs]);

  const pg = stats?.postgres || {};
  const rd = stats?.redis    || {};

  const filters = ["all","pending","active","completed","failed","dead"];

  return (
    <div style={{
      minHeight: "100vh", background: "#13131f", color: "#eee",
      fontFamily: "'Segoe UI', sans-serif", padding: 32,
    }}>
      {/* Header */}
      <div style={{ display: "flex", alignItems: "center", marginBottom: 28, gap: 12 }}>
        <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700 }}>
          Distributed Task Queue
        </h1>
        <span style={{
          fontSize: 12, padding: "3px 10px", borderRadius: 20, fontWeight: 600,
          background: connected ? "#1a2a1a" : "#2a1a1a",
          color:      connected ? "#40c080" : "#f06040",
        }}>
          {connected ? "● LIVE" : "○ connecting..."}
        </span>
      </div>

      {/* Stats Bar */}
      <div style={{ display: "flex", gap: 16, flexWrap: "wrap", marginBottom: 28 }}>
        <StatCard label="Pending"    value={pg.pending   ?? "—"} color="#f0c040" />
        <StatCard label="Active"     value={pg.active    ?? "—"} color="#40c0f0" />
        <StatCard label="Completed"  value={pg.completed ?? "—"} color="#40c080" />
        <StatCard label="Failed"     value={pg.failed    ?? "—"} color="#f06040" />
        <StatCard label="Dead (DLQ)" value={pg.dead      ?? "—"} color="#cc3333" />
        <StatCard label="Last Hour"  value={pg.completedLastHour ?? "—"} color="#a070f0" />
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 300px", gap: 24 }}>
        {/* Left — Job Feed */}
        <div>
          {/* Filter Tabs */}
          <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
            {filters.map(f => (
              <button key={f} onClick={() => setFilter(f)}
                style={{
                  padding: "6px 14px", borderRadius: 20, border: "none",
                  fontSize: 13, fontWeight: 600, cursor: "pointer",
                  background: filter === f ? "#5865f2" : "#1e1e2e",
                  color:      filter === f ? "#fff"    : "#888",
                }}>
                {f}
              </button>
            ))}
          </div>

          {/* Job Table */}
          <div style={{ background: "#1e1e2e", borderRadius: 12, overflow: "hidden" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
              <thead>
                <tr style={{ background: "#16162a" }}>
                  {["Job ID","Type","Status","Priority","Attempts","Created"].map(h => (
                    <th key={h} style={{
                      padding: "12px 16px", textAlign: "left",
                      color: "#666", fontWeight: 600, fontSize: 12,
                    }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {jobs.length === 0 && (
                  <tr><td colSpan={6} style={{ padding: 24, textAlign: "center", color: "#555" }}>
                    No jobs found
                  </td></tr>
                )}
                {jobs.map((job, i) => (
                  <tr key={job.id} style={{
                    borderTop: "1px solid #222",
                    background: i % 2 === 0 ? "transparent" : "#191928",
                  }}>
                    <td style={{ padding: "10px 16px", color: "#666", fontFamily: "monospace", fontSize: 11 }}>
                      {job.id.slice(0, 8)}…
                    </td>
                    <td style={{ padding: "10px 16px", color: "#aaa" }}>{job.job_type}</td>
                    <td style={{ padding: "10px 16px" }}><Badge status={job.status} /></td>
                    <td style={{ padding: "10px 16px", color: "#aaa" }}>{job.priority}</td>
                    <td style={{ padding: "10px 16px", color: "#aaa" }}>
                      {job.attempts}/{job.max_retries}
                    </td>
                    <td style={{ padding: "10px 16px", color: "#555", fontSize: 11 }}>
                      {new Date(job.created_at).toLocaleTimeString()}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* Right — Redis Health + Enqueue Form */}
        <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>
          {/* Redis Queue Health */}
          <div style={{ background: "#1e1e2e", borderRadius: 12, padding: 24 }}>
            <h3 style={{ margin: "0 0 16px", color: "#ccc", fontSize: 15 }}>
              Redis Queue Depths
            </h3>
            {[
              { label: "Main FIFO",  value: rd.mainQueue,     color: "#40c0f0" },
              { label: "Priority",   value: rd.priorityQueue, color: "#f0c040" },
              { label: "Delayed",    value: rd.delayedQueue,  color: "#a070f0" },
              { label: "Dead Letter",value: rd.dlq,           color: "#cc3333" },
            ].map(({ label, value, color }) => (
              <div key={label} style={{
                display: "flex", justifyContent: "space-between",
                alignItems: "center", marginBottom: 12,
              }}>
                <span style={{ color: "#888", fontSize: 13 }}>{label}</span>
                <span style={{
                  color, fontWeight: 700, fontSize: 18,
                  background: "#13131f", padding: "2px 14px", borderRadius: 8,
                }}>
                  {value ?? "—"}
                </span>
              </div>
            ))}
          </div>

          <EnqueueForm onJobEnqueued={fetchJobs} />
        </div>
      </div>
    </div>
  );
}