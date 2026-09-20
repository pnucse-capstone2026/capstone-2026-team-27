import { useState } from "react";
import { card, colors, button, inputStyle, badgeStyle, font } from "../styles.js";

const RESOURCE_TYPES = ["", "EC2", "Lambda", "S3", "RDS", "AutoScaling"];

const MODE_STYLES = {
  general: { background: "#7dd3fc", color: "#082f49" },
  event: { background: "#0c4a6e", color: "#e0f2fe" },
  night: { background: "#1d5a6e", color: "#e0f2fe" },
};

const STATUS_STYLES = {
  진행중: { background: "#0c4a6e", color: "#e0f2fe" },
  예정: { background: "#bae6fd", color: "#0c4a6e" },
  만료됨: { background: "#28282d", color: "#7d8b91" },
  영구: { background: "#7dd3fc", color: "#082f49" },
  적용중: { background: "#0c4a6e", color: "#e0f2fe" },
  대기중: { background: "#28282d", color: "#7d8b91" },
};

const HOURS = Array.from({ length: 24 }, (_, i) => i);

function computeStatus(entry) {
  if (entry.category === "recurring_hours") {
    const h = new Date().getUTCHours(); // 서버(rule_engine)도 UTC 기준으로 판정함
    const s = entry.daily_start_hour, e = entry.daily_end_hour;
    if (s == null || e == null) return "대기중";
    const active = s <= e ? (h >= s && h < e) : (h >= s || h < e);
    return active ? "적용중" : "대기중";
  }
  const now = new Date();
  if (entry.effective_from && now < new Date(entry.effective_from)) return "예정";
  if (entry.expires_at) {
    return now > new Date(entry.expires_at) ? "만료됨" : "진행중";
  }
  return entry.effective_from ? "진행중" : "영구";
}

function fmtHour(h) {
  return `${String(h).padStart(2, "0")}:00`;
}

export default function Whitelist({ entries, onCreate, onDelete }) {
  const [mode, setMode] = useState("general"); // "general" | "event" | "night"
  const [form, setForm] = useState({
    pattern: "", resource_type: "", reason: "",
    expires_at: "", effective_from: "",
    daily_start_hour: 22, daily_end_hour: 6,
  });

  function submit() {
    if (mode === "event") {
      if (!form.effective_from || !form.expires_at) return;
    } else if (mode === "night") {
      if (form.daily_start_hour === form.daily_end_hour) return; // 24시간 내내는 의미 없음
    } else if (!form.pattern.trim()) {
      return;
    }
    onCreate({
      pattern: form.pattern || "*",
      resource_type: form.resource_type || null,
      reason: form.reason,
      expires_at: mode === "night" ? null : (form.expires_at || null),
      category: mode === "event" ? "event_period" : mode === "night" ? "recurring_hours" : null,
      effective_from: mode === "event" ? form.effective_from : null,
      daily_start_hour: mode === "night" ? Number(form.daily_start_hour) : null,
      daily_end_hour: mode === "night" ? Number(form.daily_end_hour) : null,
    });
    setForm({
      pattern: "", resource_type: "", reason: "", expires_at: "", effective_from: "",
      daily_start_hour: 22, daily_end_hour: 6,
    });
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      {/* 모드 선택: 이 화이트리스트 항목이 실제로 어떤 효과를 내는지 등록 전에 먼저 고르게 한다 */}
      <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
        <button
          onClick={() => setMode("general")}
          style={{ ...button.base(), ...(mode === "general" ? button.primary() : button.ghost()) }}
        >
          🔧 일반 리소스 제외
        </button>
        <button
          onClick={() => setMode("event")}
          style={{ ...button.base(), ...(mode === "event" ? button.primary() : button.ghost()) }}
        >
          📅 이벤트 기간 예외
        </button>
        <button
          onClick={() => setMode("night")}
          style={{ ...button.base(), ...(mode === "night" ? button.primary() : button.ghost()) }}
        >
          🌙 야간/반복 시간대 예외
        </button>
      </div>

      {/* 지금 고른 모드가 실제로 파이프라인에 어떤 영향을 주는지 등록 전에 바로 보여준다 */}
      <div
        style={{
          ...card(),
          padding: "10px 16px",
          background: colors.panelRaised,
          fontSize: 13,
          color: colors.subtext,
        }}
      >
        {mode === "general" && (
          <span>
            <b style={{ color: colors.text }}>일반 리소스 제외</b>: 패턴에 매칭되는 리소스는{" "}
            <b style={{ color: colors.text }}>QA 검증을 건너뛰고 항상 정상(통과) 처리</b>됩니다.
            개발/테스트용 리소스처럼 애초에 감시 대상이 아닌 것을 등록할 때 씁니다.
          </span>
        )}
        {mode === "event" && (
          <span>
            <b style={{ color: colors.text }}>이벤트 기간 예외</b>: 등록한 기간 동안만{" "}
            <b style={{ color: colors.text }}>EDoS(트래픽 급증) 탐지에서 제외</b>됩니다. 다른 이상탐지는
            그대로 동작합니다. 세일/이벤트처럼 정상적인 트래픽 급증이 예상되는 기간에 씁니다.
          </span>
        )}
        {mode === "night" && (
          <span>
            <b style={{ color: colors.text }}>야간/반복 시간대 예외</b>: 지정한 시:분 사이엔{" "}
            <b style={{ color: colors.text }}>매일 반복해서 EDoS 탐지에서 제외</b>됩니다(날짜 지정 없이
            매일 적용). 시간은 UTC 기준입니다. 예: 22시~06시로 등록하면 매일 그 시간대의 트래픽 증가를
            정상으로 봅니다.
          </span>
        )}
      </div>

      <div style={{ ...card(), display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
        <input
          placeholder={mode === "general" ? "패턴 (예: i-batch-*, dev-*)" : "적용 리소스 (비우면 전체)"}
          value={form.pattern}
          onChange={(e) => setForm({ ...form, pattern: e.target.value })}
          style={{ ...inputStyle(), width: 200 }}
        />
        <select
          value={form.resource_type}
          onChange={(e) => setForm({ ...form, resource_type: e.target.value })}
          style={inputStyle()}
        >
          <option value="">전체 타입</option>
          {RESOURCE_TYPES.filter(t => t).map((type) => (
            <option key={type} value={type}>{type}</option>
          ))}
        </select>
        <input
          placeholder="사유"
          value={form.reason}
          onChange={(e) => setForm({ ...form, reason: e.target.value })}
          style={{ ...inputStyle(), flex: 1, minWidth: 200 }}
        />
        {mode === "event" && (
          <>
            <label style={{ ...font, fontSize: 12, color: colors.subtext }}>시작일</label>
            <input
              type="date"
              value={form.effective_from ? form.effective_from.slice(0, 10) : ""}
              onChange={(e) => setForm({ ...form, effective_from: e.target.value ? `${e.target.value}T00:00:00Z` : "" })}
              style={inputStyle()}
            />
            <label style={{ fontSize: 12, color: colors.subtext }}>종료일</label>
            <input
              type="date"
              value={form.expires_at ? form.expires_at.slice(0, 10) : ""}
              onChange={(e) => setForm({ ...form, expires_at: e.target.value ? `${e.target.value}T23:59:59Z` : "" })}
              style={inputStyle()}
            />
          </>
        )}
        {mode === "night" && (
          <>
            <label style={{ fontSize: 12, color: colors.subtext }}>매일 시작(UTC)</label>
            <select
              value={form.daily_start_hour}
              onChange={(e) => setForm({ ...form, daily_start_hour: Number(e.target.value) })}
              style={inputStyle()}
            >
              {HOURS.map((h) => <option key={h} value={h}>{fmtHour(h)}</option>)}
            </select>
            <label style={{ fontSize: 12, color: colors.subtext }}>매일 종료(UTC)</label>
            <select
              value={form.daily_end_hour}
              onChange={(e) => setForm({ ...form, daily_end_hour: Number(e.target.value) })}
              style={inputStyle()}
            >
              {HOURS.map((h) => <option key={h} value={h}>{fmtHour(h)}</option>)}
            </select>
          </>
        )}
        {mode === "general" && (
          <input
            type="date"
            title="만료일 (비우면 영구)"
            value={form.expires_at ? form.expires_at.slice(0, 10) : ""}
            onChange={(e) => setForm({ ...form, expires_at: e.target.value ? `${e.target.value}T23:59:59Z` : "" })}
            style={inputStyle()}
          />
        )}
        <button onClick={submit} style={{ ...button.base(), ...button.primary() }}>
          항목 추가
        </button>
      </div>

      <div style={{ ...card(), padding: 0, overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
          <thead>
            <tr style={{ textAlign: "left", borderBottom: `1px solid ${colors.border}` }}>
              {["ID", "패턴", "리소스 타입", "효과", "상태", "사유", "기간", ""].map((h) => (
                <th key={h} style={{ padding: "10px 16px", color: colors.subtext, fontWeight: 600 }}>
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {entries.map((entry) => {
              const isEvent = entry.category === "event_period";
              const isNight = entry.category === "recurring_hours";
              const modeKey = isNight ? "night" : isEvent ? "event" : "general";
              const status = computeStatus(entry);
              return (
                <tr key={entry.id} style={{ borderBottom: `1px solid ${colors.border}` }}>
                  <td style={{ padding: "10px 16px", fontWeight: 600, color: colors.subtext }}>{entry.id}</td>
                  <td style={{ padding: "10px 16px", fontFamily: "monospace" }}>{entry.pattern}</td>
                  <td style={{ padding: "10px 16px" }}>{entry.resource_type || "전체"}</td>
                  <td style={{ padding: "10px 16px" }}>
                    <span style={badgeStyle(MODE_STYLES, modeKey)}>
                      {isNight ? "야간반복 (EDoS만 예외)" : isEvent ? "이벤트기간 (EDoS만 예외)" : "전체 예외 (QA 자동통과)"}
                    </span>
                  </td>
                  <td style={{ padding: "10px 16px" }}>
                    <span style={badgeStyle(STATUS_STYLES, status)}>{status}</span>
                  </td>
                  <td style={{ padding: "10px 16px" }}>{entry.reason}</td>
                  <td style={{ padding: "10px 16px", color: colors.subtext }}>
                    {isNight
                      ? `매일 ${fmtHour(entry.daily_start_hour ?? 0)}~${fmtHour(entry.daily_end_hour ?? 0)} (UTC)`
                      : isEvent
                      ? `${entry.effective_from?.slice(0, 10) || "?"} ~ ${entry.expires_at?.slice(0, 10) || "?"}`
                      : entry.expires_at ? `~ ${entry.expires_at.slice(0, 10)}` : "영구"}
                  </td>
                  <td style={{ padding: "10px 16px" }}>
                    <button
                      onClick={() => onDelete(entry.id)}
                      style={{ ...button.base(), ...button.reject(), padding: "4px 10px", fontSize: 12 }}
                    >
                      삭제
                    </button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
