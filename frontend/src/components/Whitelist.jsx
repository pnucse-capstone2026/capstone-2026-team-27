import { useState } from "react";
import { card, colors, button, inputStyle } from "../styles.js";

export default function Whitelist({ entries, onCreate, onDelete }) {
  const [form, setForm] = useState({ pattern: "", resource_type: "", reason: "", expires_at: "" });

  function submit() {
    if (!form.pattern.trim()) return;
    onCreate({
      pattern: form.pattern,
      resource_type: form.resource_type || null,
      reason: form.reason,
      expires_at: form.expires_at || null,
    });
    setForm({ pattern: "", resource_type: "", reason: "", expires_at: "" });
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      <div style={{ ...card(), display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
        <input
          placeholder="패턴 (예: i-batch-*)"
          value={form.pattern}
          onChange={(e) => setForm({ ...form, pattern: e.target.value })}
          style={{ ...inputStyle(), width: 180 }}
        />
        <input
          placeholder="리소스 타입 (선택)"
          value={form.resource_type}
          onChange={(e) => setForm({ ...form, resource_type: e.target.value })}
          style={{ ...inputStyle(), width: 140 }}
        />
        <input
          placeholder="사유"
          value={form.reason}
          onChange={(e) => setForm({ ...form, reason: e.target.value })}
          style={{ ...inputStyle(), flex: 1, minWidth: 200 }}
        />
        <input
          type="date"
          value={form.expires_at ? form.expires_at.slice(0, 10) : ""}
          onChange={(e) => setForm({ ...form, expires_at: e.target.value ? `${e.target.value}T00:00:00+09:00` : "" })}
          style={inputStyle()}
        />
        <button onClick={submit} style={{ ...button.base(), ...button.primary() }}>
          항목 추가
        </button>
      </div>

      <div style={{ ...card(), padding: 0, overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
          <thead>
            <tr style={{ textAlign: "left", borderBottom: `1px solid ${colors.border}` }}>
              {["패턴", "리소스 타입", "사유", "만료일", ""].map((h) => (
                <th key={h} style={{ padding: "10px 16px", color: colors.subtext, fontWeight: 600 }}>
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {entries.map((entry) => (
              <tr key={entry.id} style={{ borderBottom: `1px solid ${colors.border}` }}>
                <td style={{ padding: "10px 16px", fontFamily: "monospace" }}>{entry.pattern}</td>
                <td style={{ padding: "10px 16px" }}>{entry.resource_type || "전체"}</td>
                <td style={{ padding: "10px 16px" }}>{entry.reason}</td>
                <td style={{ padding: "10px 16px", color: colors.subtext }}>
                  {entry.expires_at ? entry.expires_at.slice(0, 10) : "영구"}
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
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
