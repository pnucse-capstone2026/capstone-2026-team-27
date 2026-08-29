import { useState } from "react";
import { card, colors, button, badgeStyle, RESULT_STYLES, SOURCE_STYLES, inputStyle } from "../styles.js";

const RESULT_OPTIONS = ["risk_security", "cost_spike", "cost_inefficiency", "force_pass"];

export default function RuleBook({ rules, onCreate, onDelete, onToggle }) {
  const [form, setForm] = useState({ target: "", condition: "", result: RESULT_OPTIONS[0] });

  function submit() {
    if (!form.target.trim() || !form.condition.trim()) return;
    onCreate({ ...form, source: "human", enabled: true });
    setForm({ target: "", condition: "", result: RESULT_OPTIONS[0] });
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      <div style={{ ...card(), display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
        <input
          placeholder="대상 리소스 (예: EC2)"
          value={form.target}
          onChange={(e) => setForm({ ...form, target: e.target.value })}
          style={{ ...inputStyle(), width: 160 }}
        />
        <input
          placeholder="조건 (예: avg(cpu) < 5%)"
          value={form.condition}
          onChange={(e) => setForm({ ...form, condition: e.target.value })}
          style={{ ...inputStyle(), flex: 1, minWidth: 220 }}
        />
        <select
          value={form.result}
          onChange={(e) => setForm({ ...form, result: e.target.value })}
          style={inputStyle()}
        >
          {RESULT_OPTIONS.map((opt) => (
            <option key={opt} value={opt}>
              {opt}
            </option>
          ))}
        </select>
        <button onClick={submit} style={{ ...button.base(), ...button.primary() }}>
          Rule 추가
        </button>
      </div>

      <div style={{ ...card(), padding: 0, overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
          <thead>
            <tr style={{ textAlign: "left", borderBottom: `1px solid ${colors.border}` }}>
              {["ID", "대상", "조건", "결과", "출처", "활성화", ""].map((h) => (
                <th key={h} style={{ padding: "10px 16px", color: colors.subtext, fontWeight: 600 }}>
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rules.map((rule) => (
              <tr key={rule.id} style={{ borderBottom: `1px solid ${colors.border}` }}>
                <td style={{ padding: "10px 16px", fontWeight: 600 }}>{rule.id}</td>
                <td style={{ padding: "10px 16px" }}>{rule.target}</td>
                <td style={{ padding: "10px 16px", fontFamily: "monospace" }}>{rule.condition}</td>
                <td style={{ padding: "10px 16px" }}>
                  <span style={badgeStyle(RESULT_STYLES, rule.result)}>{rule.result}</span>
                </td>
                <td style={{ padding: "10px 16px" }}>
                  <span style={badgeStyle(SOURCE_STYLES, rule.source)}>{rule.source}</span>
                </td>
                <td style={{ padding: "10px 16px" }}>
                  <input
                    type="checkbox"
                    checked={rule.enabled}
                    onChange={() => onToggle(rule.id)}
                  />
                </td>
                <td style={{ padding: "10px 16px" }}>
                  <button
                    onClick={() => onDelete(rule.id)}
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
