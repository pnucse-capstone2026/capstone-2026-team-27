import { useState, useEffect } from "react";
import { card, colors, button, inputStyle, progressBarColor } from "../styles.js";

// decision_agent 프롬프트가 실제로 구분하는 경계값과 동일하게 3개로만 나눈다.
const PRIORITY_OPTIONS = [
  { tier: "availability", label: "가용성 우선", value: 15 },
  { tier: "balanced", label: "균형", value: 50 },
  { tier: "cost", label: "비용 절감 우선", value: 85 },
];

function priorityTier(value) {
  if (value <= 34) return "availability";
  if (value <= 64) return "balanced";
  return "cost";
}

function Toggle({ checked, onChange }) {
  return (
    <div
      onClick={() => onChange(!checked)}
      style={{
        width: 44,
        height: 24,
        borderRadius: 999,
        background: checked ? colors.accent : colors.border,
        position: "relative",
        cursor: "pointer",
        flexShrink: 0,
        transition: "background 0.15s",
      }}
    >
      <div
        style={{
          width: 20,
          height: 20,
          borderRadius: "50%",
          background: "#fff",
          position: "absolute",
          top: 2,
          left: checked ? 22 : 2,
          transition: "left 0.15s",
          boxShadow: "0 1px 3px rgba(0,0,0,0.3)",
        }}
      />
    </div>
  );
}

// config/decision_policy.py::MIN_POLLING_INTERVAL_MINUTES와 값 맞춰야 함 —
// 최종 강제는 서버가 하지만(우회 방지), 여기선 입력 단계에서 바로 알려주는 용도.
const MIN_POLLING_INTERVAL = 5;

function priorityDescription(value) {
  if (value <= 34) return "가용성 우선 — 서비스 중단을 피하고, Delete 대신 Resize를 선택합니다";
  if (value <= 64) return "균형 — 상황에 따라 Delete 또는 Resize를 혼용합니다";
  return "비용 절감 우선 — 미사용 리소스는 Delete를 권장합니다";
}

export default function SettingsTab({ settings, onUpdate }) {
  const [local, setLocal] = useState(settings);

  useEffect(() => setLocal(settings), [settings]);

  if (!local) return <div style={{ color: colors.subtext }}>불러오는 중...</div>;

  function commit(patch) {
    const next = { ...local, ...patch };
    setLocal(next);
    onUpdate(patch);
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 20, width: "100%", maxWidth: 640, margin: "0 auto" }}>
      <div style={card()}>
        <div style={{ fontSize: 14, fontWeight: 700, marginBottom: 12, color: colors.text }}>
          가용성 ↔ 비용 절감 우선순위
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          {PRIORITY_OPTIONS.map((opt) => {
            const active = priorityTier(local.priority_weight) === opt.tier;
            return (
              <button
                key={opt.tier}
                onClick={() => commit({ priority_weight: opt.value })}
                style={{
                  ...button.base(),
                  flex: 1,
                  ...(active ? button.primary() : button.ghost()),
                }}
              >
                {opt.label}
              </button>
            );
          })}
        </div>
        <div
          style={{
            marginTop: 12,
            padding: "10px 14px",
            background: colors.bg,
            borderRadius: 8,
            fontSize: 13,
            color: colors.text,
          }}
        >
          ({local.priority_weight}/100) {priorityDescription(local.priority_weight)}
        </div>
      </div>

      <div style={card()}>
        <div style={{ fontSize: 14, fontWeight: 700, marginBottom: 12, color: colors.text }}>
          폴링 주기 (분)
        </div>
        <input
          type="number"
          min={MIN_POLLING_INTERVAL}
          value={local.polling_interval}
          onChange={(e) => setLocal({ ...local, polling_interval: Number(e.target.value) })}
          onBlur={(e) => {
            const clamped = Math.max(MIN_POLLING_INTERVAL, Number(e.target.value) || MIN_POLLING_INTERVAL);
            setLocal({ ...local, polling_interval: clamped });
            commit({ polling_interval: clamped });
          }}
          style={{ ...inputStyle(), width: 120 }}
        />
        {local.polling_interval < MIN_POLLING_INTERVAL && (
          <div style={{ marginTop: 8, fontSize: 12, color: "#0284c7" }}>
            {MIN_POLLING_INTERVAL}분보다 짧으면 탐지 모델 학습에 영향을 줘서, 저장 시 자동으로{" "}
            {MIN_POLLING_INTERVAL}분으로 조정됩니다.
          </div>
        )}
        <div style={{ marginTop: 8, fontSize: 12, color: colors.subtext }}>
          최소 {MIN_POLLING_INTERVAL}분 (Detection Agent 학습 안정성을 위한 제한)
        </div>
      </div>

      <div style={card()}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
          <div style={{ fontSize: 14, fontWeight: 700, color: colors.text }}>LLM 비용 상한 ($/일)</div>
          <div style={{ fontSize: 13, color: colors.subtext }}>
            ${(local.llm_cost_spent_today ?? 0).toFixed(4)} / ${Number(local.llm_cost_limit).toFixed(2)}
          </div>
        </div>

        {(() => {
          const spent = local.llm_cost_spent_today ?? 0;
          const limit = Number(local.llm_cost_limit) || 0;
          const ratio = limit > 0 ? Math.min(1, spent / limit) : 0;
          return (
            <div
              style={{
                marginTop: 10,
                marginBottom: 14,
                height: 8,
                borderRadius: 999,
                background: colors.bg,
                overflow: "hidden",
              }}
            >
              <div
                style={{
                  width: `${ratio * 100}%`,
                  height: "100%",
                  background: progressBarColor(ratio),
                  transition: "width 0.2s",
                }}
              />
            </div>
          );
        })()}

        <input
          type="number"
          min={0}
          step={0.01}
          value={local.llm_cost_limit}
          onChange={(e) => setLocal({ ...local, llm_cost_limit: Number(e.target.value) })}
          onBlur={(e) => commit({ llm_cost_limit: Number(e.target.value) })}
          style={{ ...inputStyle(), width: 120 }}
        />
      </div>

      <div style={card()}>
        <div style={{ fontSize: 14, fontWeight: 700, marginBottom: 4, color: colors.text }}>
          모니터링 리소스
        </div>
        <div>
          {Object.entries(local.resources).map(([resource, enabled], idx, arr) => (
            <div
              key={resource}
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                padding: "14px 4px",
                borderBottom: idx < arr.length - 1 ? `1px solid ${colors.border}` : "none",
              }}
            >
              <span style={{ fontSize: 15, color: colors.text }}>{resource}</span>
              <Toggle
                checked={enabled}
                onChange={(checked) => {
                  const resources = { ...local.resources, [resource]: checked };
                  setLocal({ ...local, resources });
                  onUpdate({ resources });
                }}
              />
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
