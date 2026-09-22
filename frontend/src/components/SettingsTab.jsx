import { useState, useEffect } from "react";
import { card, colors, button, inputStyle, progressBarColor, badgeStyle } from "../styles.js";

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

function Toggle({ checked, onChange, disabled }) {
  return (
    <div
      onClick={() => !disabled && onChange(!checked)}
      style={{
        width: 44,
        height: 24,
        borderRadius: 999,
        background: checked ? colors.accent : colors.border,
        position: "relative",
        cursor: disabled ? "not-allowed" : "pointer",
        opacity: disabled ? 0.5 : 1,
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

function formatStartedAt(iso) {
  if (!iso) return "";
  try {
    return new Date(iso).toLocaleString("ko-KR");
  } catch {
    return iso;
  }
}

const PROCESS_BADGE = {
  running: { background: "#0c4a6e", color: "#e0f2fe" },
  stopped: { background: "#28282d", color: "#7d8b91" },
};

export default function SettingsTab({
  settings, onUpdate, onExport,
  pipelineProcess, pipelineActionPending, onStartPipeline, onStopPipeline,
}) {
  const [local, setLocal] = useState(settings);
  const [exportState, setExportState] = useState(null); // {path} | {error}

  useEffect(() => setLocal(settings), [settings]);

  if (!local) return <div style={{ color: colors.subtext }}>불러오는 중...</div>;

  const isRunning = !!pipelineProcess?.running;
  const locked = isRunning; // 실행 중이면 아래 설정 폼 전부 잠금

  function commit(patch) {
    if (locked) return;
    const next = { ...local, ...patch };
    setLocal(next);
    onUpdate(patch);
  }

  function handleExportClick() {
    setExportState(null);
    onExport()
      .then((res) => setExportState({ path: res.path }))
      .catch((err) => setExportState({ error: err.message || "저장 실패" }));
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 20, width: "100%", maxWidth: 640, margin: "0 auto" }}>
      {/* 파이프라인 실행/종료 제어 */}
      <div style={card()}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
          <div style={{ fontSize: 14, fontWeight: 700, color: colors.text }}>파이프라인 실행 제어</div>
          <span style={badgeStyle(PROCESS_BADGE, isRunning ? "running" : "stopped")}>
            {isRunning ? "실행 중" : "중지됨"}
          </span>
        </div>

        {isRunning && (
          <div style={{ fontSize: 12, color: colors.subtext, marginBottom: 12 }}>
            PID {pipelineProcess.pid} · {formatStartedAt(pipelineProcess.started_at)}부터 실행 중
          </div>
        )}

        <div style={{ display: "flex", gap: 8 }}>
          <button
            onClick={onStartPipeline}
            disabled={isRunning || pipelineActionPending}
            style={{
              ...button.base(),
              flex: 1,
              ...(isRunning ? button.ghost() : button.primary()),
              opacity: isRunning || pipelineActionPending ? 0.5 : 1,
              cursor: isRunning || pipelineActionPending ? "not-allowed" : "pointer",
            }}
          >
            ▶ 실행
          </button>
          <button
            onClick={onStopPipeline}
            disabled={!isRunning || pipelineActionPending}
            style={{
              ...button.base(),
              flex: 1,
              ...(!isRunning ? button.ghost() : button.reject()),
              opacity: !isRunning || pipelineActionPending ? 0.5 : 1,
              cursor: !isRunning || pipelineActionPending ? "not-allowed" : "pointer",
            }}
          >
            ■ 종료
          </button>
        </div>

        {isRunning && (
          <div style={{ marginTop: 10, fontSize: 12, color: colors.subtext }}>
            실행 중에는 아래 설정을 변경할 수 없습니다. 설정을 바꾸려면 먼저 종료하세요.
          </div>
        )}
      </div>

      <fieldset
        disabled={locked}
        style={{ border: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: 20 }}
      >
        <div style={{ ...card(), opacity: locked ? 0.6 : 1 }}>
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
                    cursor: locked ? "not-allowed" : "pointer",
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

        <div style={{ ...card(), opacity: locked ? 0.6 : 1 }}>
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

        <div style={{ ...card(), opacity: locked ? 0.6 : 1 }}>
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

        <div style={{ ...card(), opacity: locked ? 0.6 : 1 }}>
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
                  disabled={locked}
                  onChange={(checked) => {
                    if (locked) return;
                    const resources = { ...local.resources, [resource]: checked };
                    setLocal({ ...local, resources });
                    onUpdate({ resources });
                  }}
                />
              </div>
            ))}
          </div>
        </div>
      </fieldset>

      {/* 설정값 YAML 저장 - 실행 중에도 "현재(잠긴) 값 그대로" 내보내는 건 허용 */}
      <div style={card()}>
        <div style={{ fontSize: 14, fontWeight: 700, marginBottom: 8, color: colors.text }}>
          설정값 내보내기
        </div>
        <div style={{ fontSize: 12, color: colors.subtext, marginBottom: 12 }}>
          현재 설정값(우선순위/폴링주기/LLM비용상한/모니터링리소스)을 YAML 파일로 저장합니다.
        </div>
        <button onClick={handleExportClick} style={{ ...button.base(), ...button.ghost() }}>
          YAML로 저장
        </button>
        {exportState?.path && (
          <div style={{ marginTop: 8, fontSize: 12, color: colors.accent }}>저장됨: {exportState.path}</div>
        )}
        {exportState?.error && (
          <div style={{ marginTop: 8, fontSize: 12, color: "#e0654f" }}>{exportState.error}</div>
        )}
      </div>
    </div>
  );
}
