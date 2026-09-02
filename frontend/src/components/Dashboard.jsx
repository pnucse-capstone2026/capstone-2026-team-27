import { card, colors, font, labelStyle, badgeStyle, SEVERITY_STYLES } from "../styles.js";

const NODE_LABELS = [
  { key: "detection", label: "Detection" },
  { key: "decision", label: "Decision" },
  { key: "recovery", label: "Recovery" },
  { key: "logging", label: "Logging" },
];

const NODE_COLOR = {
  idle: "#4a4a4f",
  running: "#39d0ff",
  success: "#3b82f6",
  error: "#e0654f",
};

function StatCard({ label, value, accent, onClick, tooltip }) {
  return (
    <div
      style={{
        ...card(),
        flex: 1,
        minWidth: 160,
        cursor: "pointer",
        borderTop: `2px solid ${accent || colors.border}`,
      }}
      onClick={onClick}
      title={tooltip}
    >
      <div style={{ ...labelStyle, color: colors.subtext, marginBottom: 10 }}>{label}</div>
      <div style={{ fontFamily: font.mono, fontSize: 30, fontWeight: 700, color: accent || colors.text }}>
        {value}
      </div>
    </div>
  );
}

function RecentDetectionRow({ item }) {
  const display = item.display || {};
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        padding: "12px 0",
        borderBottom: `1px solid ${colors.border}`,
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
        <span style={badgeStyle(SEVERITY_STYLES, item.severity)}>{item.severity}</span>
        <div style={{ fontFamily: font.display, fontWeight: 700, color: colors.text }}>{item.action}</div>
        <div style={{ fontFamily: font.mono, fontSize: 12, color: colors.subtext }}>
          {item.resource_id} · {item.resource_type}
        </div>
      </div>
      {display.type === "saving" ? (
        <div style={{ fontFamily: font.mono, fontSize: 13, fontWeight: 700, color: "#3b82f6" }}>
          ${display.value.toFixed(2)}/hr 절감 가능
        </div>
      ) : (
        <div
          style={{
            fontFamily: font.mono,
            fontSize: 13,
            fontWeight: 700,
            color: display.value === "처리 완료" ? colors.subtext : "#e0654f",
          }}
        >
          {display.value}
        </div>
      )}
    </div>
  );
}

export default function Dashboard({ status, loading, recentDetections, onNavigateToLogs, onNavigateToFailures }) {
  if (loading || !status) {
    return <div style={{ color: colors.subtext }}>불러오는 중...</div>;
  }

  const { stats, nodes } = status;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 24 }}>
      <style>{`
        @keyframes node-pulse {
          0%, 100% { opacity: 1; box-shadow: 0 0 4px 0 ${NODE_COLOR.running}; }
          50% { opacity: 0.35; box-shadow: 0 0 10px 3px ${NODE_COLOR.running}; }
        }
      `}</style>
      <div style={{ display: "flex", gap: 16, flexWrap: "wrap" }}>
        <StatCard
          label="이상 탐지"
          value={stats.anomaly_detected}
          accent={colors.accent}
          onClick={onNavigateToLogs}
          tooltip="클릭하면 LLM 로그에서 관련 판단 근거를 볼 수 있습니다"
        />
        <StatCard
          label="승인 대기"
          value={stats.pending_approvals}
          accent={stats.pending_approvals > 0 ? colors.accent : undefined}
          onClick={onNavigateToLogs}
          tooltip="클릭하면 LLM 로그에서 관련 판단 근거를 볼 수 있습니다"
        />
        <StatCard
          label="이상 처리 완료"
          value={stats.anomaly_completed}
          accent="#3b82f6"
          onClick={onNavigateToLogs}
          tooltip="클릭하면 LLM 로그에서 관련 판단 근거를 볼 수 있습니다"
        />
        <StatCard
          label="처리 실패"
          value={stats.anomaly_failed}
          accent={stats.anomaly_failed > 0 ? "#e0654f" : undefined}
          onClick={onNavigateToFailures}
          tooltip="클릭하면 실패한 처리 목록(SLA 위반 사유)을 볼 수 있습니다"
        />
      </div>

      <div style={card()}>
        <div style={{ ...labelStyle, marginBottom: 22, color: colors.subtext }}>
          파이프라인 노드 상태
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          {NODE_LABELS.map((node, idx) => (
            <div key={node.key} style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <div
                style={{
                  display: "flex",
                  flexDirection: "column",
                  alignItems: "center",
                  gap: 8,
                  minWidth: 90,
                }}
              >
                <div
                  style={{
                    width: 10,
                    height: 10,
                    background: NODE_COLOR[nodes[node.key]] || NODE_COLOR.idle,
                    animation: nodes[node.key] === "running" ? "node-pulse 1s ease-in-out infinite" : "none",
                  }}
                />
                <div style={{ fontFamily: font.display, fontSize: 13, fontWeight: 700, color: colors.text }}>
                  {node.label}
                </div>
                <div style={{ fontFamily: font.mono, fontSize: 10, letterSpacing: "0.03em", color: colors.subtext }}>
                  {(nodes[node.key] || "idle").toUpperCase()}
                </div>
              </div>
              {idx < NODE_LABELS.length - 1 && (
                <div style={{ width: 40, height: 1, background: colors.border }} />
              )}
            </div>
          ))}
        </div>
      </div>

      <div style={card()}>
        <div style={{ ...labelStyle, marginBottom: 10, color: colors.subtext }}>
          최근 탐지
        </div>
        {recentDetections.length === 0 ? (
          <div style={{ color: colors.subtext, fontSize: 13, padding: "8px 0" }}>
            최근 탐지된 이상이 없습니다.
          </div>
        ) : (
          recentDetections.map((item) => <RecentDetectionRow key={item.id} item={item} />)
        )}
      </div>
    </div>
  );
}
