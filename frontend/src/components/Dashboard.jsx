import { card, colors, badgeStyle, SEVERITY_STYLES } from "../styles.js";

// 실제 파이프라인 6단계 그대로 — 예전 mock 시절엔 "Recovery"라는, 실제 에이전트에
// 없는 이름이 들어있었다 (실제 에이전트: Detection/Classification/Decision/Action/QA/Logging).
const NODE_LABELS = [
  { key: "detection", label: "Detection" },
  { key: "classification", label: "Classification" },
  { key: "decision", label: "Decision" },
  { key: "action", label: "Action" },
  { key: "qa", label: "QA" },
  { key: "logging", label: "Logging" },
];

const NODE_COLOR = {
  idle: "#cbd5e1",
  running: "#f59e0b",
  success: "#22c55e",
  error: "#ef4444",
};

function StatCard({ label, value, accent, onClick, tooltip }) {
  return (
    <div
      style={{ ...card(), flex: 1, minWidth: 160, cursor: "pointer" }}
      onClick={onClick}
      title={tooltip}
    >
      <div style={{ fontSize: 13, color: colors.subtext, marginBottom: 8 }}>{label}</div>
      <div style={{ fontSize: 28, fontWeight: 700, color: accent || colors.text }}>{value}</div>
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
        <div style={{ fontWeight: 700, color: colors.text }}>{item.action}</div>
        <div style={{ fontSize: 13, color: colors.subtext }}>
          {item.resource_id} · {item.resource_type}
        </div>
      </div>
      {display.type === "saving" ? (
        <div style={{ fontSize: 13, fontWeight: 700, color: "#16a34a" }}>
          ${display.value.toFixed(2)}/hr 절감 가능
        </div>
      ) : (
        <div
          style={{
            fontSize: 13,
            fontWeight: 700,
            color: display.value === "처리 완료" ? colors.subtext : "#dc2626",
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
      <div style={{ display: "flex", gap: 16, flexWrap: "wrap" }}>
        <StatCard
          label="이상 탐지"
          value={stats.anomaly_detected}
          onClick={onNavigateToLogs}
          tooltip="클릭하면 LLM 로그에서 관련 판단 근거를 볼 수 있습니다"
        />
        <StatCard
          label="승인 대기"
          value={stats.pending_approvals}
          onClick={onNavigateToLogs}
          tooltip="클릭하면 LLM 로그에서 관련 판단 근거를 볼 수 있습니다"
        />
        <StatCard
          label="이상 처리 완료"
          value={stats.anomaly_completed}
          onClick={onNavigateToLogs}
          tooltip="클릭하면 LLM 로그에서 관련 판단 근거를 볼 수 있습니다"
        />
        <StatCard
          label="처리 실패"
          value={stats.anomaly_failed}
          accent={stats.anomaly_failed > 0 ? "#dc2626" : undefined}
          onClick={onNavigateToFailures}
          tooltip="클릭하면 실패한 처리 목록(SLA 위반 사유)을 볼 수 있습니다"
        />
      </div>

      <div style={card()}>
        <div style={{ fontSize: 14, fontWeight: 700, marginBottom: 20, color: colors.text }}>
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
                  gap: 6,
                  minWidth: 90,
                }}
              >
                <div
                  style={{
                    width: 14,
                    height: 14,
                    borderRadius: "50%",
                    background: NODE_COLOR[nodes[node.key]] || NODE_COLOR.idle,
                  }}
                />
                <div style={{ fontSize: 13, fontWeight: 600, color: colors.text }}>{node.label}</div>
                <div style={{ fontSize: 11, color: colors.subtext }}>{nodes[node.key] || "idle"}</div>
              </div>
              {idx < NODE_LABELS.length - 1 && (
                <div style={{ width: 40, height: 2, background: colors.border }} />
              )}
            </div>
          ))}
        </div>
      </div>

      <div style={card()}>
        <div style={{ fontSize: 14, fontWeight: 700, marginBottom: 8, color: colors.text }}>
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
