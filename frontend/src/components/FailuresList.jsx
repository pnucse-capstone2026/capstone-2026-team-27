import { card, colors, badgeStyle, SEVERITY_STYLES } from "../styles.js";

const STATUS_LABEL = {
  failed_qa: "QA 실패",
  rollback_exhausted: "롤백 소진",
};

const SLA_LABEL = {
  cpu_ok: "CPU",
  cost_ok: "비용",
  availability_ok: "가용성",
};

function SlaBadges({ failure }) {
  return (
    <div style={{ display: "flex", gap: 6 }}>
      {Object.entries(SLA_LABEL).map(([key, label]) => {
        const ok = failure[key];
        if (ok === null || ok === undefined) return null;
        return (
          <span
            key={key}
            style={{
              fontSize: 11,
              padding: "2px 8px",
              borderRadius: 999,
              background: ok ? "#dcfce7" : "#fee2e2",
              color: ok ? "#166534" : "#991b1b",
            }}
          >
            {label} {ok ? "정상" : "위반"}
          </span>
        );
      })}
    </div>
  );
}

function FailureCard({ failure }) {
  return (
    <div style={{ ...card(), display: "flex", flexDirection: "column", gap: 10 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <span style={badgeStyle(SEVERITY_STYLES, failure.risk_level)}>{failure.risk_level}</span>
          <div>
            <div style={{ fontWeight: 700, color: colors.text }}>
              {failure.selected_action} · {failure.resource_type}
            </div>
            <div style={{ fontSize: 12, color: colors.subtext }}>{failure.resource_id}</div>
          </div>
        </div>
        <div style={{ textAlign: "right" }}>
          <div style={{ fontSize: 12, color: colors.subtext }}>
            {failure.timestamp ? new Date(failure.timestamp).toLocaleString("ko-KR") : ""}
          </div>
          <div style={{ fontSize: 12, fontWeight: 700, color: "#991b1b" }}>
            {STATUS_LABEL[failure.status] || failure.status}
            {failure.rollback_count ? ` (롤백 ${failure.rollback_count}회)` : ""}
          </div>
        </div>
      </div>

      <SlaBadges failure={failure} />

      {failure.sla_detail && (
        <div style={{ fontSize: 13, color: colors.text, lineHeight: 1.5 }}>{failure.sla_detail}</div>
      )}
    </div>
  );
}

export default function FailuresList({ failures }) {
  if (failures.length === 0) {
    return <div style={{ color: colors.subtext }}>실패한 처리가 없습니다.</div>;
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16, maxWidth: 640 }}>
      {failures.map((f) => (
        <FailureCard key={f.id} failure={f} />
      ))}
    </div>
  );
}
