import { card, colors, button, badgeStyle } from "../styles.js";

const TYPE_STYLES = {
  classification: { bg: "#dbeafe", text: "#1e40af" },
  decision: { bg: "#fef3c7", text: "#92400e" },
};

export default function PromotionsQueue({ promotions, onApprove, onReject }) {
  const allItems = [
    ...(promotions.classification || []).map(p => ({ ...p, type: "classification" })),
    ...(promotions.decision || []).map(p => ({ ...p, type: "decision" })),
  ];

  if (allItems.length === 0) {
    return (
      <div style={{ ...card(), textAlign: "center", color: colors.subtext, padding: 40 }}>
        승인 대기 중인 규칙 승격 후보가 없습니다.
      </div>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      <div style={{ color: colors.subtext, fontSize: 14 }}>
        LLM이 반복적으로 동일한 판단을 내린 패턴입니다. 승인하면 Rule Book에 추가되어 이후 LLM 호출 없이 규칙으로 처리됩니다.
      </div>

      {allItems.map((item) => (
        <div key={item.id} style={{ ...card(), display: "flex", flexDirection: "column", gap: 12 }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
            <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
              <span style={badgeStyle(TYPE_STYLES, item.type)}>{item.type}</span>
              <span style={{ fontWeight: 600 }}>{item.resource_type}</span>
            </div>
            <span style={{ color: colors.subtext, fontSize: 12 }}>
              {item.queued_at?.slice(0, 10)}
            </span>
          </div>

          <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
            {item.type === "classification" ? (
              <>
                <div><strong>트리거 메트릭:</strong> {item.triggered_metrics?.join(", ")}</div>
                <div><strong>이상 유형:</strong> {item.anomaly_type}</div>
                <div><strong>검증 횟수:</strong> {item.count}회</div>
                <div style={{ fontSize: 12, color: colors.subtext, marginTop: 4 }}>
                  <strong>샘플 reasoning:</strong> {item.sample_reasoning?.slice(0, 100)}...
                </div>
              </>
            ) : (
              <>
                <div><strong>이상 유형:</strong> {item.anomaly_type}</div>
                <div><strong>추천 액션:</strong> {item.dominant_action}</div>
                <div><strong>검증 횟수:</strong> {item.count}회 (일관성 {Math.round((item.action_consistency || 0) * 100)}%)</div>
              </>
            )}
          </div>

          <div style={{ display: "flex", gap: 8, marginTop: 8 }}>
            <button
              onClick={() => onApprove(item.id)}
              style={{ ...button.base(), ...button.primary(), flex: 1 }}
            >
              승인 (Rule Book에 추가)
            </button>
            <button
              onClick={() => onReject(item.id)}
              style={{ ...button.base(), ...button.reject(), flex: 1 }}
            >
              거부
            </button>
          </div>
        </div>
      ))}
    </div>
  );
}
