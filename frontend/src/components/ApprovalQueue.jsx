import { useState } from "react";
import { card, colors, button, badgeStyle, SEVERITY_STYLES } from "../styles.js";

// decision_agent.py가 저장하는 원문은
// "LLM boto3 스펙 기반 선택: 'Throttle' - <진짜 이유> (risk=MED, cost 1.68 -> 0.10 USD/hr, 절감액=1.58/hr)"
// 형태인데, 액션/위험도/절감액은 이미 배지·숫자로 따로 보여주고 있어서 중복이다.
// 카드에는 진짜 이유 문장만 뽑아서 보여준다.
function extractReason(reason) {
  if (!reason) return reason;
  return reason
    .replace(/^LLM boto3 스펙 기반 선택: '.*?'\s*-\s*/, "")
    .replace(/\s*\(risk=.*\)\s*$/, "");
}

function QueueCard({ item, onApprove, onReject }) {
  const [showPseudo, setShowPseudo] = useState(false);

  return (
    <div style={{ ...card(), display: "flex", flexDirection: "column", gap: 12 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <span style={badgeStyle(SEVERITY_STYLES, item.severity)}>{item.severity}</span>
          <div>
            <div style={{ fontWeight: 700, color: colors.text }}>
              {item.action} · {item.resource_type}
            </div>
            <div style={{ fontSize: 12, color: colors.subtext }}>{item.resource_id}</div>
          </div>
        </div>
        <div style={{ textAlign: "right" }}>
          <div style={{ fontSize: 12, color: colors.subtext }}>
            {new Date(item.timestamp).toLocaleString("ko-KR")}
          </div>
          <div style={{ fontSize: 15, fontWeight: 700, color: "#16a34a" }}>
            +${item.estimated_saving.toFixed(2)}/hr
          </div>
          <div style={{ fontSize: 11, color: colors.subtext }}>예상 절감</div>
        </div>
      </div>

      <div style={{ fontSize: 13, color: colors.text, lineHeight: 1.5 }}>
        {extractReason(item.reason)}
      </div>

      <div>
        <button
          onClick={() => setShowPseudo((v) => !v)}
          style={{ ...button.base(), ...button.ghost(), fontSize: 12, padding: "4px 10px" }}
        >
          {showPseudo ? "pseudo code 숨기기" : "pseudo code 보기"}
        </button>
        {showPseudo && (
          <pre
            style={{
              marginTop: 8,
              padding: 12,
              background: "#0f172a",
              color: "#e2e8f0",
              borderRadius: 8,
              fontSize: 12,
              overflowX: "auto",
            }}
          >
            {item.pseudo_code}
          </pre>
        )}
      </div>

      <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
        <button
          onClick={() => onReject(item.id)}
          style={{ ...button.base(), ...button.reject() }}
        >
          거부
        </button>
        <button
          onClick={() => onApprove(item.id)}
          style={{ ...button.base(), ...button.approve() }}
        >
          승인
        </button>
      </div>
    </div>
  );
}

export default function ApprovalQueue({ queue, onApprove, onReject }) {
  if (queue.length === 0) {
    return <div style={{ color: colors.subtext }}>승인 대기 중인 항목이 없습니다.</div>;
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16, maxWidth: 640 }}>
      {queue.map((item) => (
        <QueueCard key={item.id} item={item} onApprove={onApprove} onReject={onReject} />
      ))}
    </div>
  );
}
