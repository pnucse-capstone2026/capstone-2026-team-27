import { useState } from "react";
import { card, colors, badgeStyle, SOURCE_STYLES } from "../styles.js";

const STAGE_LABEL = {
  classification: "분류",
  decision: "액션 결정",
};

function PseudoCodeBlock({ pseudoCode, stageLabel }) {
  if (pseudoCode) {
    return (
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
        {pseudoCode}
      </pre>
    );
  }
  return (
    <div style={{ marginTop: 8, fontSize: 12, color: colors.subtext }}>
      판단 코드 없음 ({stageLabel} 단계는 pseudo_code를 만들지 않음)
    </div>
  );
}

function StageBlock({ label, entry }) {
  return (
    <div style={{ marginTop: 10, paddingTop: 10, borderTop: `1px solid ${colors.border}` }}>
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
        <span style={badgeStyle(SOURCE_STYLES, entry.source)}>{entry.source}</span>
        <span style={{ fontSize: 12, fontWeight: 600, color: colors.subtext }}>{label}</span>
      </div>
      <div style={{ fontSize: 13, color: colors.text }}>{entry.reasoning}</div>
    </div>
  );
}

function GroupedLogRow({ log }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div style={{ ...card(), cursor: "pointer" }} onClick={() => setExpanded((v) => !v)}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <div style={{ fontWeight: 600, color: colors.text }}>
          {log.resource_type} · {log.resource_id}
        </div>
        <div style={{ fontSize: 12, color: colors.subtext }}>
          {new Date(log.timestamp).toLocaleString("ko-KR")}
        </div>
      </div>

      <StageBlock label="분류" entry={log.classification} />
      <StageBlock label="액션 결정" entry={log.decision} />
      {expanded && <PseudoCodeBlock pseudoCode={log.decision.pseudo_code} stageLabel="액션 결정" />}
    </div>
  );
}

function SingleLogRow({ log }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div style={{ ...card(), cursor: "pointer" }} onClick={() => setExpanded((v) => !v)}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <span style={badgeStyle(SOURCE_STYLES, log.source)}>{log.source}</span>
          <span style={{ fontSize: 12, color: colors.subtext }}>
            {STAGE_LABEL[log.stage] || log.stage}
          </span>
          <div style={{ fontWeight: 600, color: colors.text }}>
            {log.resource_type} · {log.resource_id}
          </div>
        </div>
        <div style={{ fontSize: 12, color: colors.subtext }}>
          {new Date(log.timestamp).toLocaleString("ko-KR")}
        </div>
      </div>
      <div style={{ fontSize: 13, color: colors.text, marginTop: 8 }}>{log.reasoning}</div>
      {expanded && (
        <PseudoCodeBlock pseudoCode={log.pseudo_code} stageLabel={STAGE_LABEL[log.stage] || log.stage} />
      )}
    </div>
  );
}

export default function LlmLogs({ logs }) {
  if (logs.length === 0) {
    return <div style={{ color: colors.subtext }}>기록된 로그가 없습니다.</div>;
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12, width: "100%", maxWidth: 720, margin: "0 auto" }}>
      {logs.map((log) =>
        log.grouped ? <GroupedLogRow key={log.id} log={log} /> : <SingleLogRow key={log.id} log={log} />
      )}
    </div>
  );
}
