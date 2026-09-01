import { colors } from "../styles.js";

const TABS = [
  { key: "dashboard", label: "대시보드" },
  { key: "settings", label: "시스템 설정" },
  { key: "queue", label: "승인 대기" },
  { key: "promotions", label: "규칙 승격" },
  { key: "rules", label: "Rule Book" },
  { key: "whitelist", label: "화이트리스트" },
  { key: "logs", label: "LLM 로그" },
  { key: "failures", label: "처리 실패" },
];

export default function Header({ activeTab, onTabChange, pipelineRunning, pendingCount, promotionsCount, theme, onToggleTheme }) {
  return (
    <div
      style={{
        width: 220,
        flexShrink: 0,
        height: "100vh",
        position: "sticky",
        top: 0,
        display: "flex",
        flexDirection: "column",
        background: colors.panel,
        borderRight: `1px solid ${colors.border}`,
      }}
    >
      <div style={{ padding: "20px 20px 16px" }}>
        <div style={{ fontSize: 12, fontWeight: 700, letterSpacing: 1, color: colors.subtext }}>
          DETECTION
        </div>
        <div style={{ fontSize: 18, fontWeight: 700, color: colors.text, marginTop: 2 }}>
          관리자 제어판
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 6, marginTop: 10 }}>
          <span
            style={{
              width: 8,
              height: 8,
              borderRadius: "50%",
              background: pipelineRunning ? "#22c55e" : "#6b7280",
              display: "inline-block",
            }}
          />
          <span style={{ fontSize: 12, color: colors.subtext }}>
            {pipelineRunning ? "파이프라인 실행 중" : "파이프라인 중지됨"}
          </span>
        </div>
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: 2, padding: "8px 12px", flex: 1 }}>
        {TABS.map((tab) => {
          const active = tab.key === activeTab;
          return (
            <button
              key={tab.key}
              onClick={() => onTabChange(tab.key)}
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                textAlign: "left",
                padding: "10px 12px",
                borderRadius: 8,
                border: "none",
                cursor: "pointer",
                fontSize: 14,
                fontWeight: active ? 700 : 500,
                color: active ? "#ffffff" : colors.subtext,
                background: active ? colors.accent : "transparent",
              }}
            >
              <span>{tab.label}</span>
              {tab.key === "queue" && pendingCount > 0 && (
                <span
                  style={{
                    background: active ? "rgba(255,255,255,0.25)" : "#ef4444",
                    color: "#ffffff",
                    borderRadius: 999,
                    fontSize: 11,
                    fontWeight: 700,
                    padding: "1px 7px",
                  }}
                >
                  {pendingCount}
                </span>
              )}
              {tab.key === "promotions" && promotionsCount > 0 && (
                <span
                  style={{
                    background: active ? "rgba(255,255,255,0.25)" : "#f59e0b",
                    color: "#ffffff",
                    borderRadius: 999,
                    fontSize: 11,
                    fontWeight: 700,
                    padding: "1px 7px",
                  }}
                >
                  {promotionsCount}
                </span>
              )}
            </button>
          );
        })}
      </div>

      <div style={{ padding: 12, borderTop: `1px solid ${colors.border}` }}>
        <button
          onClick={onToggleTheme}
          style={{
            width: "100%",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            gap: 8,
            padding: "10px 12px",
            borderRadius: 8,
            border: `1px solid ${colors.border}`,
            background: "transparent",
            color: colors.subtext,
            cursor: "pointer",
            fontSize: 13,
          }}
        >
          {theme === "dark" ? "라이트 모드로 전환" : "다크 모드로 전환"}
        </button>
      </div>
    </div>
  );
}
