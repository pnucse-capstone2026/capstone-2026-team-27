import { colors, font, labelStyle } from "../styles.js";

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

export default function Header({ activeTab, onTabChange, pipelineRunning, pendingCount, promotionsCount, theme, onToggleTheme, onLogout }) {
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
      <div style={{ padding: "22px 20px 18px", borderBottom: `1px solid ${colors.border}` }}>
        <div style={{ ...labelStyle, color: colors.accent }}>DETECTION</div>
        <div style={{ fontFamily: font.display, fontSize: 19, fontWeight: 700, color: colors.text, marginTop: 4 }}>
          관리자 제어판
        </div>
        <style>{`
          @keyframes sidebar-pulse {
            0%, 100% { opacity: 1; box-shadow: 0 0 4px 0 ${colors.accent}; }
            50% { opacity: 0.35; box-shadow: 0 0 10px 3px ${colors.accent}; }
          }
        `}</style>
        <div style={{ display: "flex", alignItems: "center", gap: 7, marginTop: 12 }}>
          <span
            style={{
              width: 7,
              height: 7,
              background: pipelineRunning ? colors.accent : colors.subtext,
              display: "inline-block",
              animation: pipelineRunning ? "sidebar-pulse 1s ease-in-out infinite" : "none",
            }}
          />
          <span style={{ fontFamily: font.mono, fontSize: 11, letterSpacing: "0.03em", color: colors.subtext }}>
            {pipelineRunning ? "RUNNING" : "STOPPED"}
          </span>
        </div>
      </div>

      <div style={{ display: "flex", flexDirection: "column", padding: "10px 0", flex: 1 }}>
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
                padding: "11px 20px",
                border: "none",
                borderLeft: `3px solid ${active ? colors.accent : "transparent"}`,
                cursor: "pointer",
                fontFamily: font.display,
                fontSize: 14,
                fontWeight: active ? 700 : 500,
                color: active ? colors.text : colors.subtext,
                background: active ? colors.panelRaised : "transparent",
              }}
            >
              <span>{tab.label}</span>
              {tab.key === "queue" && pendingCount > 0 && (
                <span
                  style={{
                    background: colors.accent,
                    color: "#0a0a0a",
                    borderRadius: 2,
                    fontFamily: font.mono,
                    fontSize: 11,
                    fontWeight: 700,
                    padding: "1px 6px",
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
            borderRadius: 2,
            border: `1px solid ${colors.border}`,
            background: "transparent",
            color: colors.subtext,
            cursor: "pointer",
            fontSize: 13,
            fontFamily: font.display,
          }}
        >
          {theme === "dark" ? "라이트 모드로 전환" : "다크 모드로 전환"}
        </button>
        <button
          onClick={onLogout}
          style={{
            width: "100%",
            marginTop: 8,
            padding: "10px 12px",
            borderRadius: 2,
            border: `1px solid ${colors.accentDim}`,
            background: "transparent",
            color: colors.accent,
            cursor: "pointer",
            fontSize: 13,
            fontFamily: font.display,
          }}
        >
          로그아웃
        </button>
      </div>
    </div>
  );
}
