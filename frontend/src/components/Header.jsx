import { useState } from "react";
import { colors, font, labelStyle } from "../styles.js";

// 탭이 8개라 한 줄로 늘어놓으면 너무 길어져서, 성격이 비슷한 것끼리 그룹으로
// 묶고(depth 추가) 그룹을 펼쳐야 하위 탭이 보이게 했다. leaf 타입은 그룹 없이
// 바로 클릭되는 최상위 항목(대시보드/시스템설정).
const NAV = [
  { type: "leaf", key: "dashboard", label: "대시보드" },
  { type: "leaf", key: "settings", label: "시스템 설정" },
  {
    type: "group", label: "승인 대기",
    children: [
      { key: "queue", label: "Action 대기" },
      { key: "promotions", label: "규칙화 대기" },
    ],
  },
  {
    type: "group", label: "정책관리",
    children: [
      { key: "rules", label: "Rule Book" },
      { key: "whitelist", label: "예외 이벤트 관리" },
    ],
  },
  {
    type: "group", label: "운영기록",
    children: [
      { key: "failures", label: "조치 실패 이력" },
      { key: "logs", label: "LLM 로그" },
    ],
  },
];

function findGroupLabelForTab(tabKey) {
  for (const item of NAV) {
    if (item.type === "group" && item.children.some((c) => c.key === tabKey)) {
      return item.label;
    }
  }
  return null;
}

function CountBadge({ count, tone = "accent" }) {
  if (!count) return null;
  const style =
    tone === "accent"
      ? { background: colors.accent, color: "#0a0a0a" }
      : { background: "#f59e0b", color: "#ffffff" };
  return (
    <span
      style={{
        ...style,
        borderRadius: 999,
        fontFamily: font.mono,
        fontSize: 11,
        fontWeight: 700,
        padding: "1px 7px",
      }}
    >
      {count}
    </span>
  );
}

export default function Header({ activeTab, onTabChange, pipelineRunning, pendingCount, promotionsCount, theme, onToggleTheme, onLogout }) {
  // 현재 활성 탭이 속한 그룹은 기본으로 펼쳐둬서, 어느 화면에 있는지 새로고침 후에도
  // 바로 보이게 한다.
  const [expanded, setExpanded] = useState(() => {
    const g = findGroupLabelForTab(activeTab);
    return g ? { [g]: true } : {};
  });

  function toggleGroup(label) {
    setExpanded((prev) => ({ ...prev, [label]: !prev[label] }));
  }

  function countFor(tabKey) {
    if (tabKey === "queue") return pendingCount;
    if (tabKey === "promotions") return promotionsCount;
    return 0;
  }

  function groupTotal(group) {
    return group.children.reduce((sum, c) => sum + (countFor(c.key) || 0), 0);
  }

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

      <div style={{ display: "flex", flexDirection: "column", padding: "10px 0", flex: 1, overflowY: "auto" }}>
        {NAV.map((item) => {
          if (item.type === "leaf") {
            const active = item.key === activeTab;
            return (
              <button
                key={item.key}
                onClick={() => onTabChange(item.key)}
                style={{
                  display: "flex", alignItems: "center", justifyContent: "space-between",
                  textAlign: "left", padding: "11px 20px", border: "none",
                  borderLeft: `3px solid ${active ? colors.accent : "transparent"}`,
                  cursor: "pointer", fontFamily: font.display, fontSize: 14,
                  fontWeight: active ? 700 : 500,
                  color: active ? colors.text : colors.subtext,
                  background: active ? colors.panelRaised : "transparent",
                }}
              >
                <span>{item.label}</span>
              </button>
            );
          }

          // 그룹: 헤더를 누르면 펼치기/접기만 하고, 실제 탭 이동은 하위 항목에서만
          const isExpanded = !!expanded[item.label];
          const total = groupTotal(item);
          const containsActive = item.children.some((c) => c.key === activeTab);
          return (
            <div key={item.label}>
              <button
                onClick={() => toggleGroup(item.label)}
                style={{
                  width: "100%", display: "flex", alignItems: "center", justifyContent: "space-between",
                  textAlign: "left", padding: "11px 20px", border: "none",
                  borderLeft: `3px solid ${containsActive ? colors.accent : "transparent"}`,
                  cursor: "pointer", fontFamily: font.display, fontSize: 14,
                  fontWeight: containsActive ? 700 : 500,
                  color: containsActive ? colors.text : colors.subtext,
                  background: "transparent",
                }}
              >
                <span style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <span style={{ fontSize: 18, opacity: 0.85 }}>{isExpanded ? "▾" : "▸"}</span>
                  {item.label}
                </span>
                {!isExpanded && <CountBadge count={total} />}
              </button>
              {isExpanded && (
                <div>
                  {item.children.map((child) => {
                    const active = child.key === activeTab;
                    const count = countFor(child.key);
                    return (
                      <button
                        key={child.key}
                        onClick={() => onTabChange(child.key)}
                        style={{
                          width: "100%", display: "flex", alignItems: "center", justifyContent: "space-between",
                          textAlign: "left", padding: "9px 20px 9px 38px", border: "none",
                          borderLeft: `3px solid ${active ? colors.accent : "transparent"}`,
                          cursor: "pointer", fontFamily: font.display, fontSize: 13,
                          fontWeight: active ? 700 : 500,
                          color: active ? colors.text : colors.subtext,
                          background: active ? colors.panelRaised : "transparent",
                        }}
                      >
                        <span>{child.label}</span>
                        <CountBadge count={count} tone={child.key === "promotions" ? "amber" : "accent"} />
                      </button>
                    );
                  })}
                </div>
              )}
            </div>
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
