const DARK = {
  bg: "#0b0f19",
  panel: "#161c2c",
  border: "#2a3348",
  text: "#e7eaf3",
  subtext: "#8b93a7",
  accent: "#3b82f6",
};

const LIGHT = {
  bg: "#f8fafc",
  panel: "#ffffff",
  border: "#e2e8f0",
  text: "#0f172a",
  subtext: "#64748b",
  accent: "#2563eb",
};

// 다크모드 토글이 이 객체의 속성을 그때그때 바꿔치기한다 (mutate). card()/button.*()/inputStyle()
// 등 아래 공용 스타일들은 전부 "호출 시점에" colors.* 를 읽는 함수라서, 이 mutate만으로
// 화면 전체가 다시 그려질 때 자동으로 새 테마를 반영한다 — React state/context 없이도 동작.
export const colors = { ...DARK };

const THEME_STORAGE_KEY = "admin-dashboard-theme";

export function getStoredTheme() {
  try {
    return localStorage.getItem(THEME_STORAGE_KEY) || "dark";
  } catch {
    return "dark";
  }
}

export function applyTheme(theme) {
  Object.assign(colors, theme === "light" ? LIGHT : DARK);
  try {
    localStorage.setItem(THEME_STORAGE_KEY, theme);
  } catch {
    // 프라이빗 모드 등에서 localStorage 접근 실패해도 화면 전환 자체는 계속돼야 함
  }
}

export const SEVERITY_STYLES = {
  HIGH: { background: "#fee2e2", color: "#991b1b" },
  MED: { background: "#fef3c7", color: "#92400e" },
  LOW: { background: "#dcfce7", color: "#166534" },
};

export const RESULT_STYLES = {
  risk_security: { background: "#fce7f3", color: "#9d174d" },
  cost_spike: { background: "#fef9c3", color: "#854d0e" },
  cost_inefficiency: { background: "#dbeafe", color: "#1e40af" },
  force_pass: { background: "#dcfce7", color: "#166534" },
};

export const SOURCE_STYLES = {
  human: { background: "#dcfce7", color: "#166534" },
  llm: { background: "#f3e8ff", color: "#6b21a8" },
};

export function badgeStyle(map, key, fallback = { background: "#334155", color: "#e2e8f0" }) {
  return {
    display: "inline-block",
    padding: "2px 10px",
    borderRadius: 999,
    fontSize: 12,
    fontWeight: 600,
    ...(map[key] || fallback),
  };
}

// 아래는 전부 함수다 (정적 객체 아님) — colors.*를 렌더링 시점에 읽어야 다크/라이트
// 전환이 즉시 반영된다. 정적 객체로 두면 모듈이 처음 로드될 때 값이 고정돼버려서
// 테마를 바꿔도 이미 만들어진 객체 안의 색은 안 바뀐다.
export function card() {
  return {
    background: colors.panel,
    border: `1px solid ${colors.border}`,
    borderRadius: 12,
    padding: 20,
  };
}

export function inputStyle() {
  return {
    background: colors.bg,
    color: colors.text,
    border: `1px solid ${colors.border}`,
    borderRadius: 8,
    padding: "8px 12px",
    fontSize: 13,
  };
}

export const button = {
  base: () => ({
    padding: "8px 16px",
    borderRadius: 8,
    fontSize: 14,
    fontWeight: 600,
    cursor: "pointer",
  }),
  approve: () => ({
    background: "#22c55e",
    color: "#ffffff",
    border: "none",
  }),
  reject: () => ({
    background: "transparent",
    color: "#f87171",
    border: "1px solid #f87171",
  }),
  primary: () => ({
    background: colors.accent,
    color: "#ffffff",
    border: "none",
  }),
  ghost: () => ({
    background: colors.panel,
    color: colors.text,
    border: `1px solid ${colors.border}`,
  }),
};

// spend-cap 스타일 진행 바 (Google AI Studio 참고) — 사용량/한도 둘 다 넘기면 바+숫자 렌더링
export function progressBarColor(ratio) {
  if (ratio >= 1) return "#ef4444";
  if (ratio >= 0.8) return "#f59e0b";
  return colors.accent;
}
