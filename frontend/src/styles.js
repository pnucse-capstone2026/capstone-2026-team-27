// "관제실(Signal Room)" 톤 — 범용 SaaS 파랑/보라 그라디언트 대신, 이상탐지·모니터링
// 도구에 어울리는 앰버(경보) 시그널 컬러 + 관제 콘솔 느낌의 각진 레이아웃을 쓴다.
const DARK = {
  bg: "#08090b",
  panel: "#131316",
  panelRaised: "#1a1a1e",
  border: "#28282d",
  text: "#e4ecef",
  subtext: "#7d8b91",
  accent: "#39d0ff",
  accentDim: "#1d5a6e",
};

const LIGHT = {
  bg: "#eef2f4",
  panel: "#ffffff",
  panelRaised: "#f6f9fa",
  border: "#d3dce0",
  text: "#151b1e",
  subtext: "#5c6a70",
  accent: "#0891b2",
  accentDim: "#bfe6ef",
};

// 데이터/숫자는 모노스페이스(JetBrains Mono), 제목/라벨은 기하학적 산세리프
// (Space Grotesk) — 둘 다 시스템 기본 폰트(Inter/Arial 등)와 다른 인상을 주려는 선택.
export const font = {
  display: '"Space Grotesk", "Pretendard", system-ui, sans-serif',
  mono: '"JetBrains Mono", "D2Coding", monospace',
};

// 라벨류에 공통으로 쓰는 "관제판" 느낌의 대문자 + 자간 스타일
export const labelStyle = {
  fontFamily: font.mono,
  fontSize: 11,
  fontWeight: 700,
  letterSpacing: "0.12em",
  textTransform: "uppercase",
};

// 배경에 아주 옅은 격자 텍스처를 깔아서 "관제 콘솔" 분위기를 준다 (컴포넌트 최상위에 적용).
export function gridBackground() {
  return {
    backgroundColor: colors.bg,
    backgroundImage: `linear-gradient(${colors.border}22 1px, transparent 1px), linear-gradient(90deg, ${colors.border}22 1px, transparent 1px)`,
    backgroundSize: "28px 28px",
  };
}

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

// 실패(빨강)만 빼고 전부 파랑 계열로 통일 — 진하기(shade)만 달리해서 항목을 구분한다.
const BLUE_PALE = { background: "#e0f2fe", color: "#075985" };
const BLUE_SOFT = { background: "#bae6fd", color: "#0c4a6e" };
const BLUE_MED = { background: "#7dd3fc", color: "#082f49" };
const BLUE_DEEP = { background: "#0c4a6e", color: "#e0f2fe" };

export const SEVERITY_STYLES = {
  HIGH: BLUE_DEEP,
  MED: BLUE_MED,
  LOW: BLUE_PALE,
};

export const RESULT_STYLES = {
  risk_security: BLUE_DEEP,
  cost_spike: BLUE_MED,
  cost_inefficiency: BLUE_SOFT,
  force_pass: BLUE_PALE,
};

export const SOURCE_STYLES = {
  human: BLUE_PALE,
  llm: BLUE_SOFT,
};

export function badgeStyle(map, key, fallback = { background: "#1d5a6e", color: "#e0f2fe" }) {
  return {
    display: "inline-block",
    padding: "2px 8px",
    borderRadius: 2,
    fontSize: 11,
    fontWeight: 700,
    fontFamily: font.mono,
    letterSpacing: "0.03em",
    ...(map[key] || fallback),
  };
}

// 아래는 전부 함수다 (정적 객체 아님) — colors.*를 렌더링 시점에 읽어야 다크/라이트
// 전환이 즉시 반영된다. 정적 객체로 두면 모듈이 처음 로드될 때 값이 고정돼버려서
// 테마를 바꿔도 이미 만들어진 객체 안의 색은 안 바뀐다.
// 각진 모서리(반경 2px)로 "관제 패널" 느낌을 준다 — 둥근 카드형 SaaS UI와 의도적으로 대비.
export function card() {
  return {
    background: colors.panel,
    border: `1px solid ${colors.border}`,
    borderRadius: 2,
    padding: 20,
  };
}

export function inputStyle() {
  return {
    background: colors.bg,
    color: colors.text,
    border: `1px solid ${colors.border}`,
    borderRadius: 2,
    padding: "9px 12px",
    fontSize: 13,
    fontFamily: font.mono,
  };
}

export const button = {
  base: () => ({
    padding: "9px 16px",
    borderRadius: 2,
    fontSize: 13,
    fontWeight: 700,
    letterSpacing: "0.02em",
    cursor: "pointer",
    fontFamily: font.display,
  }),
  approve: () => ({
    background: "#0c4a6e",
    color: "#e0f2fe",
    border: "none",
  }),
  reject: () => ({
    background: "transparent",
    color: "#e0654f",
    border: "1px solid #e0654f",
  }),
  primary: () => ({
    background: colors.accent,
    color: "#0a0a0a",
    border: "none",
  }),
  ghost: () => ({
    background: colors.panelRaised,
    color: colors.text,
    border: `1px solid ${colors.border}`,
  }),
};

// spend-cap 스타일 진행 바 (Google AI Studio 참고) — 사용량/한도 둘 다 넘기면 바+숫자 렌더링
export function progressBarColor(ratio) {
  if (ratio >= 1) return "#e0654f"; // 한도 초과 = 실패/차단 상태라 예외적으로 빨강 유지
  if (ratio >= 0.8) return "#0284c7"; // 경고 단계는 진한 파랑으로 구분 (실패는 아님)
  return colors.accent;
}
