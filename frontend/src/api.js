const BASE_URL = "http://localhost:8000";
const TOKEN_STORAGE = "admin-dashboard-token";

export function getStoredToken() {
  try {
    return localStorage.getItem(TOKEN_STORAGE) || "";
  } catch {
    return "";
  }
}

export function setStoredToken(token) {
  try {
    localStorage.setItem(TOKEN_STORAGE, token);
  } catch {
    // localStorage 접근 불가(프라이빗 모드 등)해도 이번 세션 안에서는 계속 쓸 수 있게
  }
}

export function clearStoredToken() {
  try {
    localStorage.removeItem(TOKEN_STORAGE);
  } catch {
    // no-op
  }
}

class AuthError extends Error {}

async function request(path, options = {}) {
  const res = await fetch(`${BASE_URL}${path}`, {
    headers: {
      "Content-Type": "application/json",
      "X-Admin-Key": getStoredToken(),
    },
    ...options,
  });
  if (res.status === 401) {
    clearStoredToken();
    throw new AuthError("로그인이 필요합니다.");
  }
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(`${options.method || "GET"} ${path} failed: ${res.status} ${detail}`);
  }
  if (res.status === 204) return null;
  return res.json();
}

export async function login(username, password) {
  const res = await fetch(`${BASE_URL}/auth/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username, password }),
  });
  if (!res.ok) {
    // 429(로그인 시도 제한)처럼 원인이 다른 경우를 뭉뚱그리지 않고, 백엔드가
    // 준 실제 사유(detail)를 그대로 보여준다 — 예전에 CORS 에러를 "비밀번호
    // 틀림"으로 오해했던 것과 같은 문제가 재발하지 않게.
    // detail은 401일 땐 문자열, 429(잠금)일 땐 {message, retry_after_seconds} 객체.
    const body = await res.json().catch(() => null);
    const detail = body?.detail;
    const message =
      (typeof detail === "string" ? detail : detail?.message) ||
      "아이디 또는 비밀번호가 올바르지 않습니다.";
    const err = new Error(message);
    if (detail && typeof detail === "object" && typeof detail.retry_after_seconds === "number") {
      err.retryAfterSeconds = detail.retry_after_seconds;
    }
    throw err;
  }
  const data = await res.json();
  setStoredToken(data.token);
  return data;
}

export async function logout() {
  const token = getStoredToken();
  clearStoredToken();
  if (!token) return;
  try {
    await fetch(`${BASE_URL}/auth/logout`, {
      method: "POST",
      headers: { "X-Admin-Key": token },
    });
  } catch {
    // 서버에 로그아웃 통보 실패해도 로컬 토큰은 이미 지웠으니 로그인 화면으로는 넘어감
  }
}

export const api = {
  getStatus: () => request("/status"),
  getRecentDetections: () => request("/recent-detections"),

  getQueue: () => request("/queue"),
  approveQueueItem: (id) => request(`/queue/${id}/approve`, { method: "POST" }),
  rejectQueueItem: (id) => request(`/queue/${id}/reject`, { method: "POST" }),

  // Rules - 새 API 형식
  getRules: async () => {
    const data = await request("/rules");
    // classification + decision을 합쳐서 flat array로 반환
    const clf = (data.classification || []).map(r => ({
      ...r,
      id: r.rule_id,
      target: r.resource_types?.join(", ") || "",
      condition: JSON.stringify(r.conditions || {}),
      result: r.result?.anomaly_type || r.result?.selected_action || "",
      source: r.author === "auto-promoted" ? "llm" : "human",
    }));
    const dec = (data.decision || []).map(r => ({
      ...r,
      id: r.rule_id,
      target: r.resource_types?.join(", ") || "",
      condition: JSON.stringify(r.conditions || {}),
      result: r.result?.selected_action || "",
      source: r.author === "auto-promoted" ? "llm" : "human",
    }));
    return [...clf, ...dec];
  },
  createRule: (rule) => request("/rules/classification", { method: "POST", body: JSON.stringify({
    description: `${rule.target} ${rule.condition} -> ${rule.result}`,
    resource_types: [rule.target],
    conditions: { custom: rule.condition },
    result: { anomaly_type: rule.result },
    priority: 50,
    rationale: "관리자가 수동 추가",
  })}),
  deleteRule: (id) => request(`/rules/${id}`, { method: "DELETE" }),
  toggleRule: (id) => request(`/rules/${id}/toggle`, { method: "PATCH" }),

  // Whitelist - 새 API 형식
  getWhitelist: async () => {
    const data = await request("/whitelist");
    return data.map(e => ({
      ...e,
      id: e.entry_id,
      pattern: e.resource_id,
    }));
  },
  createWhitelistEntry: async (entry) => {
    const data = await request("/whitelist", { method: "POST", body: JSON.stringify({
      resource_id: entry.pattern,
      resource_type: entry.resource_type,
      reason: entry.reason,
      expires_at: entry.expires_at,
    })});
    return { ...data, id: data.entry_id, pattern: data.resource_id };
  },
  deleteWhitelistEntry: (id) => request(`/whitelist/${id}`, { method: "DELETE" }),

  // Promotions (승인 대기 규칙)
  getPromotions: () => request("/promotions"),
  approvePromotion: (id) => request(`/promotions/${id}/approve`, { method: "POST" }),
  rejectPromotion: (id) => request(`/promotions/${id}/reject`, { method: "POST" }),

  getLogs: () => request("/logs"),
  getFailures: () => request("/failures"),

  getSettings: () => request("/settings"),
  updateSettings: (patch) =>
    request("/settings", { method: "PATCH", body: JSON.stringify(patch) }),
};

export { AuthError };
