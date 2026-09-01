const BASE_URL = "http://localhost:8000";

async function request(path, options = {}) {
  const res = await fetch(`${BASE_URL}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(`${options.method || "GET"} ${path} failed: ${res.status} ${detail}`);
  }
  if (res.status === 204) return null;
  return res.json();
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
