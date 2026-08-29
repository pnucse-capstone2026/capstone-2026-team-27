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

  getRules: () => request("/rules"),
  createRule: (rule) => request("/rules", { method: "POST", body: JSON.stringify(rule) }),
  deleteRule: (id) => request(`/rules/${id}`, { method: "DELETE" }),
  toggleRule: (id) => request(`/rules/${id}/toggle`, { method: "PATCH" }),

  getWhitelist: () => request("/whitelist"),
  createWhitelistEntry: (entry) =>
    request("/whitelist", { method: "POST", body: JSON.stringify(entry) }),
  deleteWhitelistEntry: (id) => request(`/whitelist/${id}`, { method: "DELETE" }),

  getLogs: () => request("/logs"),
  getFailures: () => request("/failures"),

  getSettings: () => request("/settings"),
  updateSettings: (patch) =>
    request("/settings", { method: "PATCH", body: JSON.stringify(patch) }),
};
