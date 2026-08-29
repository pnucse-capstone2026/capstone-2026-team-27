import { useEffect, useState, useCallback } from "react";
import Header from "./components/Header.jsx";
import Dashboard from "./components/Dashboard.jsx";
import SettingsTab from "./components/SettingsTab.jsx";
import ApprovalQueue from "./components/ApprovalQueue.jsx";
import RuleBook from "./components/RuleBook.jsx";
import Whitelist from "./components/Whitelist.jsx";
import LlmLogs from "./components/LlmLogs.jsx";
import FailuresList from "./components/FailuresList.jsx";
import { api } from "./api.js";
import { colors, applyTheme, getStoredTheme } from "./styles.js";

export default function App() {
  const [activeTab, setActiveTab] = useState("dashboard");
  const [theme, setTheme] = useState(getStoredTheme);

  // applyTheme()는 colors 객체를 그 자리에서 바꿔치기(mutate)한다 — 렌더링 도중에
  // 바로 호출해야 이 렌더 사이클에서 만들어지는 모든 인라인 스타일이 새 테마를
  // 즉시 반영한다 (useEffect로 하면 한 프레임 늦게 반영돼서 깜빡임이 생김).
  applyTheme(theme);

  const [status, setStatus] = useState(null);
  const [statusLoading, setStatusLoading] = useState(true);
  const [recentDetections, setRecentDetections] = useState([]);

  const [queue, setQueue] = useState([]);
  const [rules, setRules] = useState([]);
  const [whitelist, setWhitelist] = useState([]);
  const [logs, setLogs] = useState([]);
  const [failures, setFailures] = useState([]);
  const [settings, setSettings] = useState(null);

  const refreshStatus = useCallback(() => {
    api
      .getStatus()
      .then(setStatus)
      .catch(console.error)
      .finally(() => setStatusLoading(false));
    api.getRecentDetections().then(setRecentDetections).catch(console.error);
  }, []);

  useEffect(() => {
    refreshStatus();
    const interval = setInterval(refreshStatus, 5000);
    return () => clearInterval(interval);
  }, [refreshStatus]);

  useEffect(() => {
    api.getQueue().then(setQueue).catch(console.error);
    api.getRules().then(setRules).catch(console.error);
    api.getWhitelist().then(setWhitelist).catch(console.error);
    api.getLogs().then(setLogs).catch(console.error);
    api.getFailures().then(setFailures).catch(console.error);
    api.getSettings().then(setSettings).catch(console.error);
  }, []);

  // ── 승인 대기 ──
  function handleApprove(id) {
    setQueue((prev) => prev.filter((q) => q.id !== id));
    api.approveQueueItem(id).catch((err) => {
      console.error(err);
      api.getQueue().then(setQueue);
    });
  }

  function handleReject(id) {
    setQueue((prev) => prev.filter((q) => q.id !== id));
    api.rejectQueueItem(id).catch((err) => {
      console.error(err);
      api.getQueue().then(setQueue);
    });
  }

  // ── Rule Book ──
  function handleCreateRule(rule) {
    const tempId = `temp-${Date.now()}`;
    setRules((prev) => [...prev, { ...rule, id: tempId }]);
    api
      .createRule(rule)
      .then((created) => setRules((prev) => prev.map((r) => (r.id === tempId ? created : r))))
      .catch((err) => {
        console.error(err);
        setRules((prev) => prev.filter((r) => r.id !== tempId));
      });
  }

  function handleDeleteRule(id) {
    const prevRules = rules;
    setRules((prev) => prev.filter((r) => r.id !== id));
    api.deleteRule(id).catch((err) => {
      console.error(err);
      setRules(prevRules);
    });
  }

  function handleToggleRule(id) {
    setRules((prev) => prev.map((r) => (r.id === id ? { ...r, enabled: !r.enabled } : r)));
    api.toggleRule(id).catch((err) => {
      console.error(err);
      api.getRules().then(setRules);
    });
  }

  // ── 화이트리스트 ──
  function handleCreateWhitelist(entry) {
    const tempId = `temp-${Date.now()}`;
    setWhitelist((prev) => [...prev, { ...entry, id: tempId }]);
    api
      .createWhitelistEntry(entry)
      .then((created) => setWhitelist((prev) => prev.map((w) => (w.id === tempId ? created : w))))
      .catch((err) => {
        console.error(err);
        setWhitelist((prev) => prev.filter((w) => w.id !== tempId));
      });
  }

  function handleDeleteWhitelist(id) {
    const prevWhitelist = whitelist;
    setWhitelist((prev) => prev.filter((w) => w.id !== id));
    api.deleteWhitelistEntry(id).catch((err) => {
      console.error(err);
      setWhitelist(prevWhitelist);
    });
  }

  // ── 설정 ──
  function handleUpdateSettings(patch) {
    setSettings((prev) => ({ ...prev, ...patch }));
    api.updateSettings(patch).catch((err) => {
      console.error(err);
      api.getSettings().then(setSettings);
    });
  }

  return (
    <div
      style={{
        display: "flex",
        minHeight: "100vh",
        background: colors.bg,
        color: colors.text,
        fontFamily: "system-ui, sans-serif",
      }}
    >
      <Header
        activeTab={activeTab}
        onTabChange={setActiveTab}
        pipelineRunning={status?.pipeline_running ?? false}
        pendingCount={queue.length}
        theme={theme}
        onToggleTheme={() => setTheme((t) => (t === "dark" ? "light" : "dark"))}
      />
      <div style={{ padding: 24, flex: 1, minWidth: 0 }}>
        {activeTab === "dashboard" && (
          <Dashboard
            status={status}
            loading={statusLoading}
            recentDetections={recentDetections}
            onNavigateToLogs={() => setActiveTab("logs")}
            onNavigateToFailures={() => setActiveTab("failures")}
          />
        )}
        {activeTab === "settings" && <SettingsTab settings={settings} onUpdate={handleUpdateSettings} />}
        {activeTab === "queue" && (
          <ApprovalQueue queue={queue} onApprove={handleApprove} onReject={handleReject} />
        )}
        {activeTab === "rules" && (
          <RuleBook
            rules={rules}
            onCreate={handleCreateRule}
            onDelete={handleDeleteRule}
            onToggle={handleToggleRule}
          />
        )}
        {activeTab === "whitelist" && (
          <Whitelist entries={whitelist} onCreate={handleCreateWhitelist} onDelete={handleDeleteWhitelist} />
        )}
        {activeTab === "logs" && <LlmLogs logs={logs} />}
        {activeTab === "failures" && <FailuresList failures={failures} />}
      </div>
    </div>
  );
}
