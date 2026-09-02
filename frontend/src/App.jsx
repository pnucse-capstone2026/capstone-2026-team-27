import { useEffect, useState, useCallback } from "react";
import Header from "./components/Header.jsx";
import Dashboard from "./components/Dashboard.jsx";
import SettingsTab from "./components/SettingsTab.jsx";
import ApprovalQueue from "./components/ApprovalQueue.jsx";
import RuleBook from "./components/RuleBook.jsx";
import Whitelist from "./components/Whitelist.jsx";
import PromotionsQueue from "./components/PromotionsQueue.jsx";
import LlmLogs from "./components/LlmLogs.jsx";
import FailuresList from "./components/FailuresList.jsx";
import Login from "./components/Login.jsx";
import { api, AuthError, getStoredToken, logout as apiLogout } from "./api.js";
import { colors, applyTheme, getStoredTheme, font, gridBackground } from "./styles.js";

export default function App() {
  const [isAuthed, setIsAuthed] = useState(() => !!getStoredToken());
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
  const [promotions, setPromotions] = useState({ classification: [], decision: [] });
  const [logs, setLogs] = useState([]);
  const [failures, setFailures] = useState([]);
  const [settings, setSettings] = useState(null);

  // 401(AuthError) 받으면 로그인 화면으로 돌려보냄, 그 외 에러는 그냥 콘솔에만
  const handleError = useCallback((err) => {
    if (err instanceof AuthError) {
      setIsAuthed(false);
    } else {
      console.error(err);
    }
  }, []);

  const refreshStatus = useCallback(() => {
    if (!isAuthed) return;
    api.getStatus().then(setStatus).catch(handleError).finally(() => setStatusLoading(false));
    api.getRecentDetections().then(setRecentDetections).catch(handleError);
  }, [isAuthed, handleError]);

  useEffect(() => {
    if (!isAuthed) return;
    refreshStatus();
    const interval = setInterval(refreshStatus, 5000);
    return () => clearInterval(interval);
  }, [isAuthed, refreshStatus]);

  useEffect(() => {
    if (!isAuthed) return;
    api.getQueue().then(setQueue).catch(handleError);
    api.getRules().then(setRules).catch(handleError);
    api.getWhitelist().then(setWhitelist).catch(handleError);
    api.getPromotions().then(setPromotions).catch(handleError);
    api.getLogs().then(setLogs).catch(handleError);
    api.getFailures().then(setFailures).catch(handleError);
    api.getSettings().then(setSettings).catch(handleError);
  }, [isAuthed, handleError]);

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

  // ── Promotions (규칙 승격 승인) ──
  function handleApprovePromotion(id) {
    api.approvePromotion(id)
      .then(() => {
        api.getPromotions().then(setPromotions);
        api.getRules().then(setRules);
      })
      .catch(console.error);
  }

  function handleRejectPromotion(id) {
    api.rejectPromotion(id)
      .then(() => api.getPromotions().then(setPromotions))
      .catch(console.error);
  }

  // ── 설정 ──
  function handleUpdateSettings(patch) {
    setSettings((prev) => ({ ...prev, ...patch }));
    api.updateSettings(patch).catch((err) => {
      console.error(err);
      api.getSettings().then(setSettings);
    });
  }

  if (!isAuthed) {
    return <Login onSuccess={() => setIsAuthed(true)} />;
  }

  return (
    <div
      style={{
        display: "flex",
        minHeight: "100vh",
        ...gridBackground(),
        color: colors.text,
        fontFamily: font.display,
      }}
    >
      <Header
        activeTab={activeTab}
        onTabChange={setActiveTab}
        pipelineRunning={status?.pipeline_running ?? false}
        pendingCount={queue.length}
        promotionsCount={(promotions.classification?.length || 0) + (promotions.decision?.length || 0)}
        theme={theme}
        onToggleTheme={() => setTheme((t) => (t === "dark" ? "light" : "dark"))}
        onLogout={() => {
          apiLogout();
          setIsAuthed(false);
        }}
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
        {activeTab === "promotions" && (
          <PromotionsQueue promotions={promotions} onApprove={handleApprovePromotion} onReject={handleRejectPromotion} />
        )}
        {activeTab === "logs" && <LlmLogs logs={logs} />}
        {activeTab === "failures" && <FailuresList failures={failures} />}
      </div>
    </div>
  );
}
