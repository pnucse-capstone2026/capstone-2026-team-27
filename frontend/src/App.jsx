import { useEffect, useState, useCallback, useRef } from "react";
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
import ToastStack from "./components/Toast.jsx";
import { api, AuthError, getStoredToken, logout as apiLogout } from "./api.js";
import { colors, applyTheme, getStoredTheme, font, gridBackground } from "./styles.js";

// Vite HMR로 App 컴포넌트가 재마운트될 때 이전 폴링 인터벌이 남아 setInterval이 중복 실행되는 문제가 발생했다. 
// 모듈 스코프 변수로 인터벌 ID를 유지하고, 새 인터벌을 시작하기 전에 기존 인터벌을 정리해 중복 폴링을 방지한다.
let _activeNotifPollIntervalId = null;

export default function App() {
  const [isAuthed, setIsAuthed] = useState(() => !!getStoredToken());
  const [activeTab, setActiveTab] = useState("dashboard");
  const [theme, setTheme] = useState(getStoredTheme);

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
  const [pipelineProcess, setPipelineProcess] = useState(null); // {running, pid, started_at}
  const [pipelineActionPending, setPipelineActionPending] = useState(false);

  const [toasts, setToasts] = useState([]);
  const lastEventIdRef = useRef(0);
  const notifBaselineSetRef = useRef(false);
  const notifPollInFlightRef = useRef(false);
  // 승인/거부 클릭 직후에도 서버 체크포인트 반영이 살짝 늦을 수 있어서, 그 사이
  // 5초 폴링이 같은 항목을 큐에 되살려 보여주는 문제가 있었다(백엔드에도 별도
  // 가드를 추가했지만, 프론트에서도 한 번 누른 건 이 세션 동안 다시 안 뜨게
  // 이중으로 막는다).
  const locallyResolvedIdsRef = useRef(new Set());

  const dismissToast = useCallback((eventId) => {
    setToasts((prev) => prev.filter((t) => t.event_id !== eventId));
  }, []);

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
    api.getPipelineProcessStatus().then(setPipelineProcess).catch(handleError);
    api.getQueue().then((q) => {
      const filtered = q.filter((item) => !locallyResolvedIdsRef.current.has(item.id));
      setQueue(filtered);
      pruneResolvedApprovalToasts(filtered);
    }).catch(handleError);
    // [2026-09-28] 원래 getLogs()는 최초 로드 시 한 번만 불러서 새 로그가 쌓여도
    // 새로고침 전엔 안 보였다 — 다른 것들과 같은 5초 주기 폴링에 합류시킨다.
    api.getLogs().then(setLogs).catch(handleError);
  }, [isAuthed, handleError]);

  const pruneResolvedApprovalToasts = useCallback((currentQueue) => {
    const pendingResourceIds = new Set(currentQueue.map((q) => q.resource_id));
    setToasts((prev) =>
      prev.filter((t) => {
        const isPendingApprovalToast = t.event_type === "decision" && t.requires_approval;
        return !isPendingApprovalToast || pendingResourceIds.has(t.resource_id);
      })
    );
  }, []);

  useEffect(() => {
    if (!isAuthed) return;
    refreshStatus();
    const interval = setInterval(refreshStatus, 5000);
    return () => clearInterval(interval);
  }, [isAuthed, refreshStatus]);

  useEffect(() => {
    if (!isAuthed) return;

    const poll = () => {
      // baseline 설정 전(첫 호출)은 서버가 전체 이력을 events로 돌려줄 수 있는데,
      // 이 요청이 2초(폴링 주기) 안에 안 끝나면 다음 poll()이 겹쳐 실행돼서
      // baseline이 아직 안 잡힌 상태로 같은 전체 이력이 또 한 번 응답으로 와
      // 그대로 toast 처리돼버린다(과거 이력 전체가 한꺼번에 쏟아지는 원인이었음,
      // 2026-09-28 실측 확인) — in-flight 가드로 겹침 자체를 막는다.
      if (notifPollInFlightRef.current) return;
      notifPollInFlightRef.current = true;
      api
        .getRecentNotifications(lastEventIdRef.current)
        .then(({ events, latest_id, db_latest_id }) => {
          if (!notifBaselineSetRef.current) {
            lastEventIdRef.current = db_latest_id;
            notifBaselineSetRef.current = true;
            return;
          }
          if (events?.length) {
            setToasts((prev) => {
              const existingIds = new Set(prev.map((t) => t.event_id));
              const deduped = events.filter((e) => !existingIds.has(e.event_id));
              return [...deduped, ...prev];
            });
            lastEventIdRef.current = latest_id;
          }
        })
        .catch(() => {})
        .finally(() => {
          notifPollInFlightRef.current = false;
        });
    };
    // HMR 등으로 이전 인스턴스의 인터벌이 안 지워진 채 남아있으면 먼저 정리
    if (_activeNotifPollIntervalId !== null) {
      clearInterval(_activeNotifPollIntervalId);
    }
    poll();
    const interval = setInterval(poll, 2000);
    _activeNotifPollIntervalId = interval;
    return () => {
      clearInterval(interval);
      if (_activeNotifPollIntervalId === interval) {
        _activeNotifPollIntervalId = null;
      }
    };
  }, [isAuthed]);

  useEffect(() => {
    if (!isAuthed) return;
    api.getQueue().then(setQueue).catch(handleError);
    api.getRules().then(setRules).catch(handleError);
    api.getWhitelist().then(setWhitelist).catch(handleError);
    api.getPromotions().then(setPromotions).catch(handleError);
    // getLogs()는 refreshStatus()의 5초 폴링에 이미 포함됨(최초 마운트 때도 거기서 호출됨)
    api.getFailures().then(setFailures).catch(handleError);
    api.getSettings().then(setSettings).catch(handleError);
  }, [isAuthed, handleError]);

  // ── 승인 대기 ──
  function handleApprove(id) {
    locallyResolvedIdsRef.current.add(id);
    setQueue((prev) => {
      const next = prev.filter((q) => q.id !== id);
      pruneResolvedApprovalToasts(next);
      return next;
    });
    api.approveQueueItem(id).catch((err) => {
      console.error(err);
      // 실패했으면 다시 보여야 하니 로컬 제외 목록에서 빼고 원상복구
      locallyResolvedIdsRef.current.delete(id);
      api.getQueue().then(setQueue);
    });
  }

  function handleReject(id) {
    locallyResolvedIdsRef.current.add(id);
    setQueue((prev) => {
      const next = prev.filter((q) => q.id !== id);
      pruneResolvedApprovalToasts(next);
      return next;
    });
    api.rejectQueueItem(id).catch((err) => {
      console.error(err);
      locallyResolvedIdsRef.current.delete(id);
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

  function handleExportSettings() {
    return api.exportSettingsYaml();
  }

  // ── 파이프라인 실행/종료 ──
  function handleStartPipeline() {
    setPipelineActionPending(true);
    api.startPipeline()
      .then(setPipelineProcess)
      .catch((err) => alert(err.message || "파이프라인 시작 실패"))
      .finally(() => setPipelineActionPending(false));
  }

  function handleStopPipeline() {
    setPipelineActionPending(true);
    api.stopPipeline()
      .then(setPipelineProcess)
      .catch((err) => alert(err.message || "파이프라인 종료 실패"))
      .finally(() => setPipelineActionPending(false));
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
        pipelineRunning={pipelineProcess?.running ?? false}
        pendingCount={queue.length}
        promotionsCount={(promotions.classification?.length || 0) + (promotions.decision?.length || 0)}
        lastNormalCheckAt={status?.last_normal_check_at ?? null}
        nodesAsOf={status?.as_of ?? null}
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
        {activeTab === "settings" && (
          <SettingsTab
            settings={settings}
            onUpdate={handleUpdateSettings}
            onExport={handleExportSettings}
            pipelineProcess={pipelineProcess}
            pipelineActionPending={pipelineActionPending}
            onStartPipeline={handleStartPipeline}
            onStopPipeline={handleStopPipeline}
          />
        )}
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
      <ToastStack toasts={toasts} onDismiss={dismissToast} onNavigateToQueue={() => setActiveTab("queue")} />
    </div>
  );
}
