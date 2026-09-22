import { useEffect, useState } from "react";
import { colors, font } from "../styles.js";

function accentFor(text) {
  if (text.startsWith("↩️")) return "#e0654f"; // 롤백
  if (text.startsWith("✅")) return "#3ecf8e"; // 완료
  if (text.includes("확인이 필요")) return "#f5a623"; // 승인 필요
  return colors.accent; // 자동 처리(발견)
}

const AUTO_DISMISS_FADE_MS = 7000;
const AUTO_DISMISS_REMOVE_MS = 7500;

function ToastItem({ toast, onDone, onNavigateToQueue }) {
  const [leaving, setLeaving] = useState(false);
  const persistent = toast.event_type === "decision" && toast.requires_approval;

  useEffect(() => {
    if (persistent) return;
    const leaveTimer = setTimeout(() => setLeaving(true), AUTO_DISMISS_FADE_MS);
    const removeTimer = setTimeout(() => onDone(toast.event_id), AUTO_DISMISS_REMOVE_MS);
    return () => {
      clearTimeout(leaveTimer);
      clearTimeout(removeTimer);
    };
  }, [toast.event_id, onDone, persistent]);

  function handleClick() {
    if (!persistent) return;
    onNavigateToQueue?.();
    onDone(toast.event_id);
  }

  return (
    <div
      onClick={persistent ? handleClick : undefined}
      style={{
        display: "flex",
        alignItems: "center",
        gap: 10,
        minWidth: 280,
        maxWidth: 380,
        padding: "14px 16px",
        borderRadius: 4,
        background: colors.panelRaised,
        border: `1px solid ${colors.border}`,
        borderLeft: `4px solid ${accentFor(toast.text)}`,
        boxShadow: "0 8px 24px rgba(0,0,0,0.35)",
        color: colors.text,
        fontFamily: font.display,
        fontSize: 14,
        lineHeight: 1.4,
        cursor: persistent ? "pointer" : "default",
        transform: leaving ? "translateX(-24px)" : "translateX(0)",
        opacity: leaving ? 0 : 1,
        transition: "all 0.4s ease",
      }}
      title={persistent ? "클릭하면 승인 대기 탭으로 이동해요 (승인 대기열에서 처리하면 자동으로도 사라져요)" : undefined}
    >
      {toast.text}
    </div>
  );
}

export default function ToastStack({ toasts, onDismiss, onNavigateToQueue }) {
  if (!toasts.length) return null;
  return (
    <div
      style={{
        position: "fixed",
        top: 20,
        left: 240,
        zIndex: 9999,
        display: "flex",
        flexDirection: "column",
        gap: 10,
      }}
    >
      {toasts.map((t) => (
        <ToastItem key={t.event_id} toast={t} onDone={onDismiss} onNavigateToQueue={onNavigateToQueue} />
      ))}
    </div>
  );
}
