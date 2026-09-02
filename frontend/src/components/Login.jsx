import { useState, useEffect, useRef } from "react";
import { card, colors, button, inputStyle, font, labelStyle, gridBackground } from "../styles.js";
import { login } from "../api.js";

export default function Login({ onSuccess }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [checking, setChecking] = useState(false);
  const [lockoutUntil, setLockoutUntil] = useState(null);
  const [remainingSeconds, setRemainingSeconds] = useState(0);
  const intervalRef = useRef(null);

  useEffect(() => {
    if (!lockoutUntil) return;

    function tick() {
      const secondsLeft = Math.max(0, Math.ceil((lockoutUntil - Date.now()) / 1000));
      setRemainingSeconds(secondsLeft);
      if (secondsLeft <= 0) {
        clearInterval(intervalRef.current);
        setLockoutUntil(null);
        setError("");
      }
    }

    tick();
    intervalRef.current = setInterval(tick, 1000);
    return () => clearInterval(intervalRef.current);
  }, [lockoutUntil]);

  async function handleSubmit(e) {
    e.preventDefault();
    if (!username.trim() || !password || remainingSeconds > 0) return;

    setChecking(true);
    setError("");

    try {
      await login(username.trim(), password);
      onSuccess();
    } catch (err) {
      if (typeof err.retryAfterSeconds === "number") {
        setLockoutUntil(Date.now() + err.retryAfterSeconds * 1000);
      }
      setError(err.message || "아이디 또는 비밀번호가 올바르지 않습니다.");
    } finally {
      setChecking(false);
    }
  }

  return (
    <div
      style={{
        minHeight: "100vh",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        fontFamily: font.display,
        ...gridBackground(),
      }}
    >
      <form
        onSubmit={handleSubmit}
        style={{ ...card(), width: 320, borderLeft: `3px solid ${colors.accent}` }}
      >
        <div style={{ ...labelStyle, color: colors.accent }}>DETECTION</div>
        <div style={{ fontSize: 20, fontWeight: 700, color: colors.text, marginTop: 6, marginBottom: 20 }}>
          관리자 로그인
        </div>

        <input
          type="text"
          autoFocus
          value={username}
          onChange={(e) => setUsername(e.target.value)}
          placeholder="아이디"
          style={{ ...inputStyle(), width: "100%", boxSizing: "border-box" }}
        />
        <input
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          placeholder="비밀번호"
          style={{ ...inputStyle(), width: "100%", boxSizing: "border-box", marginTop: 10 }}
        />

        {error && (
          <div style={{ color: "#f87171", fontSize: 13, marginTop: 10 }}>
            {remainingSeconds > 0
              ? `로그인 시도가 너무 많습니다. ${remainingSeconds}초 후 다시 시도해주세요.`
              : error}
          </div>
        )}

        <button
          type="submit"
          disabled={checking || remainingSeconds > 0}
          style={{ ...button.base(), ...button.primary(), width: "100%", marginTop: 16 }}
        >
          {remainingSeconds > 0 ? `${remainingSeconds}초 후 다시 시도` : checking ? "확인 중..." : "로그인"}
        </button>
      </form>
    </div>
  );
}
