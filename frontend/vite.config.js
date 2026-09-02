import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    // 3000번이 이미 사용 중이면 3001로 조용히 넘어가지 않고 바로 에러 내고 죽는다.
    // (조용히 다른 포트로 넘어가면 백엔드 CORS 허용 origin과 안 맞아서 로그인 실패처럼 보이는 엉뚱한 에러가 남)
    strictPort: true,
  },
});
