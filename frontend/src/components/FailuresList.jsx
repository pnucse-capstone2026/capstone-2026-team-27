import { card, colors, badgeStyle, SEVERITY_STYLES } from "../styles.js";

const STATUS_LABEL = {
  failed_qa: "QA 실패",
  rollback_exhausted: "롤백 소진",
};

// cpu_ok는 이름과 달리 실제로는 detection 단계에서 트리거된 지표(triggered_metrics)를
// 리소스에 안 가리고 범용으로 검사한 결과다(QA_agent.py 참고 - 필드명만 하위호환용으로
// cpu_ok 유지). 그래서 항상 "CPU"라고 라벨을 고정하면 EC2가 아닌 리소스에서는 틀린
// 라벨이 나온다 - sla_detail 문자열에서 실제로 검사한 지표명을 뽑아 동적으로 보여준다.
const METRIC_LABEL = {
  cpu_utilization: "CPU", network_in: "네트워크 수신", network_out: "네트워크 송신",
  bytes_downloaded: "다운로드량", number_of_requests: "요청수", invocation_count: "호출수",
  error_count: "에러수", group_desired_capacity: "목표용량", group_in_service_instances: "가동인스턴스",
  request_count: "트래픽", database_connections: "DB연결수", read_iops: "읽기IOPS",
  write_iops: "쓰기IOPS", duration_avg: "평균실행시간", cost: "비용",
};

function metricNamesFromDetail(detail) {
  if (!detail) return [];
  // "cpu_utilization 3.0 <= 5.0%, network_in 105.0 <= 기준선 100.0*1.5" 형태에서
  // 콤마로 구분된 각 구간의 첫 단어(지표명)만 뽑는다.
  return detail
    .split(",")
    .map((seg) => seg.trim().split(" ")[0])
    .filter((name) => name && METRIC_LABEL[name]);
}

function dynamicMetricLabel(detail) {
  const names = metricNamesFromDetail(detail);
  if (names.length === 0) return "메트릭"; // 파싱 실패 시 동적으로라도 중립적인 이름
  return names.map((n) => METRIC_LABEL[n]).join("/");
}

const SLA_LABEL = {
  cost_ok: "비용",
  availability_ok: "가용성",
};

function SlaBadges({ failure }) {
  const badges = [];

  // cpu_ok는 실제 검사 대상 지표가 리소스마다 다르므로 라벨을 동적으로 계산
  if (failure.cpu_ok !== null && failure.cpu_ok !== undefined) {
    badges.push({ key: "cpu_ok", label: dynamicMetricLabel(failure.sla_detail), ok: failure.cpu_ok });
  }
  for (const [key, label] of Object.entries(SLA_LABEL)) {
    const ok = failure[key];
    if (ok === null || ok === undefined) continue;
    badges.push({ key, label, ok });
  }

  return (
    <div style={{ display: "flex", gap: 6 }}>
      {badges.map(({ key, label, ok }) => (
        <span
          key={key}
          style={{
            fontSize: 11,
            padding: "2px 8px",
            borderRadius: 999,
            background: ok ? "#e0f2fe" : "#fee2e2",
            color: ok ? "#075985" : "#991b1b",
          }}
        >
          {label} {ok ? "정상" : "위반"}
        </span>
      ))}
    </div>
  );
}

function FailureCard({ failure }) {
  return (
    <div style={{ ...card(), display: "flex", flexDirection: "column", gap: 10 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <span style={badgeStyle(SEVERITY_STYLES, failure.risk_level)}>{failure.risk_level}</span>
          <div>
            <div style={{ fontWeight: 700, color: colors.text }}>
              {failure.selected_action} · {failure.resource_type}
            </div>
            <div style={{ fontSize: 12, color: colors.subtext }}>{failure.resource_id}</div>
          </div>
        </div>
        <div style={{ textAlign: "right" }}>
          <div style={{ fontSize: 12, color: colors.subtext }}>
            {failure.timestamp ? new Date(failure.timestamp).toLocaleString("ko-KR") : ""}
          </div>
          <div style={{ fontSize: 12, fontWeight: 700, color: "#991b1b" }}>
            {STATUS_LABEL[failure.status] || failure.status}
            {failure.rollback_count ? ` (롤백 ${failure.rollback_count}회)` : ""}
          </div>
        </div>
      </div>

      <SlaBadges failure={failure} />

      {failure.sla_detail && (
        <div style={{ fontSize: 13, color: colors.text, lineHeight: 1.5 }}>{failure.sla_detail}</div>
      )}
    </div>
  );
}

export default function FailuresList({ failures }) {
  if (failures.length === 0) {
    return <div style={{ color: colors.subtext }}>실패한 처리가 없습니다.</div>;
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16, width: "100%", maxWidth: 640, margin: "0 auto" }}>
      {failures.map((f) => (
        <FailureCard key={f.id} failure={f} />
      ))}
    </div>
  );
}
