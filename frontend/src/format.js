// $/hr 절감액 표시용 — 일반적인 값은 소수점 2자리로 충분하지만, S3 등 트래픽이
// 미미한 테스트 리소스는 실제 절감액이 $0.0000003/hr처럼 2자리에서 그냥 0으로
// 뭉개진다. 0이 아닌 값은 유효숫자가 보일 때까지 소수점 자리수를 늘린다.
export function formatUsdPerHour(value) {
  if (value === null || value === undefined || Number.isNaN(value)) return "0.00";
  if (value === 0) return "0.00";

  if (Math.abs(value) >= 0.01) return value.toFixed(2);

  let decimals = 2;
  while (decimals < 20 && parseFloat(value.toFixed(decimals)) === 0) {
    decimals += 1;
  }
  // 유효숫자가 막 나타나는 자리에서 끊으면 딱 1자리만 보이니, 한 자리 더 보여준다.
  return value.toFixed(Math.min(decimals + 1, 20));
}
