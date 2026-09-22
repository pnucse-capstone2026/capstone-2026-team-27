"""
여러 팀원이 각자 ec2_zombie_setup.py, lambda_retry_trial.py 등으로 실험을
돌리면, 결과 파일명이 타임스탬프만으로 만들어져서(예: ec2_zombie_manifest_20260914_170255.json)
우연히 같은 초에 실행하면 파일명이 겹쳐 덮어쓸 위험이 있다 - 실제로 여러
브랜치에서 같은 파일명이 확인됨(2026-09-22). 그래서 실행자 태그를 환경변수로
받아서 파일명 끝에 붙인다.

사용법: 실행 전에 본인 이름/이니셜을 환경변수로 지정
    export EXPERIMENT_RUNNER=soyoung   (bash)
    $env:EXPERIMENT_RUNNER = "soyoung" (PowerShell)
지정 안 하면 접미사 없이 기존과 동일하게 동작한다(하위 호환).
"""

from __future__ import annotations

import os
import re

_RUNNER = os.environ.get("EXPERIMENT_RUNNER", "").strip()
_SAFE_RUNNER = re.sub(r"[^a-zA-Z0-9_-]", "", _RUNNER)


def runner_suffix() -> str:
    """파일명에 붙일 접미사. EXPERIMENT_RUNNER 미설정 시 빈 문자열(기존 동작 유지)."""
    return f"__{_SAFE_RUNNER}" if _SAFE_RUNNER else ""
