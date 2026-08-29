"""
playground/generate_pipeline_diagram.py

pipeline/graph.py를 전혀 수정하지 않고, 이미 컴파일된 app(StateGraph)에서
Mermaid 다이어그램을 뽑아낸다. LangGraph는 그래프 구조 자체를 데이터로
갖고 있기 때문에, graph.py에 코드를 추가할 필요 없이 이 스크립트만으로
시각화가 가능하다.

실행: 프로젝트 루트에서 `python playground/generate_pipeline_diagram.py`
결과: playground/pipeline_diagram.mmd (Mermaid 소스) 파일로 저장 + 콘솔 출력
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.graph import app

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "pipeline_diagram.mmd")


def main():
    mermaid_src = app.get_graph().draw_mermaid()

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(mermaid_src)

    print(mermaid_src)
    print(f"\n[저장 완료] {OUTPUT_PATH}")
    print("(mermaid.live 같은 뷰어에 붙여넣거나, .md 파일 안에 ```mermaid 블록으로 넣으면 렌더링됨)")


if __name__ == "__main__":
    main()
