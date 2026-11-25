"""운세 프롬프트 구성 (LLM 입력 정규화)."""

from __future__ import annotations

from typing import Any

SYSTEM_PROMPT = (
    "너는 한국어 사주/운세 상담가다. 입력된 생년월일시와 운세 종류를 바탕으로 "
    "근거를 들어 담담하게 풀어 쓰고, 미래를 단정하지 않는다. "
    "출력은 '한 줄 제목'과 '본문 3문단'으로 구성한다."
)

FORTUNE_TYPE_LABEL = {
    "TODAY": "오늘의 운세",
    "LOVE": "연애운",
    "MONEY": "재물운",
    "WORK": "직장운",
    "YEARLY": "신년 총운",
}

GENDER_LABEL = {"M": "남성", "F": "여성", "U": "미상"}
CALENDAR_LABEL = {"SOLAR": "양력", "LUNAR": "음력"}


def build_user_prompt(reading_input: dict[str, Any], fortune_type: str) -> str:
    """reading_input 은 camelCase(by_alias) dict 를 그대로 받는다."""
    label = FORTUNE_TYPE_LABEL.get(fortune_type, fortune_type)
    birth = reading_input.get("birthDate", "미상")
    time = reading_input.get("birthTime") or "시간 미상"
    calendar = CALENDAR_LABEL.get(reading_input.get("calendar", "SOLAR"), "양력")
    gender = GENDER_LABEL.get(reading_input.get("gender", "U"), "미상")
    question = reading_input.get("question")

    lines = [
        f"운세 종류: {label}",
        f"생년월일시: {birth} {time} ({calendar})",
        f"성별: {gender}",
    ]
    if question:
        lines.append(f"특별 질문: {question}")
    lines.append("위 정보로 제목 한 줄과 본문 3문단을 작성해줘.")
    return "\n".join(lines)
