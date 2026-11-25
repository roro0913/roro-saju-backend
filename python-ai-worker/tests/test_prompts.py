"""캐시 키 정규화 / 프롬프트 구성 테스트."""

from app import keys
from app.fortune.prompts import build_user_prompt


def test_input_hash_is_order_insensitive() -> None:
    a = {"birthDate": "1993-09-13", "birthTime": "03:53", "calendar": "SOLAR", "gender": "M"}
    b = {"gender": "M", "calendar": "SOLAR", "birthTime": "03:53", "birthDate": "1993-09-13"}
    assert keys.input_hash(a, "LOVE") == keys.input_hash(b, "LOVE")


def test_input_hash_depends_on_fortune_type_and_question() -> None:
    base = {"birthDate": "1993-09-13", "birthTime": "03:53", "calendar": "SOLAR", "gender": "M"}
    assert keys.input_hash(base, "LOVE") != keys.input_hash(base, "MONEY")
    with_question = {**base, "question": "이직해도 될까요?"}
    assert keys.input_hash(base, "LOVE") != keys.input_hash(with_question, "LOVE")


def test_input_hash_is_32_chars() -> None:
    assert len(keys.input_hash({"birthDate": "1993-09-13"}, "TODAY")) == 32


def test_prompt_contains_normalised_fields() -> None:
    prompt = build_user_prompt(
        {"birthDate": "1993-09-13", "birthTime": None, "calendar": "LUNAR", "gender": "F"},
        "MONEY",
    )
    assert "재물운" in prompt
    assert "1993-09-13" in prompt
    assert "시간 미상" in prompt
    assert "음력" in prompt
    assert "여성" in prompt
