import pandas as pd
import pytest

from alphasift.akshare_boards import fetch_board_list_frame


def test_fetch_board_list_frame_uses_em_when_available(monkeypatch):
    class FakeAkshare:
        @staticmethod
        def stock_board_industry_name_em():
            return pd.DataFrame([{"板块名称": "银行", "涨跌幅": 1.2, "排名": 3}])

        @staticmethod
        def stock_board_industry_summary_ths():
            raise AssertionError("ths should not be called when em succeeds")

    monkeypatch.setitem(__import__("sys").modules, "akshare", FakeAkshare)

    frame, backend, note = fetch_board_list_frame("industry")

    assert backend == "em"
    assert note is None
    assert frame is not None
    assert frame.iloc[0]["板块名称"] == "银行"


def test_fetch_board_list_frame_falls_back_to_ths_when_em_fails(monkeypatch):
    class FakeAkshare:
        @staticmethod
        def stock_board_concept_name_em():
            raise ConnectionError("Remote end closed connection without response")

        @staticmethod
        def stock_board_concept_name_ths():
            return pd.DataFrame([{"name": "AI算力", "code": "308001"}])

    monkeypatch.setitem(__import__("sys").modules, "akshare", FakeAkshare)

    frame, backend, note = fetch_board_list_frame("concept", max_retries=0)

    assert backend == "ths"
    assert frame is not None
    assert frame.iloc[0]["board_name"] == "AI算力"
    assert note is not None
    assert "fallback: ths" in note


def test_fetch_board_list_frame_returns_error_when_all_backends_fail(monkeypatch):
    class FakeAkshare:
        @staticmethod
        def stock_board_industry_name_em():
            raise ConnectionError("em down")

        @staticmethod
        def stock_board_industry_summary_ths():
            raise RuntimeError("ths down")

    monkeypatch.setitem(__import__("sys").modules, "akshare", FakeAkshare)

    frame, backend, note = fetch_board_list_frame("industry", max_retries=0)

    assert frame is None
    assert backend is None
    assert "em down" in (note or "")
    assert "ths down" in (note or "")
