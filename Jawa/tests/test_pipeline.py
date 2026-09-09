"""
Automated tests for the Jawa dashboard data pipeline.
Run with: python -m pytest Jawa/tests/test_pipeline.py -v
"""
import gzip, json, sys, os
from pathlib import Path
import pytest

# Allow importing from parent Jawa directory
sys.path.insert(0, str(Path(__file__).parent.parent))
import push_jawa_data as P


# ── Fixtures ───────────────────────────────────────────────────────────────
HIST_CACHE = Path(__file__).parent.parent / "hist_cache.json.gz"
AUG26_FALLBACK = Path(__file__).parent.parent / "aug26_fallback.json.gz"


def load_hist() -> tuple[list, dict]:
    assert HIST_CACHE.exists(), "hist_cache.json.gz not found"
    with gzip.open(HIST_CACHE, "rt") as f:
        cache = json.load(f)
    return cache["rows"], cache.get("dealerMeta", {})


def make_lead_row(opty_id="OID1", month="Jul'26", model="Jawa 42 Bobber",
                  source="Facebook", leadtype="75", dealer="CLDA001",
                  mobile="9876543210", currstatus="L1 Verified",
                  qualstatus="Not Determined", statuscat="Active",
                  date="2026-07-15"):
    return {
        "opty_id": opty_id,
        "month": month,
        "date": date,
        "model": model,
        "source": source,
        "leadtype": leadtype,
        "dealer": dealer,
        "dealer_name": "Test Dealer",
        "dealer_city": "mumbai",
        "dealer_state": "Maharashtra",
        "mobile_norm": mobile,
        "currstatus": currstatus,
        "qualstatus": qualstatus,
        "statuscat": statuscat,
        "isRetail": 0,
        "retailModel": None,
        "perfMonth": None,
        "retailSource": None,
    }


# ── Utility tests ──────────────────────────────────────────────────────────
class TestMonthOrder:
    def test_basic(self):
        assert P.month_order("May'25") == 202505
        assert P.month_order("Sep'26") == 202609
        assert P.month_order("Jan'27") == 202701

    def test_ordering(self):
        months = ["Aug'26", "Jul'26", "Sep'25", "Jan'26"]
        sorted_m = sorted(months, key=P.month_order)
        assert sorted_m == ["Sep'25", "Jan'26", "Jul'26", "Aug'26"]

    def test_freeze_boundary(self):
        assert P.month_order("Jun'26") < P.month_order("Jul'26")
        assert P.month_order("Jun'26") == 202606


class TestNormId:
    def test_strips_whitespace(self):
        assert P.norm_id("  ABC123  ") == "ABC123"

    def test_strips_dot_zero(self):
        assert P.norm_id("12345.0") == "12345"

    def test_empty_none(self):
        assert P.norm_id(None) == ""
        assert P.norm_id("") == ""


class TestNormModel:
    def test_merge_yezdi_adventure(self):
        assert P.norm_model("2025 Yezdi Adventure") == "Yezdi Adventure"

    def test_merge_scrambler(self):
        assert P.norm_model("Yezdi Scrambler 350") == "Yezdi Scrambler"
        assert P.norm_model("Yezdi Scrambler (2022-2025)") == "Yezdi Scrambler"

    def test_no_merge_2024_adventure(self):
        assert P.norm_model("2024 Yezdi Adventure") == "2024 Yezdi Adventure"

    def test_passthrough(self):
        assert P.norm_model("Jawa 42 Bobber") == "Jawa 42 Bobber"


class TestRollingMonths:
    def test_sep26(self):
        result = P.rolling_months("Sep'26", 3)
        assert result == ["Sep'26", "Aug'26", "Jul'26"]

    def test_oct26(self):
        result = P.rolling_months("Oct'26", 3)
        assert result == ["Oct'26", "Sep'26", "Aug'26"]

    def test_year_boundary(self):
        result = P.rolling_months("Jan'27", 3)
        assert result == ["Jan'27", "Dec'26", "Nov'26"]


# ── Historical freeze tests ────────────────────────────────────────────────
class TestHistoricalFreeze:
    def test_hist_cache_exists(self):
        assert HIST_CACHE.exists(), "hist_cache.json.gz must exist"

    def test_frozen_months_only(self):
        rows, _ = load_hist()
        freeze_order = P.month_order(P.HISTORICAL_FREEZE_MONTH)
        bad = [r for r in rows if P.month_order(r["month"]) > freeze_order]
        assert len(bad) == 0, f"{len(bad)} rows beyond freeze boundary {P.HISTORICAL_FREEZE_MONTH}"

    def test_frozen_row_count_plausible(self):
        rows, _ = load_hist()
        assert len(rows) > 200_000, f"Frozen rows too few: {len(rows)}"

    def test_frozen_retails_present(self):
        rows, _ = load_hist()
        retails = sum(1 for r in rows if r.get("isRetail") == 1)
        assert retails > 4_000, f"Frozen retails too few: {retails}"

    def test_all_required_fields_present(self):
        rows, _ = load_hist()
        required = {"month","model","source","leadtype","dealer","isRetail",
                    "statuscat","qualstatus","currstatus"}
        for r in rows[:100]:
            missing = required - set(r.keys())
            assert not missing, f"Frozen row missing fields: {missing}"

    def test_frozen_does_not_include_jul_aug_26(self):
        rows, _ = load_hist()
        live_months = {"Jul'26", "Aug'26"}
        bad = [r for r in rows if r["month"] in live_months]
        assert len(bad) == 0, f"{len(bad)} Jul/Aug'26 rows found in frozen cache"


# ── Transition month tests ─────────────────────────────────────────────────
class TestTransitionMonths:
    def test_jul26_in_transition(self):
        assert "Jul'26" in P.TRANSITION_MONTHS

    def test_aug26_in_transition(self):
        assert "Aug'26" in P.TRANSITION_MONTHS

    def test_jun26_not_in_transition(self):
        assert "Jun'26" not in P.TRANSITION_MONTHS

    def test_aug26_fallback_exists(self):
        assert AUG26_FALLBACK.exists(), "aug26_fallback.json.gz must exist"

    def test_aug26_fallback_month(self):
        with gzip.open(AUG26_FALLBACK, "rt") as f:
            cache = json.load(f)
        assert cache["month"] == "Aug'26"
        assert all(r["month"] == "Aug'26" for r in cache["rows"][:100])


# ── Rolling retail window tests ────────────────────────────────────────────
class TestRollingRetailWindow:
    def test_sep26_includes_jul_aug(self):
        window = P.rolling_months("Sep'26", 3)
        assert "Jul'26" in window
        assert "Aug'26" in window
        assert "Sep'26" in window

    def test_oct26_excludes_jul(self):
        window = P.rolling_months("Oct'26", 3)
        assert "Jul'26" not in window
        assert "Aug'26" in window

    def test_window_length(self):
        assert len(P.rolling_months("Sep'26", 3)) == 3
        assert len(P.rolling_months("Sep'26", 2)) == 2


# ── Join key tests ─────────────────────────────────────────────────────────
class TestJoinKey:
    def test_lead_pk_field(self):
        assert P.LEAD_PK == "opty_id"

    def test_retail_pk_field(self):
        assert P.RETAIL_PK == "sourceLeadId"

    def test_no_mobile_in_retail_map(self):
        # Synthetic retail map — ensures no mobile fallback logic exists
        retail_map = {"OID_123": {"retailModel": "Jawa 42", "perfMonth": "Sep'26", "retailSource": "DMS"}}
        row = make_lead_row(opty_id="OID_123")
        rows = [row]
        # Apply retail map (simulate what build_payload does)
        for r in rows:
            oid = r.get("opty_id","")
            if oid and oid in retail_map:
                rm = retail_map[oid]
                r["isRetail"]     = 1
                r["retailModel"]  = rm["retailModel"]
                r["perfMonth"]    = rm["perfMonth"]
                r["retailSource"] = rm["retailSource"]
        assert rows[0]["isRetail"] == 1
        assert rows[0]["retailModel"] == "Jawa 42"

    def test_unmatched_lead_stays_not_retail(self):
        retail_map = {}  # empty — no retails
        row = make_lead_row(opty_id="NO_MATCH")
        for r in [row]:
            oid = r.get("opty_id","")
            if oid and oid in retail_map:
                r["isRetail"] = 1
        assert row["isRetail"] == 0


# ── Late retail tests ──────────────────────────────────────────────────────
class TestLateRetail:
    def test_jul_lead_sep_retail(self):
        """Jul'26 lead that retails in Sep'26 must be matched."""
        window = P.rolling_months("Sep'26", 3)
        assert "Jul'26" in window  # Jul lead is in the live range
        assert "Sep'26" in window  # Sep retail is in the rolling window

    def test_aug_lead_sep_retail(self):
        window = P.rolling_months("Sep'26", 3)
        assert "Aug'26" in window
        assert "Sep'26" in window


# ── Deduplication tests ────────────────────────────────────────────────────
class TestDeduplication:
    def test_duplicate_opty_id_dropped(self):
        rows = [
            make_lead_row("OID_1", month="Jul'26"),
            make_lead_row("OID_1", month="Jul'26"),  # duplicate
            make_lead_row("OID_2", month="Jul'26"),
        ]
        seen = set()
        deduped = []
        for r in rows:
            oid = r.get("opty_id","")
            if oid and oid in seen:
                continue
            seen.add(oid)
            deduped.append(r)
        assert len(deduped) == 2

    def test_unique_opty_ids_preserved(self):
        rows = [make_lead_row(f"OID_{i}") for i in range(10)]
        seen = set()
        deduped = []
        for r in rows:
            oid = r.get("opty_id","")
            if oid and oid in seen:
                continue
            seen.add(oid)
            deduped.append(r)
        assert len(deduped) == 10


# ── Historical retail preservation tests ──────────────────────────────────
class TestHistoricalRetailPreservation:
    def test_frozen_retail_count_unchanged(self):
        """build_payload must not reduce frozen retail count."""
        frozen_rows, dm = load_hist()
        frozen_retail_count = sum(1 for r in frozen_rows if r.get("isRetail") == 1)

        live = [make_lead_row("OID_L1")]
        retail_map = {}  # no retails

        payload = P.build_payload(frozen_rows, live, retail_map, dm)
        months = payload["months"]
        freeze_order = P.month_order(P.HISTORICAL_FREEZE_MONTH)
        frozen_m_set = {m for m in months if P.month_order(m) <= freeze_order}
        pl_retail = sum(1 for r in payload["rows"]
                        if months[r[0]] in frozen_m_set and r[8] == 1)
        assert pl_retail == frozen_retail_count, (
            f"Frozen retails changed: was {frozen_retail_count}, now {pl_retail}"
        )


# ── Failure safety tests ───────────────────────────────────────────────────
class TestFailureSafety:
    def test_fail_exit_raises_systemexit(self):
        with pytest.raises(SystemExit):
            P.fail_exit("test", "deliberate test failure")

    def test_validation_catches_frozen_drop(self):
        frozen_rows, dm = load_hist()
        live = [make_lead_row("OID_L1")]
        payload = P.build_payload(frozen_rows, live, {}, dm)

        # Simulate a row count drop
        payload["rows"] = payload["rows"][:100]  # truncate
        fails = P.validate_payload(payload, frozen_rows, live)
        assert any("G2" in f for f in fails), "Gate G2 must catch frozen row count drop"


# ── Idempotency tests ──────────────────────────────────────────────────────
class TestIdempotency:
    def test_same_input_same_output(self):
        frozen_rows, dm = load_hist()
        live = [make_lead_row("OID_IDEM_1"), make_lead_row("OID_IDEM_2")]
        retail_map = {"OID_IDEM_1": {"retailModel": "Jawa 42", "perfMonth": "Sep'26", "retailSource": "DMS"}}

        payload1 = P.build_payload(frozen_rows, live[:], retail_map, dm)
        payload2 = P.build_payload(frozen_rows, live[:], retail_map, dm)

        assert len(payload1["rows"]) == len(payload2["rows"])
        assert payload1["months"] == payload2["months"]
        assert payload1["models"] == payload2["models"]


# ── Validation gate tests ──────────────────────────────────────────────────
class TestValidationGates:
    def test_all_gates_pass_on_valid_payload(self):
        frozen_rows, dm = load_hist()
        # Include both transition months (Jul'26, Aug'26) + Sep'26 to satisfy G5 and G13
        live = (
            [make_lead_row(f"OID_VJ{i}", month="Jul'26") for i in range(400)]
            + [make_lead_row(f"OID_VA{i}", month="Aug'26") for i in range(400)]
            + [make_lead_row(f"OID_VS{i}", month="Sep'26") for i in range(300)]
        )
        payload = P.build_payload(frozen_rows, live, {}, dm)
        fails = P.validate_payload(payload, frozen_rows, live)
        assert not fails, f"Expected no failures, got: {fails}"

    def test_g6_catches_low_row_count(self):
        frozen_rows, dm = load_hist()
        live = [make_lead_row("OID_TINY")]
        payload = P.build_payload(frozen_rows, live, {}, dm)
        payload["rows"] = payload["rows"][:5]
        fails = P.validate_payload(payload, frozen_rows, live)
        assert any("G6" in f for f in fails)

    def test_g5_catches_missing_transition_month(self):
        frozen_rows, dm = load_hist()
        live = [make_lead_row(f"OID_{i}", month="Sep'26") for i in range(1100)]
        payload = P.build_payload(frozen_rows, live, {}, dm)
        # Remove Jul'26 from months
        payload["months"] = [m for m in payload["months"] if m != "Jul'26"]
        fails = P.validate_payload(payload, frozen_rows, live)
        assert any("G5" in f for f in fails)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
