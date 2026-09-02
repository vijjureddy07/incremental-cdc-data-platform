"""Unit tests for CDC event model contracts."""


from src.cdc.models import CDCEvent, CDCOperation


def test_cdc_event_creation_and_dict_conversion():
    """Verify CDCEvent initialization and serialization to dictionary."""
    ev = CDCEvent(
        event_id="evt_001",
        table_name="accounts",
        operation=CDCOperation.INSERT.value,
        business_key={"account_id": "ACC-0001"},
        sequence_number=1,
        event_timestamp="2026-01-15T10:00:00Z",
        source_commit_timestamp="2026-01-15T10:00:01Z",
        batch_id="batch_001",
        payload={"account_id": "ACC-0001", "status": "ACTIVE"},
        before_payload=None,
        source_system="b2b_saas_postgres",
    )

    ev_dict = ev.to_dict()
    assert ev_dict["event_id"] == "evt_001"
    assert ev_dict["operation"] == "INSERT"
    assert ev_dict["business_key"] == {"account_id": "ACC-0001"}
    assert ev_dict["before_payload"] is None
    assert ev_dict["payload"]["status"] == "ACTIVE"

    restored_ev = CDCEvent.from_dict(ev_dict)
    assert restored_ev.event_id == "evt_001"
    assert restored_ev.sequence_number == 1
    assert restored_ev.source_system == "b2b_saas_postgres"


def test_cdc_operation_enum():
    """Verify CDC operations enum validity checks."""
    assert CDCOperation.has_value("INSERT")
    assert CDCOperation.has_value("UPDATE")
    assert CDCOperation.has_value("DELETE")
    assert not CDCOperation.has_value("UPSERT")
    assert not CDCOperation.has_value("TRUNCATE")
