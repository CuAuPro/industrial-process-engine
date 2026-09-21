from industrial_process_engine.domain import (
    ProcessTimeRecord, ProductContext, ProductState, SyncState, WindowQuality, WindowRecord,
)
from industrial_process_engine.storage.sqlite import SQLiteStore


def add_completed_run(store, process_id, run_id, end_ts, sync_state):
    store.start_product(process_id, ProductContext(run_id, run_id, 0))
    store.persist_window(WindowRecord(
        process_id=process_id,
        run_id=run_id,
        product_id=run_id,
        segment_no=1,
        window_no=0,
        axis="distance",
        ts_start=0,
        ts_end=end_ts,
        elapsed_start_s=0,
        elapsed_end_s=end_ts / 1000,
        position_start_m=0,
        position_end_m=1,
        values={"temperature": 800},
        quality=WindowQuality.GOOD,
    ))
    store.complete_product(process_id, run_id, end_ts, None, {})
    store.set_sync_state(process_id, run_id, sync_state)


def test_retention_only_removes_expired_synced_products_and_old_events(config_factory):
    config = config_factory()
    store = SQLiteStore(config_factory.sqlite_path, config.aggregation_storage_schema)
    store.initialize()
    add_completed_run(store, "TEST_LINE", "OLD_SYNCED", 1_000, SyncState.SYNCED)
    add_completed_run(store, "TEST_LINE", "RECENT_SYNCED", 10_000, SyncState.SYNCED)
    add_completed_run(store, "TEST_LINE", "OLD_PENDING", 1_000, SyncState.PENDING)
    add_completed_run(store, "TEST_LINE", "OLD_FAILED", 1_000, SyncState.FAILED)
    store.start_product("TEST_LINE", ProductContext("ACTIVE", "ACTIVE", 0))
    store.log_event(1_000, "TEST_LINE", "OLD", "old event")
    store.log_event(10_000, "TEST_LINE", "RECENT", "recent event")
    store.log_event(1_000, "OTHER_LINE", "OLD", "other line event")
    store.persist_process_time(ProcessTimeRecord(
        "TEST_LINE", 0, 1_000, {}, WindowQuality.DATA_GAP,
    ))
    store.persist_process_time(ProcessTimeRecord(
        "TEST_LINE", 1_000, 2_000, {}, WindowQuality.DATA_GAP,
    ))
    store.set_process_time_sync_state("TEST_LINE", [0], SyncState.SYNCED)

    products, windows, events = store.apply_retention("TEST_LINE", 5_000, 5_000)

    assert (products, windows, events) == (1, 1, 1)
    assert store.get_product("TEST_LINE", "OLD_SYNCED", False) is None
    assert store.get_product("TEST_LINE", "RECENT_SYNCED", False) is not None
    assert store.get_product("TEST_LINE", "OLD_PENDING", False)["sync_state"] == "PENDING"
    assert store.get_product("TEST_LINE", "OLD_FAILED", False)["sync_state"] == "FAILED"
    assert store.get_product("TEST_LINE", "ACTIVE", False)["state"] == ProductState.ACTIVE
    with store.connection() as db:
        assert db.execute(
            "SELECT count(*) FROM event_log WHERE process_id='TEST_LINE' AND event_type='OLD'"
        ).fetchone()[0] == 0
        assert db.execute(
            "SELECT count(*) FROM event_log WHERE process_id='OTHER_LINE' AND event_type='OLD'"
        ).fetchone()[0] == 1
        assert db.execute(
            "SELECT count(*) FROM process_data_time WHERE process_id='TEST_LINE' AND ts_start=0"
        ).fetchone()[0] == 0
        assert db.execute(
            "SELECT sync_state FROM process_data_time WHERE process_id='TEST_LINE' AND ts_start=1000"
        ).fetchone()[0] == "PENDING"


def test_local_retention_defaults(config_factory):
    retention = config_factory().local_retention
    assert retention.synced_days == 7
    assert retention.event_log_days == 30
