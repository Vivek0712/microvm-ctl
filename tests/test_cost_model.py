from microvm.monitor import CostModel


def test_vcpu_ratio_is_half_memory():
    assert CostModel(memory_gb=2).vcpu == 1
    assert CostModel(memory_gb=8).vcpu == 4


def test_suspended_session_beats_always_on():
    m = CostModel(memory_gb=2, snapshot_gb=0.61)
    s = m.session(active_s=30 * 60, suspended_s=8 * 3600, cycles=1)
    assert s["total_usd"] < s["vs_always_on_usd"]
    assert s["savings_pct"] > 80


def test_terminate_beats_suspend_for_one_shot_jobs():
    m = CostModel(memory_gb=2)
    one_shot = m.session(active_s=8, suspended_s=0, cycles=0)["total_usd"]
    parked = m.session(active_s=8, suspended_s=3600, cycles=1)["total_usd"]
    assert one_shot < parked
