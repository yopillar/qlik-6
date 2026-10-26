from orchestrate import order_scripts


def test_reader_runs_after_writer_whatever_the_extension():
    io = {
        "b.qvs": {"reads": ["lib://out/ventes.qvd"], "writes": []},
        "a.qvs": {"reads": [], "writes": ["lib://out/VENTES.QVD"]},
    }
    order, cycles = order_scripts(io)
    assert order == [("a.qvs", []), ("b.qvs", ["a.qvs"])]
    assert not cycles


def test_cycle_is_broken_and_reported():
    io = {"a": {"reads": ["y"], "writes": ["x"]},
          "b": {"reads": ["x"], "writes": ["y"]}}
    order, cycles = order_scripts(io)
    assert [s for s, _ in order] == ["a", "b"]
    assert cycles == ["a"]
