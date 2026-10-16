from scripts.summarize_docker_stats import memory_bytes


def test_memory_bytes_accepts_docker_units():
    assert memory_bytes("1GiB / 2GiB") == 1024**3
    assert memory_bytes("512MiB / 2GiB") == 512 * 1024**2
