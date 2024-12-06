class ResourceExistsError(ValueError):
    pass


def gib_to_bytes(gb: int) -> int:
    return 1073741824 * gb


def gb_to_bytes(gb: int) -> int:
    return 1000000000 * gb


def bytes_to_gib(in_bytes: int):
    return in_bytes / 1073741824