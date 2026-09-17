import mmh3

def get_partition(id: int, partition_count: int):
    return mmh3.hash(str(id), signed=False) % partition_count