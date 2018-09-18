import sys


def flip(x: int, y: int) -> (int, int):
    if x < y:
        return y, x
    return x, y


for line in sys.stdin:
    v1, v2 = line.split()
    if v1 != v2:
        v1, v2 = flip(v1, v2)
        print(f"{v1}\t{v2}")
