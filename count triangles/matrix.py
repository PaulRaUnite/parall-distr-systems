import numpy as np

edges = {(int(v1), int(v2)) for v1, v2 in (line.split() for line in open("./data/graph.txt")) if v1 != v2}
print(len(edges))
vertices = set()
for v1, v2 in edges:
    vertices.add(v1)
    vertices.add(v2)

count = max(vertices)
matrix = np.zeros((count, count), dtype=int)
for v1, v2 in edges:
    matrix[v1 - 1][v2 - 1] = 1
    matrix[v2 - 1][v1 - 1] = 1

result = np.linalg.matrix_power(matrix, 3)
print(matrix)
print(result)
print(sum(result.diagonal(), 0) / 6)
