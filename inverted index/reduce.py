import sys
from typing import Dict, Tuple, Set

words: Dict[str, Tuple[int, Set[str]]] = dict()
for line in sys.stdin:
	word, file = line.split("\t", maxsplit=1)
	if word in words:
		count, files = words[word]
		count += 1
		files.add(file)
		words[word] = (count, files)
	else:
		words[word] = (1, {file})

for word, count, files in map(lambda wcf: (wcf[0], wcf[1][0], wcf[1][1]), words.items()):
	print(f"{word},{count},{' '.join(files)}")