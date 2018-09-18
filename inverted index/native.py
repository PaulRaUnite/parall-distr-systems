import os
import re
from typing import Dict, Tuple, Set

if __name__ == '__main__':
    index: Dict[str, Tuple[int, Set[str]]] = dict()
    header_re = re.compile(r"[\w-]+:.+")
    word_re = re.compile(r"([a-zA-Z']{2,})")
    for dirpath, dnames, fnames in os.walk("./data/20_newsgroup/"):
        for f in fnames:
            with open(os.path.join(dirpath, f), errors="ignore") as file:
                try:
                    for line in file:
                        if line == "\n" or header_re.fullmatch(line) is not None:
                            continue
                        for word in (word.lower() for word in word_re.findall(line)):
                            if word.endswith("''"):
                                word = word[:-2]
                            if word.startswith("'") and word.endswith("'"):
                                word = word[1:-1]
                            if not word:
                                continue
                            try:
                                count, files = index[word]
                                count += 1
                                files.add(f)
                                index[word] = (count, files)
                            except KeyError:
                                index[word] = (1, {f})
                except UnicodeDecodeError as e:
                    print(f, dirpath, dnames)
                    raise e

    for k, v in index.items():
        print(f"{k},{v[0]},{' '.join(v[1])}")