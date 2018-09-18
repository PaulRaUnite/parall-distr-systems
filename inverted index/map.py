import sys
import re

additional_info_re = re.compile(r"\w+:")
for line in sys.stdin:
	try:
		file, content = line.split(":", maxsplit=1)
	except ValueError:
		continue
	if content.startwith("Path:")
	for word in content.split():
		print(f"{word}\t{file}")
