import sys

def do_reduce(word, values): 
    return word, sum(values) 

kv = dict()
for key, value in map(lambda x: x.split("\t"), (line for line in sys.stdin)):
	if key not in kv:
		kv[key] = int(value)
	else:
		kv[key] += int(value)

for key, value in kv.items():
	print(f"{key}\t{value}")
