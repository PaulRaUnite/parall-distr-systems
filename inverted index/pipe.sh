#!/usr/bin/env bash
grep -r "" data/* | python3 map.py | sort | python3 reduce.py > output.txt