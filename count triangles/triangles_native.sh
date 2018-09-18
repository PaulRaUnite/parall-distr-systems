#!/usr/bin/env bash
cat ./data/graph.txt | python3 mapper.py | sort | python3 reducer.py