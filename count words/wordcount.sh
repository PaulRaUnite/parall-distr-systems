rm -rf output
yarn jar ~/Libs/hadoop-3.1.1/share/hadoop/tools/lib/hadoop-streaming-3.1.1.jar -input ./data -output ./output -file mapper.py -file reducer.py -mapper "python3 mapper.py" -reducer "python3 reducer.py"
