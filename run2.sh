echo "Generating the configuration database. This may take a while..."
python exp/run_join_exp.py -s exp/join_exp_config.csv
echo "[Success] The configuration database is generated."

echo "Run microbenchmarks from Section 5.2.1 to 5.2.7"
python exp/run_join_exp.py \
        -p secondR \
        -b ./bin/volcano/join_exp_4b4b \
        -c exp/join_exp_config.csv \
        -y exp/join_runs.yaml \
        -e 0 1 2 3 4 \
        -r $1 \
        -p exp_results/gpu_join \
        -d $2