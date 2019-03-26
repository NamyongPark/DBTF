#!/usr/bin/env bash

cd $(dirname $(readlink -f $0))

OUTPUT_DIR_PATH=/home/${USER}/tmp/DBTF/cp
rm -rf ${OUTPUT_DIR_PATH}
mkdir -p ${OUTPUT_DIR_PATH}
if [[ ! -d "${OUTPUT_DIR_PATH}" ]]; then
    echo "unable to make output directory: ${OUTPUT_DIR_PATH}"
    exit 1
fi

hadoop fs -rm -r -f sample.tensor
hadoop fs -put sample.tensor sample.tensor

echo "Executing DBTF for Boolean CP factorization on sample.tensor..."

./dbtf.sh \
--factorization-method cp \
--base-index 0 \
--rank 10 \
--mode1-length 30 \
--mode2-length 40 \
--mode3-length 50 \
--initial-factor-matrix-density 0.01 \
--converge-by-num-iters 10 \
--num-unfolded-tensor-partitions 8 \
--output-dir-path ${OUTPUT_DIR_PATH} \
--output-file-prefix sample \
--tensor-file-separator-type csv \
--compute-error every \
sample.tensor

echo -e "\nOutput saved to ${OUTPUT_DIR_PATH}\n"
