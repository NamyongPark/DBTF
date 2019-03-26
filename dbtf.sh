#!/usr/bin/env bash
BASEPATH=$(dirname $0)
cd ${BASEPATH}

which spark-submit > /dev/null
status=$?
if test ${status} -ne 0 ; then
    echo ""
    echo "Spark is not installed in the system."
    echo "Please install Spark and make sure the Spark binary is accessible."
    exit 127
fi

# SPARK OPTIONS
MASTER="local[*]"
DRIVER_MEMORY=2048m
EXECUTOR_MEMORY=4096m
NUM_EXECUTORS=1
EXECUTOR_CORES=4

DBTF_ASSEMBLY_JAR=./target/scala-2.11/DBTF-assembly-2.0.jar

spark-submit --class dbtf.DBTFDriver --master ${MASTER} \
--driver-memory ${DRIVER_MEMORY} --executor-memory ${EXECUTOR_MEMORY} --num-executors ${NUM_EXECUTORS} --executor-cores ${EXECUTOR_CORES} ${DBTF_ASSEMBLY_JAR} \
"$@"
