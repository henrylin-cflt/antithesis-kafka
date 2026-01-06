#!/bin/sh

echo "starting cluster"
java -cp load.jar:antithesis-ffi-1.4.4.jar:antithesis-instrumentor-1.4.4.jar:antithesis-sdk-catalog.jar com.antithesis.kafka.LoadApplication workload-config.yaml &
#java -cp load.jar com.antithesis.kafka.LoadApplication workload-config.yaml &

echo "workload connected"
# Wait for sometime to start the fault injector and antithesis campaign
sleep 5

echo "starting workload loop"

for i in $(seq 1 100); do
    echo "starting workload ${i}"
    num_threads=$(( $(head -c 4 /dev/urandom | od -An -tu4) % 13 + 2 ))
    echo "num_threads ${num_threads}"
    workload_id="$(date +%Y%m%d-%H%M%S)-$$"
    echo "workload_id ${workload_id}"
    java -Dlogfile.name=./logs/kafka-workload-${workload_id}.log -Djava.util.concurrent.ForkJoinPool.common.parallelism=num_threads -cp workload.jar:antithesis-ffi-1.4.4.jar:antithesis-instrumentor-1.4.4.jar:antithesis-sdk-catalog.jar com.antithesis.kafka.WorkloadApplication workload-config.yaml ${workload_id} &
    #java -Dlogfile.name=./logs/kafka-workload-${workload_id}.log -Djava.util.concurrent.ForkJoinPool.common.parallelism=num_threads -cp workload.jar com.antithesis.kafka.WorkloadApplication workload-config.yaml ${workload_id} &
    # limit parallel jobs
    sleep 15
done&

while true
do
    echo "validation check"
    ./validation ./logs
    # java -cp validation.jar:antithesis-ffi-1.4.4.jar:antithesis-instrumentor-1.4.4.jar:antithesis-sdk-catalog.jar com.antithesis.kafka.validation.ValidationApplication ./logs
    # java -cp . $JAVA_OPTS -jar validation.jar ./logs
    sleep 30
done