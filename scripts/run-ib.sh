#!/bin/bash

ITERS=10
RUNS=-1
FLOWS=10
#BUFF_SZ=8388608
#BUFF_SZ=4194304
#BUFF_SZ=41943040
BUFF_SZ=456131
LOGFOLDER=/mnt/tcp-logs

HOSTFILE=/home/azhpcuser/ib-6-hops/hostfile
NUM_HOSTS=2

GROUP1FILE=/home/azhpcuser/ib-6-hops/group1
NUM_GROUP1=1


NUM_PROCS=$((NUM_HOSTS * FLOWS))

#Runs between LL02 and LL03
mpirun -np ${NUM_PROCS} -hostfile ${HOSTFILE} --map-by ppr:${FLOWS}:node \
        --use-hwthread-cpus --bind-to cpulist:ordered  --cpu-list 5,7,9,11,13,15,17,19,21,23 --report-bindings\
        -mca plm_rsh_no_tree_spawn 1 -mca plm_rsh_num_concurrent 800 \
        -x UCX_IB_SL=1 \
        -x LD_LIBRARY_PATH -x UCX_NET_DEVICES=mlx5_ib2:1 -x UCX_TLS=rc \
        /home/azhpcuser/mpi-perf/mpi_perf -f ${GROUP1FILE} -n ${NUM_GROUP1} -p ${FLOWS} -u 1 -r ${RUNS} -i ${ITERS} -b ${BUFF_SZ} -l ${LOGFOLDER}

#mpirun -np ${NUM_PROCS} -hostfile ${HOSTFILE} --map-by ppr:${FLOWS}:node --bind-to core \
#       -mca plm_rsh_no_tree_spawn 1 -mca plm_rsh_num_concurrent 800 \
#       -x LD_LIBRARY_PATH -x UCX_NET_DEVICES=eth0 -x UCX_TLS=tcp \
#       numactl --cpunodebind=0 --membind 0 \
#       /home/azhpcuser/mpi-perf/mpi_perf -f ${GROUP1FILE} -n ${NUM_GROUP1} -p ${FLOWS} -u 1 -r ${RUNS} -i ${ITERS} -b ${BUFF_SZ} -l ${LOGFOLDER}


