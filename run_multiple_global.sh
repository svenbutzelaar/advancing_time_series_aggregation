#!/bin/bash

# first 
# run  500 500 4000
# later if necaserry
# experiments that have large error on hig n: 500 4000 8760
# finer detail needed: 100 100 3000

# ============================================================
# Experiment sweep script
# ============================================================

# Datasets (3)
#   BaseDataset
#   LowVar
#   HighVar

# ExtremePreservation (4)
#   NoExtremePreservation
#   Afterwards
#   SeperateExtremesSum
#   DynamicProgramming

# ClusteringMethod (2)
#   PerLocation
#   PerProfile

# Total combinations:
#   3 datasets
# * 4 extreme preservation methods
# * 2 clustering methods
# = 24 experiment groups
#
#
# ============================================================

DATASETS=(
    "BaseDataset"
    "LowVar"
    "HighVar"
)

EXTREME_PRESERVATIONS=(
    "NoExtremePreservation"
    "Afterwards"
    "SeperateExtremesSum"
    "DynamicProgramming"
)

CLUSTERING_METHODS=(
    "Global"
)

# ------------------------------------------------------------
# Main sweep: 500 -> 4000
# ------------------------------------------------------------
for dataset in "${DATASETS[@]}"
do
    for n in $(seq 500 500 4000)
    do
        for ep in "${EXTREME_PRESERVATIONS[@]}"
        do
            for cm in "${CLUSTERING_METHODS[@]}"
            do
                echo "Submitting: dataset=$dataset ep=$ep cm=$cm n=$n"

                sbatch run.sh \
                    --n_prime=$n \
                    --dataset=$dataset \
                    --extreme_preservation=$ep \
                    --clustering_method=$cm
            done
        done
    done



done

