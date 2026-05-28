#!/bin/bash


# Dataset 3
#     BaseDataset
#     LowVar
#     HighVar

# ExtremePreservation 4
#     NoExtremePreservation
#     Afterwards
#     SeperateExtremesSum
#     DynamicProgramming


# ClusteringMethod 2
#     UTR ----
#     PerLocation
#     PerProfile

# NumberClusters 87
#     seq 100 100 8760

# 87 * 3 * (1 + 2 * 4) = 
# 87 * 27 =
# 2349


# -------------------------------
# NoExtremePreservation: step 1000
# -------------------------------
for n in $(seq 2000 1000 8760)
do
    sbatch run.sh --n_prime=$n --extreme_preservation=NoExtremePreservation
done
for n in $(seq 100 100 2000)
do
    sbatch run.sh --n_prime=$n --extreme_preservation=NoExtremePreservation
done

# -------------------------------
# Afterwards: step 500
# -------------------------------
for n in $(seq 500 500 8760)
do
    sbatch run.sh --n_prime=$n --extreme_preservation=Afterwards
done

# -------------------------------
# SeperateExtremesSum: step 100
# -------------------------------
for n in $(seq 100 100 8760)
do
    sbatch run.sh --n_prime=$n --extreme_preservation=SeperateExtremesSum
done

# -------------------------------
# SeperateExtremesSum: step 500 + extra flag
# -------------------------------
for n in $(seq 500 500 8760)
do
    sbatch run.sh \
        --n_prime=$n \
        --extreme_preservation=NoExtremePreservation \
        --clustering_method=PerProfile
done

sbatch run.sh --n_prime=8760 --extreme_preservation=NoExtremePreservation

for n in \
    4380 2920 2190 1752 \
    1460 1095 876 730
do
    sbatch run.sh \
        --n_prime=$n \
        --extreme_preservation=NoExtremePreservation \
        --clustering_method=UTR
done

for n in $(seq 500 500 6000)
do
    sbatch run.sh \
        --n_prime=$n \
        --extreme_preservation=NoExtremePreservation \
        --clustering_method=Global
done

for n in $(seq 100 100 4501)
do
    sbatch run.sh \
        --n_prime=$n \
        --extreme_preservation=DynamicProgramming
done


for n in $(seq 600 100 1100)
do
    sbatch run.sh \
        --n_prime=$n \
        --extreme_preservation=DynamicProgramming \
        --max_block_size=672
done

for n in $(seq 600 100 1100)
do
    sbatch run.sh \
        --n_prime=$n \
        --extreme_preservation=DynamicProgramming \
        --max_block_size=2688
done