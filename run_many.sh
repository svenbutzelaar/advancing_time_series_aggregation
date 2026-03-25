#!/bin/bash

# -------------------------------
# NoExtremePreservation: step 1000
# -------------------------------
for n in $(seq 1000 1000 8760)
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
        --extreme_preservation=SeperateExtremesSum \
        --dependant_per_location=false
done

sbatch run.sh --n_prime=8760 --extreme_preservation=NoExtremePreservation