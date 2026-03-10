#!/bin/sh

#SBATCH --job-name=fully_flexible_temporal_resolution_experiments
#SBATCH --partition=compute
#SBATCH --account=education-eemcs-msc-cs
#SBATCH --time=3:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=30
#SBATCH --mem-per-cpu=3968MB

# === Load modules ===
module load 2025
module load openmpi
module load gurobi/12.0.0
module load julia

# ================================
# Setup directories
# ================================

base_dir="/scratch/sbutzelaar/advancing_time_series_aggregation"

timestamp_file=$(date "+%Y%m%d_%H%M%S")
LOG_DIR="$base_dir/logs"
[ -d "$LOG_DIR" ] || mkdir -p "$LOG_DIR"

EXTRA_ARGS="$@"

# ================================
# Helpers
# ================================

run_experiment() {
    local calc_ens=$1
    local script_name=$2
    local log_file="$LOG_DIR/${script_name}_calcens_${calc_ens}_${timestamp_file}.log"

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] START $script_name calc_ens=$calc_ens"
    echo "Logging to $log_file"

    srun julia --project cli.jl $([ "$calc_ens" = "true" ] && echo "--calc_ens") $EXTRA_ARGS "$script_name" > "$log_file" 2>&1

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] DONE $script_name calc_ens=$calc_ens"
}

# ================================
# Run experiments
# ================================

run_experiment false run_experiment.jl

srun julia --project cli.jl $EXTRA_ARGS create_ens_experiment_db.jl > "$LOG_DIR/create_ens_experiment_db_${timestamp_file}.log" 2>&1

run_experiment true run_experiment.jl