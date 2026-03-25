#!/bin/sh

#SBATCH --job-name=fully_flexible_temporal_resolution_experiments
#SBATCH --partition=compute
#SBATCH --account=education-eemcs-msc-cs
#SBATCH --time=1:30:00
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

timestamp=$(date "+%Y%m%d_%H%M%S")
time_start=$(date +%s)
LOG_DIR="$base_dir/logs"
[ -d "$LOG_DIR" ] || mkdir -p "$LOG_DIR"

# Build a slug from the config args for use in the log filename
config_slug=$(echo "$@" | sed 's/--//g; s/ /_/g')

log_file="$LOG_DIR/run_experiment_${config_slug}_${timestamp}.log"

# ================================
# Run
# ================================

echo "[$(date '+%Y-%m-%d %H:%M:%S')] START run_experiment.jl args=$@"
echo "Logging to $log_file"

srun julia --project cli.jl "$@" run_experiment.jl > "$log_file" 2>&1
exit_code=$?

if [ $exit_code -ne 0 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] FAILED run_experiment.jl (exit code $exit_code)" >&2
    exit $exit_code
fi

time_end=$(date +%s)
elapsed=$(( time_end - time_start ))
elapsed_fmt=$(printf '%02dh:%02dm:%02ds' $(( elapsed/3600 )) $(( (elapsed%3600)/60 )) $(( elapsed%60 )))


echo "[$(date '+%Y-%m-%d %H:%M:%S')] DONE run_experiment.jl — total runtime: $elapsed_fmt"
