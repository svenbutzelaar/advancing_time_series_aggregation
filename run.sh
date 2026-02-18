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

# === Run the scenario ===

INPUT="$1"
LOG1="logs/${INPUT}_julia.log"

timestamp() {
    date "+%Y-%m-%d %H:%M:%S"
}

log() {
    echo "[$(timestamp)] $1"
}

# ---- Run normal case ----
log "START Julia run for $INPUT"
log "Writing log to: $LOG1  (use 'tail -f $LOG1' to watch live)"

#  --- create output directories ---
base_dir="/scratch/sbutzelaar/advancing_time_series_aggregation"
case_name="$1"
input_dir="$base_dir/$case_name"
output_dir="$input_dir/output"
[ -d "$output_dir" ] || mkdir -p "$output_dir"

start_ts=$(date +%s)

srun julia --project ./run_experiment.jl "$INPUT" > "$LOG1" 2>&1

end_ts=$(date +%s)
log "DONE Julia run for $INPUT (took $((end_ts - start_ts)) seconds)"
echo >> "$LOG1"
echo "Completed at $(timestamp), runtime: $((end_ts - start_ts)) sec" >> "$LOG1"

log "ALL DONE for $INPUT"
