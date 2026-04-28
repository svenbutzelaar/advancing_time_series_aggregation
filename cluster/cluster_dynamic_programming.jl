using Statistics

include("profile_type.jl")
include("config.jl")

# =============================================================================
# Segment cost  (band-limited)
# =============================================================================
#
# For a contiguous block [i, k] (1-indexed, inclusive) and column j:
#
#   v̂_{[i,k],j} = max_{t∈[i,k]} x_{t,j}   if max ≥ τ_high[j]  and j is Demand
#               = min_{t∈[i,k]} x_{t,j}   if min ≤ τ_low[j]   and j is Renewable
#               = μ_{[i,k],j}              otherwise
#
#   cost(i, k) = Σ_j Σ_{t=i}^{k} (x_{t,j} - v̂_{[i,k],j})²
#
# Storage layout: seg_cost[i, ℓ] = cost of the block starting at i with
# length ℓ, for ℓ ∈ 1..min(L, n_t - i + 1).
#
# Complexity: O(n·L·d) time,  O(n·L) space  (vs O(n²·d) / O(n²) full).
# =============================================================================

"""
    compute_segment_costs_banded(
        values          :: Matrix{Float64},   # (n_t × d)
        modes           :: Vector{ProfileType},
        high_thresholds :: Vector{Float64},
        low_thresholds  :: Vector{Float64},
        max_block_size  :: Int,
    ) -> Matrix{Float64}                      # (n_t × max_block_size)

`result[i, ℓ]` = SSE cost of the block starting at timestep `i` with length `ℓ`.
"""
function compute_segment_costs_banded(
    values          :: Matrix{Float64},
    modes           :: Vector{ProfileType},
    high_thresholds :: Vector{Float64},
    low_thresholds  :: Vector{Float64},
    max_block_size  :: Int,
)
    n_t, d = size(values)
    L      = min(max_block_size, n_t)

    seg_cost = zeros(Float64, n_t, L)

    # Prefix sums for O(1) range sum / sum-of-squares queries
    prefix_sum   = zeros(Float64, n_t + 1, d)
    prefix_sumsq = zeros(Float64, n_t + 1, d)
    for j in 1:d, t in 1:n_t
        prefix_sum[t+1, j]   = prefix_sum[t, j]   + values[t, j]
        prefix_sumsq[t+1, j] = prefix_sumsq[t, j] + values[t, j]^2
    end

    seg_sum(i, k, j)   = prefix_sum[k+1, j]   - prefix_sum[i, j]
    seg_sumsq(i, k, j) = prefix_sumsq[k+1, j] - prefix_sumsq[i, j]

    # Reusable per-column running max/min buffers (avoid allocation in hot loop)
    seg_max = Vector{Float64}(undef, d)
    seg_min = Vector{Float64}(undef, d)

    for i in 1:n_t
        @inbounds for j in 1:d
            seg_max[j] = values[i, j]
            seg_min[j] = values[i, j]
        end

        max_k = min(i + L - 1, n_t)

        for k in i:max_k
            ℓ = k - i + 1

            if k > i
                @inbounds for j in 1:d
                    v = values[k, j]
                    seg_max[j] = seg_max[j] < v ? v : seg_max[j]
                    seg_min[j] = seg_min[j] > v ? v : seg_min[j]
                end
            end

            cost = 0.0
            @inbounds for j in 1:d
                mode = modes[j]

                v_hat = if mode == Demand && seg_max[j] >= high_thresholds[j]
                    seg_max[j]
                elseif (mode == Solar || mode == WindOnshore || mode == WindOffshore) &&
                       seg_min[j] <= low_thresholds[j]
                    seg_min[j]
                else
                    seg_sum(i, k, j) / ℓ
                end

                s    = seg_sum(i, k, j)
                ssq  = seg_sumsq(i, k, j)
                cost += ssq - 2.0 * v_hat * s + ℓ * v_hat^2
            end

            seg_cost[i, ℓ] = cost
        end
    end

    return seg_cost
end


# =============================================================================
# Dynamic programming  (band-limited)
# =============================================================================

"""
    optimal_partition_dp_banded(
        seg_cost       :: Matrix{Float64},   # (n_t × L) from compute_segment_costs_banded
        K              :: Int,
        max_block_size :: Int,
    ) -> Vector{Int}

Solves the optimal K-partition using:

    dp[t] = min_{s : max(k, t-L+1) ≤ s ≤ t}  dp_prev[s-1] + seg_cost[s, t-s+1]

Only two DP rows are kept in memory at any time.

Complexity: O(K·n·L) time, O(K·n) space (split table).
"""
function optimal_partition_dp_banded(
    seg_cost       :: Matrix{Float64},
    K              :: Int,
    max_block_size :: Int,
)
    n_t = size(seg_cost, 1)
    L   = min(max_block_size, n_t)

    @assert K >= 1
    @assert K <= n_t   "n_prime ($K) cannot exceed number of timesteps ($n_t)"
    @assert n_t <= K * L  "Infeasible: $n_t timesteps cannot be covered by $K blocks " *
                          "of max length $L"

    INF = typemax(Float64)

    dp_prev = fill(INF, n_t)
    dp_curr = fill(INF, n_t)

    # split_table[k, t] = start index of block k in the optimal k-block
    # solution ending at t.  Stored for all k so we can backtrack.
    split_table = zeros(Int, K, n_t)

    # Base case: k = 1
    for t in 1:min(L, n_t)
        dp_prev[t]        = seg_cost[1, t]
        split_table[1, t] = 1
    end

    for k in 2:K
        fill!(dp_curr, INF)

        for t in k:n_t
            s_lo = max(k, t - L + 1)

            best_cost  = INF
            best_split = s_lo

            @inbounds for s in s_lo:t
                prev = dp_prev[s - 1]
                prev == INF && continue
                c = prev + seg_cost[s, t - s + 1]
                if c < best_cost
                    best_cost  = c
                    best_split = s
                end
            end

            dp_curr[t]        = best_cost
            split_table[k, t] = best_split
        end

        dp_prev, dp_curr = dp_curr, dp_prev
    end

    # ── Backtrack ─────────────────────────────────────────────────────────────
    cuts = zeros(Int, K)
    t    = n_t
    for k in K:-1:1
        cuts[k] = split_table[k, t]
        t        = cuts[k] - 1
    end

    partitions = Vector{Int}(undef, K)
    for k in 1:K
        stop          = k < K ? cuts[k+1] - 1 : n_t
        partitions[k] = stop - cuts[k] + 1
    end

    return partitions
end


# =============================================================================
# Representative and mean values from a partition
# =============================================================================

"""
    collect_rep_and_mean_values(
        values          :: Matrix{Float64},
        partitions      :: Vector{Int},
        modes           :: Vector{ProfileType},
        high_thresholds :: Vector{Float64},
        low_thresholds  :: Vector{Float64},
    ) -> (result_values, mean_values)
"""
function collect_rep_and_mean_values(
    values          :: Matrix{Float64},
    partitions      :: Vector{Int},
    modes           :: Vector{ProfileType},
    high_thresholds :: Vector{Float64},
    low_thresholds  :: Vector{Float64},
)
    _, d = size(values)
    result_values = Vector{Vector{Float64}}()
    mean_values   = Vector{Vector{Float64}}()

    cursor = 1
    for p in partitions
        block = view(values, cursor:cursor+p-1, :)

        rep_vec  = Vector{Float64}(undef, d)
        mean_vec = Vector{Float64}(undef, d)

        for j in 1:d
            col  = block[:, j]
            μ    = mean(col)
            bmax = maximum(col)
            bmin = minimum(col)

            mean_vec[j] = μ
            rep_vec[j]  = if modes[j] == Demand && bmax >= high_thresholds[j]
                bmax
            elseif (modes[j] == Solar || modes[j] == WindOnshore || modes[j] == WindOffshore) &&
                   bmin <= low_thresholds[j]
                bmin
            else
                μ
            end
        end

        push!(result_values, rep_vec)
        push!(mean_values,   mean_vec)
        cursor += p
    end

    return result_values, mean_values
end


# =============================================================================
# Public entry point
# =============================================================================

"""
    optimal_time_partitioning_dp(
        values         :: Matrix{Float64},
        modes          :: Vector{ProfileType},
        config         :: ClusteringConfig;
        max_block_size :: Int = 168,
    ) -> (partitions, result_values, mean_values, ward_errors, ldc_errors)

Drop-in replacement for `hierarchical_time_clustering_ward`.  Finds the
globally optimal K-partition (K = config.n_prime) under the extreme-aware
SSE objective using dynamic programming with a band constraint.

`max_block_size` (L) caps how long any single block may be, enabling the
banded O(n·L·d) cost precomputation and O(K·n·L) DP instead of the full
O(n²) versions.  Sensible defaults for energy systems:

  | L    | physical meaning | cost table | DP ops (K=100) |
  |------|-----------------|------------|----------------|
  | 8760 | no cap          | ~600 MB    | ~7.7 B         |
  | 168  | 1 week (default)| ~9 MB      | ~147 M         |
  |  48  | 2 days          | ~3 MB      | ~42 M          |
  |  24  | 1 day           | ~1.5 MB    | ~21 M          |
"""
function optimal_time_partitioning_dp(
    values :: Matrix{Float64},
    modes  :: Vector{ProfileType},
    config :: ClusteringConfig = ClusteringConfig();
)
    n_t, d = size(values)
    @assert length(modes) == d "Length of modes must match number of columns"
    max_block_size = config.max_block_size

    K = config.n_prime
    @assert K <= n_t  "n_prime ($K) exceeds number of timesteps ($n_t)"
    @assert n_t <= K * max_block_size "Infeasible: cannot cover $n_t timesteps with $K blocks of max length $max_block_size.  Increase max_block_size or reduce n_prime."

    # ── Thresholds ───────────────────────────────────────────────────────────
    high_thresholds = zeros(d)
    low_thresholds  = zeros(d)
    for j in 1:d
        sorted_col = sort(values[:, j])
        high_idx   = ceil(Int, config.high_percentile * n_t)
        low_idx    = ceil(Int, config.low_percentile  * n_t)
        high_thresholds[j] = sorted_col[high_idx]
        low_thresholds[j]  = sorted_col[low_idx]
    end

    # ── Banded segment costs ─────────────────────────────────────────────────
    seg_cost = compute_segment_costs_banded(
        values, modes, high_thresholds, low_thresholds, max_block_size
    )

    # ── Optimal partition ────────────────────────────────────────────────────
    partitions = optimal_partition_dp_banded(seg_cost, K, max_block_size)

    # ── Representative and mean values ───────────────────────────────────────
    result_values, mean_values = collect_rep_and_mean_values(
        values, partitions, modes, high_thresholds, low_thresholds
    )

    ward_errors = Vector{Vector{Float64}}()
    ldc_errors  = Vector{Vector{Float64}}()

    return partitions, result_values, mean_values, ward_errors, ldc_errors
end