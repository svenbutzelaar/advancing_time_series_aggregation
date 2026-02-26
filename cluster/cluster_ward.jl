using Statistics
using DataStructures

include("profile_type.jl")
include("config.jl")

# =========================
# Linked list node
# =========================

mutable struct LinkedListNode
    start_index::Int
    end_index::Int

    sum_of_values::Vector{Float64}
    max_of_values::Vector{Float64}
    min_of_values::Vector{Float64}

    count::Int

    representative::Vector{Float64}

    prev_node::Union{Nothing, LinkedListNode}
    next_node::Union{Nothing, LinkedListNode}
    active::Bool

    function LinkedListNode(
        start_index::Int,
        end_index::Int,
        value::AbstractVector{<:Float64},
    )
        v = collect(value)

        new(
            start_index,
            end_index,
            copy(v),
            copy(v),
            copy(v),
            1,
            copy(v),
            nothing,
            nothing,
            true
        )
    end
end

# =========================
# Merge logic
# =========================

function merge_nodes!(
    c1::LinkedListNode,
    c2::LinkedListNode,
    modes,
    high_thresholds,
    low_thresholds,
    config,
)

    c1.end_index = c2.end_index
    c1.sum_of_values .+= c2.sum_of_values
    c1.max_of_values .= max.(c1.max_of_values, c2.max_of_values)
    c1.min_of_values .= min.(c1.min_of_values, c2.min_of_values)
    c1.count += c2.count

    # update representative AFTER merge
    update_representative!(c1, modes, high_thresholds, low_thresholds, config)

    c1.next_node = c2.next_node

    if c2.next_node !== nothing
        c2.next_node.prev_node = c1
    end

    c2.active = false
    c2.prev_node = nothing
    c2.next_node = nothing
end

# =========================
# Ward dissimilarity
# =========================

function compute_ward_dissimilarity(
    c1::LinkedListNode,
    c2::LinkedListNode,
)
    diff = c1.representative .- c2.representative
    sqdist = sum(diff .* diff)

    return (c1.count * c2.count) / (c1.count + c2.count) * sqdist
end

# =========================
# Representative value
# =========================

function update_representative!(
    c::LinkedListNode,
    modes::Vector{ProfileType},
    high_thresholds::Vector{Float64},
    low_thresholds::Vector{Float64},
    config::ClusteringConfig,
)
    c.representative .= c.sum_of_values ./ c.count

    if config.extreme_preservation == DuringClustering
        for j in eachindex(c.sum_of_values)    
            mode = modes[j]
            c.representative[j] = getRepresentativeValue(c, mode, j, high_thresholds, low_thresholds)
        end
    end
end

function getRepresentativeValue(
    c::LinkedListNode,
    mode::ProfileType,
    j::Int,
    high_thresholds::Vector{Float64},
    low_thresholds::Vector{Float64},
)
    if mode == Demand
        return c.max_of_values[j] >= high_thresholds[j] ?
               c.max_of_values[j] :
               c.representative[j]

    elseif mode == Solar || mode == WindOnshore || mode == WindOffshore
        return c.min_of_values[j] <= low_thresholds[j] ?
               c.min_of_values[j] :
               c.representative[j]
    end

    return c.representative[j]
end

# =========================
# Heap wrapper
# =========================

struct HeapEntry
    ward_criterion::Float64
    c1::LinkedListNode
    c2::LinkedListNode
end

Base.isless(a::HeapEntry, b::HeapEntry) =
    a.ward_criterion < b.ward_criterion

# =========================
# Main clustering function
# =========================

function hierarchical_time_clustering_ward(
    values::Matrix{Float64},
    n_prime::Int,
    modes::Vector{ProfileType},
    config::ClusteringConfig = ClusteringConfig(),
)

    n, d = size(values)
    @assert length(modes) == d "Length of modes must match number of columns"

    # -------------------------
    # Threshold computation
    # -------------------------

    high_thresholds = zeros(d)
    low_thresholds  = zeros(d)

    for j in 1:d
        sorted_col = sort(values[:, j])

        high_idx = ceil(Int, config.high_percentile * n)
        low_idx  = ceil(Int, config.low_percentile * n)

        high_thresholds[j] = sorted_col[high_idx]
        low_thresholds[j]  = sorted_col[low_idx]
    end

    # -------------------------
    # Optional full-resolution stats
    # -------------------------

    full_resolution_values_sorted = nothing

    if config.calc_stats
        full_resolution_values_sorted = [
            sort(values[:, j]; rev=true) for j in 1:d
        ]
    end

    ward_errors = Vector{Vector{Float64}}()
    ldc_errors_per_merge = Vector{Vector{Float64}}()
    result_values = Vector{Vector{Float64}}()
    mean_values = Vector{Vector{Float64}}()

    # -------------------------
    # Initialize clusters
    # -------------------------

    clusters = [LinkedListNode(i, i, view(values, i, :)) for i in 1:n]

    for i in 1:n-1
        clusters[i].next_node = clusters[i+1]
        clusters[i+1].prev_node = clusters[i]
    end

    heap = MutableBinaryMinHeap{HeapEntry}()

    function push_merge(c1::LinkedListNode, c2::LinkedListNode)
        if c1.active && c2.active
            ward_crit = compute_ward_dissimilarity(c1, c2)
            push!(heap, HeapEntry(ward_crit, c1, c2))
        end
    end

    for i in 1:n-1
        push_merge(clusters[i], clusters[i+1])
    end

    merges = 0
    total_merges = n - n_prime

    # =========================
    # Merge loop
    # =========================

    while merges < total_merges

        isempty(heap) && break

        entry = pop!(heap)

        if !(entry.c1.active &&
             entry.c2.active &&
             entry.c1.next_node === entry.c2)
            continue
        end

        # -------------------------
        # Optional statistics
        # -------------------------

        if config.calc_stats

            active_clusters = filter(c -> c.active, clusters)

            ldc_error_vec = Float64[]
            ward_error_vec = Float64[]

            for j in 1:d

                merged_values = Float64[]

                for c in active_clusters
                    append!(merged_values, fill(c.representative[j], c.count))
                end

                merged_sorted = sort(merged_values; rev=true)

                # LDC RMSE
                mse = mean(
                    (full_resolution_values_sorted[j] .- merged_sorted).^2
                )
                push!(ldc_error_vec, sqrt(mse))

                # Ward SSE
                push!(ward_error_vec,
                    sum((values[:, j] .- merged_values).^2)
                )
            end

            push!(ldc_errors_per_merge, ldc_error_vec)
            push!(ward_errors, ward_error_vec)
        end

        # -------------------------
        # Merge
        # -------------------------

        merge_nodes!(
            entry.c1, 
            entry.c2,
            modes,
            high_thresholds,
            low_thresholds,
            config,
        )
        merges += 1

        if entry.c1.prev_node !== nothing &&
           entry.c1.prev_node.active
            push_merge(entry.c1.prev_node, entry.c1)
        end

        if entry.c1.next_node !== nothing &&
           entry.c1.next_node.active
            push_merge(entry.c1, entry.c1.next_node)
        end
    end

    # =========================
    # Collect results
    # =========================

    result_partitions = Int[]
    active_clusters = filter(c -> c.active, clusters)

    for c in active_clusters

        push!(result_partitions, c.count)
        if config.extreme_preservation == Afterwards
            rep_vec = [
                getRepresentativeValue(
                    c,
                    modes[j],
                    j,
                    high_thresholds,
                    low_thresholds
                )
                for j in 1:d
            ]
            push!(result_values, rep_vec)
        else
            push!(result_values, c.representative)
        end
        push!(mean_values, c.sum_of_values ./ c.count)
    end

    return result_partitions,
           result_values,
           mean_values,
           ward_errors,
           ldc_errors_per_merge
end