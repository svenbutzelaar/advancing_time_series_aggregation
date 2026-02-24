using Statistics
using DataStructures


include("profile_type.jl")

mutable struct LinkedListNode
    start_index::Int
    end_index::Int
    sum_of_values::Vector{Float64}
    max_of_values::Vector{Float64}
    min_of_values::Vector{Float64}
    count::Int
    centroid::Vector{Float64}
    prev_node::Union{Nothing, LinkedListNode}
    next_node::Union{Nothing, LinkedListNode}
    active::Bool

    function LinkedListNode(start_index::Int, end_index::Int, value::AbstractVector{<:Float64})
        v = collect(value)   # ensure concrete Vector{Float64}
        new(
            start_index,
            end_index,
            v,
            copy(v),  # max
            copy(v),  # min
            1,
            copy(v),
            nothing,
            nothing,
            true
        )
    end
end

# Merge two adjacent clusters
function merge_nodes!(c1::LinkedListNode, c2::LinkedListNode)
    c1.end_index = c2.end_index
    c1.sum_of_values .+= c2.sum_of_values
    c1.max_of_values .= max.(c1.max_of_values, c2.max_of_values)
    c1.min_of_values .= min.(c1.min_of_values, c2.min_of_values)
    c1.count += c2.count
    c1.centroid .= c1.sum_of_values ./ c1.count
    c1.next_node = c2.next_node

    if c2.next_node !== nothing
        c2.next_node.prev_node = c1
    end

    c2.active = false
    c2.prev_node = nothing
    c2.next_node = nothing
end

# Compute Ward's linkage criterion (increase in within-cluster variance)
function compute_ward_dissimilarity(c1::LinkedListNode, c2::LinkedListNode)
    diff = c1.centroid .- c2.centroid
    sqdist = sum(diff .* diff)
    return (c1.count * c2.count) / (c1.count + c2.count) * sqdist
end

function getRepresentativeValue(
        c::LinkedListNode,
        mode::ProfileType,
        j::Int,
        high_thresholds::Vector{Float64},
        low_thresholds::Vector{Float64},
        do_extreme_preservation::Bool,
    )

    mean_val = c.sum_of_values[j] / c.count
    if !do_extreme_preservation
        return mean_val
    end

    if mode == Demand
        if c.max_of_values[j] >= high_thresholds[j]
            # @show c.max_of_values[j], mode, c.centroid[j], c.count
            return c.max_of_values[j]
        else
            return mean_val
        end

    elseif mode == Solar || mode == WindOnshore || mode == WindOffshore
        if c.min_of_values[j] <= low_thresholds[j]
            # @show c.min_of_values[j], mode, c.centroid[j], c.count
            return c.min_of_values[j]
        else
            return mean_val
        end

    else
        return mean_val
    end
end

# Wrapper struct for heap entries to enable comparison
struct HeapEntry
    ward_criterion::Float64
    c1::LinkedListNode
    c2::LinkedListNode
end

# Define comparison for min-heap (based on Ward criterion)
Base.isless(a::HeapEntry, b::HeapEntry) = a.ward_criterion < b.ward_criterion

# Internal clustering function
function hierarchical_time_clustering_ward(
        values::Matrix{Float64}, 
        n_prime::Int,
        modes::Vector{ProfileType};
        calc_stats=false,
        do_extreme_preservation=true,
        high_percentile=0.95,
        low_percentile=0.05,
    )
    n, d = size(values)
    @assert length(modes) == d "Length of modes must match number of columns"
    
    high_thresholds = zeros(d)
    low_thresholds  = zeros(d)

    for j in 1:d
        sorted_col = sort(values[:, j])
        high_idx = ceil(Int, high_percentile * n)
        low_idx  = ceil(Int, low_percentile * n)

        high_thresholds[j] = sorted_col[high_idx]
        low_thresholds[j]  = sorted_col[low_idx]
    end
    
    if calc_stats
        full_resolution_values_sorted = Vector{Vector{Float64}}()
        for j in 1:d
            push!(full_resolution_values_sorted, sort(values[:, j]; rev=true))
        end
    end

    # @show high_thresholds
    # @show low_thresholds

    ward_errors = Vector{Vector{Float64}}()
    ldc_errors_per_merge = Vector{Vector{Float64}}()
    result_values = Vector{Vector{Float64}}()
    mean_values = Vector{Vector{Float64}}()

    # Create initial clusters
    clusters = [LinkedListNode(i, i, view(values, i, :)) for i in 1:n]
    for i in 1:n-1
        clusters[i].next_node = clusters[i+1]
        clusters[i+1].prev_node = clusters[i]
    end

    # Priority queue for merges
    heap = MutableBinaryMinHeap{HeapEntry}()

    function push_merge(c1::LinkedListNode, c2::LinkedListNode)
        if c1.active && c2.active
            ward_crit = compute_ward_dissimilarity(c1, c2)
            entry = HeapEntry(ward_crit,  c1, c2)
            push!(heap, entry)
        end
    end

    # Add initial adjacent pairs
    for i in 1:n-1
        push_merge(clusters[i], clusters[i+1])
    end

    merges = 0
    total_merges = n - n_prime

    while merges < total_merges
        if isempty(heap)
            break
        end

        entry = pop!(heap)

        # Validate clusters
        if !(entry.c1.active && entry.c2.active && entry.c1.next_node === entry.c2)
            continue
        end

        if calc_stats
            ldc_error_vec = Float64[]
            ward_error_vec = Float64[]

            # Can be faster by always keeping track of active clusters
            active_clusters = filter(c -> c.active, clusters)

            for j in 1:d
                merged_values = Float64[]
                    for c in active_clusters
                        rep_val = getRepresentativeValue(
                            c,
                            modes[j],
                            j,
                            high_thresholds,
                            low_thresholds,
                            do_extreme_preservation
                        )
                        for _ in 1:c.count
                            push!(merged_values, rep_val)
                        end
                    end
                merged_sorted = sort(merged_values; rev=true)

                # Root Mean Squared Error (RMSE) of LDC error
                n_total = length(merged_sorted)
                mse = sum((full_resolution_values_sorted[j] .- merged_sorted).^2) / n_total
                rmse = sqrt(mse)
                push!(ldc_error_vec, rmse)
                
                # sum Squared errors 
                push!(ward_error_vec, sum((values[:, j] .- merged_values).^2))
            end

            push!(ldc_errors_per_merge, ldc_error_vec)
            push!(ward_errors, ward_error_vec)
        end

        # Merge nodes
        merge_nodes!(entry.c1, entry.c2)

        merges += 1

        # Add new merge candidates
        if entry.c1.prev_node !== nothing && entry.c1.prev_node.active
            push_merge(entry.c1.prev_node, entry.c1)
        end
        if entry.c1.next_node !== nothing && entry.c1.next_node.active
            push_merge(entry.c1, entry.c1.next_node)
        end
    end

    # Collect results
    result_partitions = Int[]
    active_clusters = filter(c -> c.active, clusters)

    for c in active_clusters
        push!(result_partitions, c.count)
        rep_vec = Vector{Float64}(undef, d)
        for j in 1:d
            rep_vec[j] = getRepresentativeValue(
                c,
                modes[j],
                j,
                high_thresholds,
                low_thresholds,
                do_extreme_preservation
            )
        end
        push!(result_values, rep_vec)
        push!(mean_values, c.centroid)
    end

    return result_partitions, result_values, mean_values, ward_errors, ldc_errors_per_merge
end