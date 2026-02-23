using Statistics
using DataStructures

mutable struct LinkedListNode
    start_index::Int
    end_index::Int
    sum_of_values::Vector{Float64}
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
    return (2 * c1.count * c2.count) / (c1.count + c2.count) * sqdist
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
        n_prime::Int;
    )
    n, d = size(values)

    errors_per_merge = Float64[]
    result_values = Vector{Vector{Float64}}()

    # Create initial clusters
    clusters = [LinkedListNode(i, i, view(values, i, :)) for i in 1:n]
    for i in 1:n-1
        clusters[i].next_node = clusters[i+1]
        clusters[i+1].prev_node = clusters[i]
    end

    # Priority queue for merges using proper binary heap
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

        entry = pop!(heap)  # O(log n) operation

        # Check if clusters are still valid and adjacent
        if !(entry.c1.active && entry.c2.active && entry.c1.next_node === entry.c2)
            continue
        end

        push!(errors_per_merge, entry.ward_criterion)
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
    result = Int[]
    active_clusters = filter(c -> c.active, clusters)

    for c in active_clusters
        push!(result, c.count)
        push!(result_values, c.centroid)
    end

    return result, result_values, errors_per_merge
end