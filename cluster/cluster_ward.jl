using Statistics
using DataStructures

# Define a LinkedListNode struct
mutable struct LinkedListNode
    start_index::Int
    end_index::Int
    sum_of_values::Float64
    count::Int
    centroid::Float64
    min_v::Float64
    max_v::Float64
    prev_node::Union{Nothing, LinkedListNode}
    next_node::Union{Nothing, LinkedListNode}
    active::Bool

    function LinkedListNode(start_index::Int, end_index::Int, value::Float64)
        new(start_index, end_index, value, 1, value, value, value, nothing, nothing, true)
    end
end

# Merge two adjacent clusters
function merge_nodes!(c1::LinkedListNode, c2::LinkedListNode)
    c1.end_index = c2.end_index
    c1.sum_of_values += c2.sum_of_values
    c1.count += c2.count
    c1.centroid = c1.sum_of_values / c1.count
    c1.next_node = c2.next_node
    c1.min_v = min(c1.min_v, c2.min_v)
    c1.max_v = max(c1.max_v, c2.max_v)

    if c2.next_node !== nothing
        c2.next_node.prev_node = c1
    end

    c2.active = false
    # Clean up references to help with memory management
    c2.prev_node = nothing
    c2.next_node = nothing
end

# Compute Ward's linkage criterion (increase in within-cluster variance)
function compute_ward_dissimilarity(c1::LinkedListNode, c2::LinkedListNode)
    return (2 * c1.count * c2.count) / (c1.count + c2.count) * (c1.centroid - c2.centroid)^2
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
function hierarchical_time_clustering_ward(values::Vector{Float64}, n_prime::Int)
    n = length(values)

    # Create initial clusters
    clusters = [LinkedListNode(i, i, values[i]) for i in 1:n]
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
    total_within_cluster_variance = 0.0
    active_clusters = filter(c -> c.active, clusters)

    for c in active_clusters
        cluster_values = values[c.start_index:c.end_index]
        # Use L2 (variance) to be consistent with Ward's method
        variance = sum((cluster_values .- c.centroid).^2)
        total_within_cluster_variance += variance
        push!(result, c.count)
    end

    stats = Dict(
        "num_clusters" => length(active_clusters),
        "avg_cluster_size" => n / length(active_clusters),
        "total_within_cluster_variance" => total_within_cluster_variance
    )

    return result, stats
end