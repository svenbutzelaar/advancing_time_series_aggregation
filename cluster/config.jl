using ArgParse

@enum ExtremePreservation begin
    NoExtremePreservation
    Afterwards
    SeperateExtremesSum
    SeperateTops
    DynamicProgramming
end


@enum ClusteringMethod begin
    UTR
    PerLocation
    PerProfile
    DemandOverAvailabilities
    Global
end

const UPDATE_AFTER_CLUSTERING = (
    Afterwards,
    # SeperateExtremes,
    SeperateExtremesSum,
    SeperateTops
)

should_update_extremes_after_clustering(ep::ExtremePreservation) =
    ep in UPDATE_AFTER_CLUSTERING

Base.@kwdef struct ClusteringConfig
    calc_stats::Bool = false
    n_prime::Int64 = 8760
    extreme_preservation::ExtremePreservation = NoExtremePreservation
    clustering_method::ClusteringMethod = PerLocation
    high_percentile::Float64 = 0.95
    low_percentile::Float64 = 0.05
    tops_window::Int = 5
    max_block_size::Int = 168
end

function experiment_name(config::ClusteringConfig)
    ep_str = if config.extreme_preservation == SeperateTops
        "SeperateTops_w$(config.tops_window)"
    elseif config.extreme_preservation == DynamicProgramming
        "DynamicProgramming_s$(config.max_block_size)"
    else
        String(Symbol(config.extreme_preservation))
    end

    return join([
        "ward",
        "k$(config.n_prime)",
        lowercase(String(Symbol(config.clustering_method))),
        ep_str,
        "hp$(round(config.high_percentile, digits=2))",
        "lp$(round(config.low_percentile, digits=2))",
    ], "_")
end

function get_config_from_experiment_name(name::String)::ClusteringConfig
    parts = split(name, "_")
    
    # Expected format:
    # ward_k{n_prime}_{clustering_method}_{ep_str}_hp{high_percentile}_lp{low_percentile}
    # where ep_str is either a plain ExtremePreservation name, or "SeperateTops_w{tops_window}"
    
    @assert parts[1] == "ward" "Expected name to start with 'ward', got: $(parts[1])"
    
    # k{n_prime}
    n_prime = parse(Int64, parts[2][2:end])  # strip leading 'k'
    
    # clustering_method (stored as lowercase in experiment_name)
    clustering_method_str = parts[3]
    clustering_method = let
        all = instances(ClusteringMethod)
        idx = findfirst(m -> lowercase(string(m)) == clustering_method_str, all)
        @assert !isnothing(idx) "Unknown clustering method: $clustering_method_str"
        all[idx]  # ← now it's a ClusteringMethod
    end
    
    # hp{high_percentile} and lp{low_percentile} are always the last two parts
    high_percentile = parse(Float64, parts[end-1][3:end])  # strip leading 'hp'
    low_percentile  = parse(Float64, parts[end][3:end])    # strip leading 'lp'
    
    # Everything between index 4 and end-2 is the ep_str (1 or 2 parts)
    ep_parts = parts[4:end-2]
    
    extreme_preservation, tops_window, max_block_size = if length(ep_parts) == 2
        # SeperateTops_w{tops_window}
        if ep_parts[1] == "SeperateTops"
            tops_window = parse(Int, ep_parts[2][2:end])  # strip leading 'w'
            SeperateTops, tops_window, 168
        elseif ep_parts[1] == "DynamicProgramming"
            max_block_size = parse(Int, ep_parts[2][2:end])  # strip leading 'm'
            DynamicProgramming, 5, max_block_size
        else
            throw("Unexpected two-part ep: $(join(ep_parts, "_"))")
        end
    else
        ep_sym = Symbol(ep_parts[1])
        ep = getfield(@__MODULE__, ep_sym)::ExtremePreservation
        ep, 5, 168  # default tops_window
    end

    @show(extreme_preservation, tops_window, max_block_size, clustering_method, parts)
    
    return ClusteringConfig(
        n_prime              = n_prime,
        clustering_method    = clustering_method,
        extreme_preservation = extreme_preservation,
        high_percentile      = high_percentile,
        low_percentile       = low_percentile,
        tops_window          = tops_window,
        max_block_size       = max_block_size,
    )
end

function parse_cli()

    s = ArgParseSettings()

    @add_arg_table s begin
        "script"
            help = "Julia file to run"
            required = true

        "--n_prime"
            arg_type = Int
            default = 8760

        "--calc_stats"
            action = :store_true

        "--calc_ens"
            action = :store_true

        "--extreme_preservation"
            arg_type = String
            default = "NoExtremePreservation"

        "--clustering_method"
            arg_type = String
            default = "PerLocation"

        "--high_percentile"
            arg_type = Float64
            default = 0.95

        "--low_percentile"
            arg_type = Float64
            default = 0.05
        "--tops_window"
            arg_type = Int
            default = 5
        "--max_block_size"
            arg_type = Int
            default = 168
    end

    return parse_args(s)
end