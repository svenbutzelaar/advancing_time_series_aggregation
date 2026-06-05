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
    FullResolution
end

@enum Dataset begin
    BaseDataset
    LowVar
    HighVar
end

const UPDATE_AFTER_CLUSTERING = (
    Afterwards,
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
    dataset::Dataset = BaseDataset
end

function dataset_db_file(dataset::Dataset)
    return if dataset == BaseDataset
        "db_files/base_db.db"
    elseif dataset == LowVar
        "db_files/low_var.db"
    elseif dataset == HighVar
        "db_files/high_var.db"
    end
end

function dataset_db_full_resolution_file(dataset::Dataset)
    return if dataset == BaseDataset
        "db_files/obz-invest-full-resolution-base.db"
    elseif dataset == LowVar
        "db_files/obz-invest-full-resolution-low-var.db"
    elseif dataset == HighVar
        "db_files/obz-invest-full-resolution-high-var.db"
    end
end

function experiment_name(config::ClusteringConfig)
    ep_str = if config.extreme_preservation == SeperateTops
        "SeperateTops_w$(config.tops_window)"
    elseif config.extreme_preservation == DynamicProgramming
        "DynamicProgramming_s$(config.max_block_size)"
    else
        String(Symbol(config.extreme_preservation))
    end

    dataset_str = lowercase(String(Symbol(config.dataset)))

    return join([
        "ward",
        "k$(config.n_prime)",
        lowercase(String(Symbol(config.clustering_method))),
        ep_str,
        "hp$(round(config.high_percentile, digits=2))",
        "lp$(round(config.low_percentile, digits=2))",
        dataset_str,
    ], "_")
end

function get_config_from_experiment_name(name::String)::ClusteringConfig
    parts = split(name, "_")

    @assert parts[1] == "ward" "Expected name to start with 'ward', got: $(parts[1])"

    n_prime = parse(Int64, parts[2][2:end])

    clustering_method_str = parts[3]
    clustering_method = let
        all = instances(ClusteringMethod)
        idx = findfirst(m -> lowercase(string(m)) == clustering_method_str, all)
        @assert !isnothing(idx) "Unknown clustering method: $clustering_method_str"
        all[idx]
    end

    # dataset is always the last part
    dataset_str = parts[end]
    dataset = let
        all = instances(Dataset)
        idx = findfirst(d -> lowercase(string(d)) == dataset_str, all)
        @assert !isnothing(idx) "Unknown dataset: $dataset_str"
        all[idx]
    end

    high_percentile = parse(Float64, parts[end-2][3:end])
    low_percentile  = parse(Float64, parts[end-1][3:end])

    ep_parts = parts[4:end-3]

    extreme_preservation, tops_window, max_block_size = if length(ep_parts) == 2
        if ep_parts[1] == "SeperateTops"
            tops_window = parse(Int, ep_parts[2][2:end])
            SeperateTops, tops_window, 168
        elseif ep_parts[1] == "DynamicProgramming"
            max_block_size = parse(Int, ep_parts[2][2:end])
            DynamicProgramming, 5, max_block_size
        else
            throw("Unexpected two-part ep: $(join(ep_parts, "_"))")
        end
    else
        ep_sym = Symbol(ep_parts[1])
        ep = getfield(@__MODULE__, ep_sym)::ExtremePreservation
        ep, 5, 168
    end

    return ClusteringConfig(
        n_prime              = n_prime,
        clustering_method    = clustering_method,
        extreme_preservation = extreme_preservation,
        high_percentile      = high_percentile,
        low_percentile       = low_percentile,
        tops_window          = tops_window,
        max_block_size       = max_block_size,
        dataset              = dataset,
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

        "--dataset"
            arg_type = String
            default = "BaseDataset"
    end

    return parse_args(s)
end