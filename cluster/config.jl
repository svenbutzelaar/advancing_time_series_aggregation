using ArgParse

@enum ExtremePreservation begin
    NoExtremePreservation
    Afterwards
    DuringClustering
    SeperateExtremes
    SeperateExtremesSum
end

const UPDATE_AFTER_CLUSTERING = (
    Afterwards,
    SeperateExtremes,
    SeperateExtremesSum,
)

should_update_extremes_after_clustering(ep::ExtremePreservation) =
    ep in UPDATE_AFTER_CLUSTERING

Base.@kwdef struct ClusteringConfig
    calc_stats::Bool = false
    n_prime::Int64 = 8760
    extreme_preservation::ExtremePreservation = NoExtremePreservation
    dependant_per_location::Bool = true
    high_percentile::Float64 = 0.95
    low_percentile::Float64 = 0.05
end

function experiment_name(config::ClusteringConfig)
    return join([
        "ward",
        "k$(config.n_prime)",
        config.dependant_per_location ? "perlocation" : "perprofile",
        String(Symbol(config.extreme_preservation)),
        "hp$(round(config.high_percentile, digits=2))",
        "lp$(round(config.low_percentile, digits=2))",
    ], "_")
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

        "--dependant_per_location"
            arg_type = Bool
            default = true

        "--high_percentile"
            arg_type = Float64
            default = 0.95

        "--low_percentile"
            arg_type = Float64
            default = 0.05
    end

    return parse_args(s)
end