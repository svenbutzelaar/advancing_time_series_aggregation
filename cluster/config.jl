@enum ExtremePreservation begin
    NoExtremePreservation
    Afterwards
    DuringClustering
end

Base.@kwdef struct ClusteringConfig
    calc_stats::Bool = false
    extreme_preservation::ExtremePreservation = NoExtremePreservation
    dependant_per_location::Bool = true
    high_percentile::Float64 = 0.95
    low_percentile::Float64 = 0.05
end

function experiment_name(config::ClusteringConfig, n_prime::Int)
    return join([
        "ward",
        "k$(n_prime)",
        config.dependant_per_location ? "perlocation" : "perprofile",
        String(Symbol(config.extreme_preservation)),
        "hp$(round(config.high_percentile, digits=2))",
        "lp$(round(config.low_percentile, digits=2))",
    ], "_")
end
