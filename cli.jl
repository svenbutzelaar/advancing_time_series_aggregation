include("cluster/config.jl")

function main()

    args = parse_cli()

    ep = getfield(Main, Symbol(args["extreme_preservation"]))::ExtremePreservation

    config = ClusteringConfig(
        calc_stats = args["calc_stats"],
        n_prime = args["n_prime"],
        extreme_preservation = ep,
        dependant_per_location = args["dependant_per_location"],
        high_percentile = args["high_percentile"],
        low_percentile = args["low_percentile"],
    )

    script = args["script"]

    println("Running: $script")
    println("Config: ", config)
    println("Experiment: ", experiment_name(config))

    # Make config and calc_ens globally available to included scripts
    global CONFIG = config
    global CALC_ENS = args["calc_ens"]

    include(script)

end

main()