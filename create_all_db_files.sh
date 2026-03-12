for method in NoExtremePreservation Afterwards DuringClustering SeperateExtremes SeperateExtremesSum; do
    for n in 50 100 200 500 1000 1500; do
    julia --project cli.jl create_obz_db.jl \
        --n_prime=$n \
        --extreme_preservation=$method
done