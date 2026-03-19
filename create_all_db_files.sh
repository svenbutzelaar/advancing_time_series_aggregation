for method in NoExtremePreservation Afterwards SeperateExtremesSum; do
    # for n in 50 100 200 500 1000 1500; do
    for n in 750 1250 2000; do
        julia --project cli.jl create_obz_db_fast.jl \
            --n_prime=$n \
            --extreme_preservation=$method
    done
done