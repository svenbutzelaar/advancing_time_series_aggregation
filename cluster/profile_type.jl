
@enum ProfileType begin
    Demand
    Solar
    WindOnshore
    WindOffshore
    Unknown
end

function getProfileType(profile_name::AbstractString)::ProfileType
    name = lowercase(profile_name)

    if occursin("demand", name)
        return Demand
    elseif occursin("solar", name)
        return Solar
    elseif occursin("wind_onshore", name) || occursin("onshore", name)
        return WindOnshore
    elseif occursin("wind_offshore", name) || occursin("offshore", name)
        return WindOffshore
    else
        return Unknown
    end
end