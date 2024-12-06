#!/bin/bash
cd "$(dirname "$0")"

# Skript pre stiahnutie datasetov z webu IMDb
# Kvôli ich veľkosti (a faktu, že sa aktualizujú denne) som ich nezahrnul do repozitára

subory=(
    "title.akas.tsv.gz"
    "title.basics.tsv.gz"
    "title.crew.tsv.gz"
    "title.episode.tsv.gz"
    "title.principals.tsv.gz"
    "title.ratings.tsv.gz"
)

for subor in "${subory[@]}"; do
    echo "Sťahovanie $subor..."
    curl -O "https://datasets.imdbws.com/$subor"
done

echo "Všetky datasety boli stiahnuté."
