for i in {1..23}
do
    curl "https://sandbox-api.brewerydb.com/v2/beers?p=$i&key=830b1c0765823374301a4e66579641fd" | jq .data | json_pp >> beers.json
done