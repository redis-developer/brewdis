for i in {195..390}
do
    curl "https://api.brewerydb.com/v2/beers?p=$i&key=8792f7cc9f9f5b58337ab7190decce15" | jq .data | json_pp >> beers.json
done
