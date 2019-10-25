if [ $# -lt 2 ]
    then
        echo "Usage: <start> <end>";
        exit 1;
fi
for ((i = $1; i < $2; i++));
do
    curl "https://api.brewerydb.com/v2/beers?p=$i&key=8792f7cc9f9f5b58337ab7190decce15&withBreweries=Y&withIngredients=Y" -o page-$i.json;
done
