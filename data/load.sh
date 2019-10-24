redis-cli flushall
echo "Creating stores index"
redis-cli FT.CREATE stores SCHEMA \
	store TAG SORTABLE \
	description TEXT \
	market TAG SORTABLE \
	parent TAG SORTABLE \
	address TEXT \
	city TEXT SORTABLE \
	country TAG SORTABLE \
	inventoryAvailableToSell TAG SORTABLE \
	isDefault TAG SORTABLE \
	preferred TAG SORTABLE \
	latitude NUMERIC SORTABLE \
	location GEO \
	longitude NUMERIC SORTABLE \
	rollupInventory TAG SORTABLE \
	state TAG SORTABLE \
	type TAG SORTABLE \
	postalCode TAG SORTABLE
riot file-import --file stores.csv --header --proc "location=#geo(longitude,latitude)" --index stores --keyspace store --keys store

echo "Creating products index"
redis-cli FT.CREATE products SCHEMA \
	sku TAG SORTABLE \
	name TEXT SORTABLE \
	description TEXT PHONETIC dm:en \
	category TAG SORTABLE \
	categoryName TEXT SORTABLE \
	style TAG SORTABLE \
	styleName TEXT SORTABLE \
	brewery TAG SORTABLE \
	breweryName TEXT SORTABLE \
	isOrganic TAG SORTABLE \
	abv NUMERIC SORTABLE \
	label TAG SORTABLE \
	ibu NUMERIC SORTABLE
riot file-import --file brewerydb/products.json.gz --proc "sku=id" "label=containsKey('labels')" "category=style.category.id" "categoryName=style.category.name" "styleName=style.shortName" "style=style.id" "brewery=containsKey('breweries')?breweries[0].id:null" "breweryName=containsKey('breweries')?breweries[0].nameShortDisplay:null" "breweryIcon=containsKey('breweries')?breweries[0].containsKey('images')?breweries[0].get('images').get('icon'):null:null" --index products --keyspace product --keys sku

echo "Creating inventory index"
redis-cli FT.CREATE inventory SCHEMA \
	id TAG SORTABLE \
	store TAG SORTABLE \
	sku TAG SORTABLE \
	location GEO \
	availableToPromise NUMERIC SORTABLE \
	onHand NUMERIC SORTABLE \
	allocated NUMERIC SORTABLE \
	reserved NUMERIC SORTABLE \
	virtualHold NUMERIC SORTABLE \
	epoch NUMERIC SORTABLE