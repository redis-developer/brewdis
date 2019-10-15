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
	abv NUMERIC SORTABLE \
	ibu NUMERIC SORTABLE \
	style.category.name TAG SORTABLE \
	style.category.id TAG SORTABLE \
	description TEXT \
	name TEXT SORTABLE \
	isOrganic TAG SORTABLE \
	style.id TAG SORTABLE \
	style.name TAG SORTABLE \
	style.description TEXT
riot file-import --file products.json --proc "sku=remove('id')" --index products --keyspace product --keys sku

echo "Creating inventory index"
redis-cli FT.CREATE inventory SCHEMA \
	id TAG SORTABLE \
	store TAG SORTABLE \
	sku TAG SORTABLE \
	abv NUMERIC SORTABLE \
	ibu NUMERIC SORTABLE \
	address TEXT \
	inventoryAvailableToSell TAG SORTABLE \
	city TEXT SORTABLE \
	country TAG SORTABLE \
	isDefault TAG SORTABLE \
	preferred TAG SORTABLE \
	latitude NUMERIC SORTABLE \
	location GEO \
	longitude NUMERIC SORTABLE \
	market TAG SORTABLE \
	isOrganic TAG SORTABLE \
	parent TAG SORTABLE \
	style.category.name TAG SORTABLE \
	description TEXT \
	name TEXT SORTABLE \
	style.name TAG SORTABLE \
	rollupInventory TAG SORTABLE \
	state TAG SORTABLE \
	storeDescription TEXT \
	type TAG SORTABLE \
	postalCode TAG SORTABLE \
	epoch NUMERIC SORTABLE