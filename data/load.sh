redis-cli FT.CREATE stores SCHEMA \
	id TAG SORTABLE \
	address TEXT \
	address2 TEXT \
	address3 TEXT \
	availableToSell TAG SORTABLE \
	city TEXT SORTABLE \
	country TAG SORTABLE \
	description TEXT \
	isDefault TAG SORTABLE \
	isPreferred TAG SORTABLE \
	latitude NUMERIC SORTABLE \
	location GEO \
	longitude NUMERIC SORTABLE \
	market TAG SORTABLE \
	parentDc TAG SORTABLE \
	rollupInventory TAG SORTABLE \
	state TAG SORTABLE \
	type TAG SORTABLE \
	zip TAG SORTABLE
riot file-import --file stores.json --proc "id=#context.index+1" --index stores --keyspace store --keys id
redis-cli FT.CREATE products SCHEMA \
	sku TAG SORTABLE \
	abv NUMERIC SORTABLE \
	category TAG SORTABLE \
	description TEXT \
	name TEXT SORTABLE \
	organic TAG SORTABLE \
	style TAG SORTABLE
riot file-import --file products.json --proc "sku=#context.index+1" --index products --keyspace product --keys sku
redis-cli FT.CREATE inventory SCHEMA \
	store TAG SORTABLE \
	sku TAG SORTABLE \
	abv NUMERIC SORTABLE \
	address TEXT \
	address2 TEXT \
	address3 TEXT \
	availableToSell TAG SORTABLE \
	city TEXT SORTABLE \
	country TAG SORTABLE \
	isDefaultStore TAG SORTABLE \
	isPreferredStore TAG SORTABLE \
	latitude NUMERIC SORTABLE \
	location GEO \
	longitude NUMERIC SORTABLE \
	market TAG SORTABLE \
	organic TAG SORTABLE \
	parentDc TAG SORTABLE \
	productCategory TAG SORTABLE \
	productDescription TEXT \
	productName TEXT SORTABLE \
	productStyle TAG SORTABLE \
	rollupInventory TAG SORTABLE \
	state TAG SORTABLE \
	storeDescription TEXT \
	storeType TAG SORTABLE \
	zip TAG SORTABLE