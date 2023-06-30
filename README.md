# Brewdis


Real-time inventory demo based on data from [https://brewerydb.com](brewerydb.com).

## Tech Stack


- Spring Boot
- Gradle 6.8.3
- Kordamp Gradle plugins 0.46.0.
- Redis
- Redis Search



## Architecture

![image](https://raw.githubusercontent.com/redis-developer/brewdis/master/app/architecture.svg)





## Run the demo

```
git clone https://github.com/redis-developer/brewdis.git
cd brewdis
docker-compose up
```

Access the demo at http://localhost

## Demo Steps



## Products

Launch `redis-cli`

### Show number of documents inRedis Searchindex:

```
FT.INFO products
```

### Run simple keyword search:

```
FT.SEARCH products chambly
```

TIP: `name`, `description`, `breweryName` are phonetic text fields so you will notice results containing words that sound similar.

### Run prefix search:

```
`FT.SEARCH products chamb*`
```

- Open http://localhost[]
- Enter a simple keyword search, e.g. `chambly`. Note highlighted matches.
- Expand the filter panel by clicking on the filter button (image:https://pic.onlinewebfonts.com/svg/img_3152.png[width=24])
- Enter some characters in the Brewery field to retrieve suggestions fromRedis Search(e.g. `Unib`)
- Click the `Submit` button
- Refine the search by adding a constraint on the alcohol content (ABV field):

```
`@abv:[7 9]`
```
- Change the sort-by field to `ABV` and click `Submit`


### Availability

Click `Availability` on one of the search results. This takes you to the availability map for that product.
. The map shows stores near you where the selected product is currently available.
. Stores in `green` have more than 20 in stock, `amber`: 10 to 20, `red`: less than 10

### Inventory

- Click on a store and then on the link that pops up
- This takes you to the real-time inventory for that store
- The *Available to Promise* field is updated in real-time based on current difference between supply (*On Hand*) and demand (*Reserved + Allocated + Virtual Hold*).


### Configuration

The app server is built with Spring Boot which can be configured different ways: [Spring Boot Externalized Configuration](https://docs.spring.io/spring-boot/docs/2.2.x/reference/html/spring-boot-features.html#boot-features-external-config)

Depending on the way you're running the demo you can either:

- create a `application.properties` file based on the [one that ships with Brewdis](https://github.com/redis-developer/brewdis/blob/master/demo/brewdis-api/src/main/resources/application.properties)
- specify JVM arguments like this:


```
java -jar brewdis.jar --spring.redis.host=localhost --spring.redis.port=6379 ...
```

- use environment variables:


```
export spring.redis.host=localhost
export spring.redis.port=8080
export ...
java -jar brewdis.jar
```

Here are the most common configuration options for this demo:

- `spring.redis.host`: Redis database hostname (default: `localhost`)
- `spring.redis.port`: Redis database port (default: `6379`)
- `stomp.host`: Websocket server hostname (default: `localhost`)
- `stomp.port`: Websocket server port (default: `8080`)
- `stomp.protocol`: Websocket protocol (default: `ws`). Use `wss` for secure websockets
- `inventory.generator.rate`: duration in millis the generator should sleep between inventory updates (default: `100`)
- `availability-radius`: radius to find in-store availability (default: `25 mi`)
