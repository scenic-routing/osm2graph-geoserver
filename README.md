# Load Geo Graph Data to GeoServer/PostGIS

This module reads relationships from a Neo4j graph database and loads their geometries (e.g `LINESTRING(-122.399779 37.753165,-122.3995458 37.7530642)`) into PostGIS/GeoServer so they can be visualized as WMS/WFS layers.

This module is designed to process graphs created by [scenic-routing/osm2graph-neo4j](https://github.com/scenic-routing/osm2graph-neo4j). Each relationship encounted in the graph with the `associatedData` property set will be loaded into a "visualization table" in PostGIS. This property can be set by modules like [scenic-routing/elevation-geotiff](https://github.com/scenic-routing/elevation-geotiff).

Next, a workspace and PostGIS store will be created in GeoServer, and a feature type (essentially a layer) will be created for each "visualization table" that was created in PostGIS (essentially publishing the relationship's `associatedData`).

**Note:** This module depends on external PostGIS and GeoServer instances that can be connected to from the JVM. Connection information must be configured in a `.json` config file (see Configuration section below).

## Build

Built with the Java 11 JDK and Neo4j Server Community (version: 4.4.1). Code should be fairly portable, as no advanced features of Java or Neo4j are used.

This is a maven project and can be built using:
```
mvn clean install
```

Make sure the Neo4j library version in the `pom.xml` file matches the Neo4j Server version. Available versions in Maven Central can be found [here](https://mvnrepository.com/artifact/org.neo4j/neo4j).

## Configure

This module requires the path to a `.json` configuration file to be set in the `CONFIG_PATH` environment variable. This can be set with a command like:

```
export CONFIG_PATH=/development/osm2graph-geoserver/sample-config.json
```

A sample config file is [provided here](./sample-config.json). Make sure all values are populated and GeoServer/PostGIS connection information is correct.

## Run

Once the config file path is set in `CONFIG_PATH`, ensure the PostGIS and GeoServer instance are running, and execute the following command:

```
java -jar target/osm2graph-geoserver-0.0.3-SNAPSHOT.jar
```

## Test

Tests can be executed with:

```
mvn test
```
