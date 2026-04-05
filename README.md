# zoomap
Trying vibe coding with a simple app displaying POIs on a map

# Description
A simple web app for zoo visitors to see what animals are shown.
Features in descending priority are
* Display animals in a list
* Display animals on a map (allowing points and polygons for enclosures)
* Full Text search for animals in list
* Selecting animals in list to highlight on map

# Documentation

For a detailed description of the system design see [Architecture](docs/architecture.adoc).

# Plan
* Simplest possible frontend (no react or similar)
* Using osm poi data
  * Downloading relevant POIs
  * Serving relevant POIs from the local directory, not using osm api
* Using a local json file as database
  * using geojson
  * adding relevant metadata 
* Using a free tile provider like osm for simple background tiles