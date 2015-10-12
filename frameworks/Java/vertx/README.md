# Vertx Benchmarking Test

This is the vertx portion of a [benchmarking test suite](../) comparing a variety of web development platforms.

Minimal attempts have been made for performance.  

### Plaintext Test

* [Plaintext test source](WebServer.java)

### JSON Serialization Test

* [JSON test source](WebServer.java)

### Database Single query Test

* [Database Single query test source](WebServer.java)

### Database Multiple queries Test

* [Database Multiple queries test source](WebServer.java)

### Database Data updates Test

* [Database Data updates test source](WebServer.java)

### Fortunes Test

* [Fortunes test source](WebServer.java)

## Versions

* [Java OpenJDK 1.8](http://openjdk.java.net/)
* [vertx 3.0](http://vertx.io/)
* [vertx-mongo-client](http://vertx.io)
* [freemarker 2.3.22](http://freemarker.org)

## Test URLs

### Plaintext Test

    http://localhost:8080/plaintext

### JSON Encoding Test

    http://localhost:8080/json

### Database Mapping Test

    http://localhost:8080/db?queries=5

### Database Single query Test

    http://localhost:8080/db

### Database Multiple queries Test

    http://localhost:8080/queries?queries=5

### Database Data updates Test

    http://localhost:8080/updates?queries=3

### Fortunes Test

    http://localhost:8080/fortunes
