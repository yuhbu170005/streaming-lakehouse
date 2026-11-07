FROM apache/iceberg-rest-fixture:1.10.1
USER root
RUN curl -fsSLo postgres.jar https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.10/postgresql-42.7.10.jar
USER iceberg:iceberg
CMD ["java", "-cp", "iceberg-rest-adapter.jar:postgres.jar", "org.apache.iceberg.rest.RESTCatalogServer"]
