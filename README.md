# SparkQueryExecutor

## Compile:
    sbt test package

## Run pipeline:
    /spark/bin/spark-submit \
        --name "Spark Query Executor 2" \
        --properties-file src/main/resources/application.properties \
        --jars $(echo dependencies/*.jar | tr ' ' ',') \
        --deploy-mode cluster \
        target/scala-2.12/sparkqueryexecutor2*.jar

