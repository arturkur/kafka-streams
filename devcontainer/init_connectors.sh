curl --location 'localhost:8083/connectors' \
--header 'Content-Type: application/json' \
--data '{
    "name": "sock-shop-source",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "database.hostname": "database",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "nopass",
        "database.dbname" : "sock_shop",
        "database.server.name": "sock_shop",
        "database.whitelist": "sock_shop",
        "schema.whitelist": "public",
        "database.history.kafka.bootstrap.servers": "kafka:9092",
        "database.history.kafka.topic": "schema-changes.sock-shop",
        "topic.prefix": "sock-shop",
        "plugin.name": "pgoutput"
    }
}' | jq .

curl --location 'localhost:8083/connectors' \
--header 'Content-Type: application/json' \
--data '{
    "name": "sock-shop-sink",
    "config": {
        "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
        "tasks.max": "1",
        "topics": "customers",
        "connection.url": "jdbc:postgresql://database:5432/sock_shop",
        "connection.username": "postgres",
        "connection.password": "nopass",
        "insert.mode": "upsert",
        "primary.key.mode": "record_value",
        "primary.key.fields": "id",
        "topics": "sock, purchase, sale, result",
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"
    }
}' | jq .