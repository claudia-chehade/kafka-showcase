docker exec -it starter_kafka0_1  kafka-topics --create --topic foo --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181

## eine bsh im docker öffnen,dann kann ich alle kafka commands direkt im docker ausführen
```
docker exec -it starter_kafka0_1  bash
```

um die bsh zu verlassen
```
CTRK+d
```
zum Beispiel den producer starten

https://docs.docker.com/engine/reference/commandline/run/