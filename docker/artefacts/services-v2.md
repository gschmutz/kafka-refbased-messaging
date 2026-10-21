# platys-platform - List of Services

| Service | Links | External<br>Port | Internal<br>Port | Description
|--------------|------|------|------|------------
|[akhq](./documentation/services/akhq )|[Web UI](http://192.168.1.112:28345) - [Rest API](http://192.168.1.112:28345/api)|28345<br>28320<br>|8080<br>28081<br>|Kafka GUI
|[jikkou](./documentation/services/jikkou )||||Resource as Code framework for Apache Kafka
|[kafka-1](./documentation/services/kafka )||9092<br>19092<br>29092<br>39092<br>9992<br>1234<br>|9092<br>19092<br>29092<br>39092<br>9992<br>1234<br>|Kafka Broker 1
|[kafka-2](./documentation/services/kafka )||9093<br>19093<br>29093<br>39093<br>9993<br>1235<br>|9093<br>19093<br>29093<br>39093<br>9992<br>1234<br>|Kafka Broker 2
|[kafka-3](./documentation/services/kafka )||9094<br>19094<br>29094<br>39094<br>9994<br>1236<br>|9094<br>19094<br>29094<br>39094<br>9992<br>1234<br>|Kafka Broker 3
|[markdown-viewer](./documentation/services/markdown-viewer )|[Web UI](http://192.168.1.112:80)|80<br>|3000<br>|Platys Platform homepage viewer
|[rustfs-1](./documentation/services/rustfs )|[Web UI](http://192.168.1.112:9014)|9005<br>9014<br>|9000<br>9010<br>|Software-defined Object Storage
|[rustfs-mc](./documentation/services/rustfs )||||RustFS CLI
|[schema-registry-1](./documentation/services/schema-registry )|[Rest API](http://192.168.1.112:8081)|8081<br>|8081<br>|Confluent Schema Registry|

**Note:** init container ("init: true") are not shown