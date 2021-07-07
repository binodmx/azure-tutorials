# azure-tutorials

### azure-event-hubs

> Make sure to define connection strings and other resources names in Sender.java and Receiver.java

1. Go to `receiver` directory and run `mvn package exec:java -Dexec.mainClass=Receiver`
2. Go to `sender` directory and run `mvn package exec:java -Dexec.mainClass=Sender`

### azure-service-bus

> Make sure to define connection strings and other resources names in Publisher.java and Subscriber.java

1. Go to `publisher` directory and run `mvn package exec:java -Dexec.mainClass=Publisher`
2. Go to `subscriber` directory and run `mvn package exec:java -Dexec.mainClass=Subscriber`
