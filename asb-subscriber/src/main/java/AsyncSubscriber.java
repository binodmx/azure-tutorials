import com.azure.core.amqp.AmqpRetryMode;
import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.messaging.servicebus.ServiceBusReceiverAsyncClient;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusException;

import java.time.Duration;

public class AsyncSubscriber {
    static String connectionString = Environment.connectionString1;
    static String topicName = Environment.topicName;
    static String subName = Environment.subscriptionName;
    static ServiceBusReceiverAsyncClient serviceBusReceiverAsyncClient;

    public static void main(String[] args) throws InterruptedException {
        // create Retry Options for the Service Bus client
        AmqpRetryOptions amqpRetryOptions = new AmqpRetryOptions();
        amqpRetryOptions.setDelay(Duration.ofSeconds(1));
        amqpRetryOptions.setMaxRetries(5);
        amqpRetryOptions.setMaxDelay(Duration.ofSeconds(15));
        amqpRetryOptions.setMode(AmqpRetryMode.EXPONENTIAL);
        amqpRetryOptions.setTryTimeout(Duration.ofSeconds(5));

        // instantiate a client that will be used to call the service
        serviceBusReceiverAsyncClient = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .retryOptions(amqpRetryOptions)
                .receiver()
                .topicName(topicName)
                .subscriptionName(subName)
                .buildAsyncClient();

        // listening to Service Bus to receive messages
        receiveMessages();
    }

    static void receiveMessages() {
        long startTime = System.currentTimeMillis();

        // receive messages
        serviceBusReceiverAsyncClient.receiveMessages()
            .flatMap(
                serviceBusReceivedMessage -> {
                    System.out.printf("Sequence #: %s. Contents: %s%n", serviceBusReceivedMessage.getSequenceNumber(),
                            serviceBusReceivedMessage.getBody());
                    return serviceBusReceiverAsyncClient.complete(serviceBusReceivedMessage);
                }
            ).subscribe(
                (ignore) -> System.out.println("Message received."),
                error -> {
                    System.out.println("error: " + error);
                    System.out.println("error.getMessage(): " + error.getMessage());
                    System.out.println("error.getLocalizedMessage(): " + error.getLocalizedMessage());
                    System.out.println("error.fillInStackTrace(): " + error.fillInStackTrace());
                    System.out.println("error.getCause(): " + error.getCause());
                    long stopTime = System.currentTimeMillis();
                    System.out.println("Message not received");
                    System.out.println("Time elapsed: " + (stopTime - startTime) + "ms");
                    ServiceBusException serviceBusException = (ServiceBusException) error;
                    System.out.println("isTransient: " + serviceBusException.isTransient());
                }
            );
    }
}
