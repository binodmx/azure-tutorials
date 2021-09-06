import com.azure.core.amqp.AmqpRetryMode;
import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.messaging.servicebus.ServiceBusReceiverAsyncClient;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;

import java.time.Duration;

public class AsyncSubscriber {
    static String connectionString = Environment.connectionString;
    static String topicName = Environment.topicName;
    static String subName = Environment.subscriptionName;
    static ServiceBusReceiverAsyncClient serviceBusReceiverAsyncClient;

    public static void main(String[] args) {
        // Create Retry Options for the Service Bus client
        AmqpRetryOptions amqpRetryOptions = new AmqpRetryOptions();
        amqpRetryOptions.setDelay(Duration.ofSeconds(1));
        amqpRetryOptions.setMaxRetries(5);
        amqpRetryOptions.setMaxDelay(Duration.ofSeconds(15));
        amqpRetryOptions.setMode(AmqpRetryMode.EXPONENTIAL);
        amqpRetryOptions.setTryTimeout(Duration.ofSeconds(5));

        // Create a Service Bus async receiver client for the topic
        serviceBusReceiverAsyncClient = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .retryOptions(amqpRetryOptions)
                .receiver()
                .topicName(topicName)
                .subscriptionName(subName)
                .buildAsyncClient();

        // Listen to Service Bus to receive messages
        receiveMessages();
    }

    static void receiveMessages() {
        serviceBusReceiverAsyncClient.receiveMessages().subscribe(
                message -> System.out.println("Message received: " + message.getBody()),
                error -> System.out.println("Error occurred while receiving message: " + error)
            );
    }
}
