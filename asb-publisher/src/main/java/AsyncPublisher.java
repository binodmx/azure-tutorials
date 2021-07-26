import com.azure.core.amqp.AmqpRetryMode;
import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusException;
import com.azure.messaging.servicebus.ServiceBusSenderAsyncClient;
import com.microsoft.azure.servicebus.management.ManagementClient;
import com.microsoft.azure.servicebus.management.TopicDescription;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class AsyncPublisher {
    static String connectionString = Environment.connectionString1;
    static String topicName = Environment.topicName;
    static ServiceBusSenderAsyncClient serviceBusSenderAsyncClient;

    public static void main(String[] args) throws InterruptedException {
        // create topic if topic does not exist
        ConnectionStringBuilder connectionStringBuilder = new ConnectionStringBuilder(connectionString);
        ManagementClient managementClient = new ManagementClient(connectionStringBuilder);
        try {
            if (!managementClient.topicExists(topicName)) {
                TopicDescription topicDescription = managementClient.createTopic(topicName);
                System.out.println(topicDescription);
            }
        } catch (com.microsoft.azure.servicebus.primitives.ServiceBusException e) {
            System.out.println(e);
        } catch (InterruptedException e) {
            System.out.println(e);
        }

        // create Retry Options for the Service Bus client
        AmqpRetryOptions amqpRetryOptions = new AmqpRetryOptions();
        amqpRetryOptions.setDelay(Duration.ofSeconds(1));
        amqpRetryOptions.setMaxRetries(5);
        amqpRetryOptions.setMaxDelay(Duration.ofSeconds(15));
        amqpRetryOptions.setMode(AmqpRetryMode.EXPONENTIAL);
        amqpRetryOptions.setTryTimeout(Duration.ofSeconds(5));

        // instantiate a client that will be used to call the service
        serviceBusSenderAsyncClient = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .retryOptions(amqpRetryOptions)
                .sender()
                .topicName(topicName)
                .buildAsyncClient();

        // send message in a loop
        for (int i = 0; i < 100; i++) {
            sendMessage(new ServiceBusMessage(i + ": Hello, World!\n"), i);
        }

        // close the serviceBusSenderAsyncClient
        // serviceBusSenderAsyncClient.close();
    }

    static void sendMessage(ServiceBusMessage serviceBusMessage, int i) throws InterruptedException {
        long startTime = System.currentTimeMillis();

        // send the message to the topic
        serviceBusSenderAsyncClient.sendMessage(serviceBusMessage).subscribe(
                unused -> System.out.println(i + ": Message sent successfully"),
                error -> {
                    System.out.println("error: " + error);
                    System.out.println("error.getMessage(): " + error.getMessage());
                    System.out.println("error.getLocalizedMessage(): " + error.getLocalizedMessage());
                    System.out.println("error.fillInStackTrace(): " + error.fillInStackTrace());
                    System.out.println("error.getCause(): " + error.getCause());
                    long stopTime = System.currentTimeMillis();
                    System.out.println(i + ": Message not sent");
                    System.out.println("Time elapsed: " + (stopTime - startTime) + "ms");
                    ServiceBusException serviceBusException = (ServiceBusException) error;
                    System.out.println("isTransient: " + serviceBusException.isTransient());
                },
                () -> {
                    long stopTime = System.currentTimeMillis();
                    System.out.println(i + ": Message sent successfully");
                    System.out.println("Time elapsed: " + (stopTime - startTime) + "ms");
                }
        );

        // wait for 5 seconds
        TimeUnit.SECONDS.sleep(5);
    }

}
