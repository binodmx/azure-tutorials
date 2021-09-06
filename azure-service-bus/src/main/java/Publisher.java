import com.azure.core.amqp.AmqpRetryMode;
import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;

import com.microsoft.azure.servicebus.management.ManagementClient;
import com.microsoft.azure.servicebus.management.TopicDescription;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class Publisher {
    static String connectionString = Environment.connectionString;
    static String topicName = Environment.topicName;
    static ServiceBusSenderClient senderClient;

    public static void main(String[] args) throws InterruptedException {
        // Create topic if topic does not exist
        ConnectionStringBuilder connectionStringBuilder = new ConnectionStringBuilder(connectionString);
        ManagementClient managementClient = new ManagementClient(connectionStringBuilder);
        try {
            if (!managementClient.topicExists(topicName)) {
                TopicDescription topicDescription = managementClient.createTopic(topicName);
                System.out.println(topicDescription);
            }
        } catch (ServiceBusException e) {
            System.out.println(e);
        } catch (InterruptedException e) {
            System.out.println(e);
        }

        // Create Retry Options for the Service Bus client
        AmqpRetryOptions amqpRetryOptions = new AmqpRetryOptions();
        amqpRetryOptions.setDelay(Duration.ofSeconds(1));
        amqpRetryOptions.setMaxRetries(5);
        amqpRetryOptions.setMaxDelay(Duration.ofSeconds(15));
        amqpRetryOptions.setMode(AmqpRetryMode.EXPONENTIAL);
        amqpRetryOptions.setTryTimeout(Duration.ofSeconds(5));

        // Create a Service Bus sender client for the topic
        senderClient = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .retryOptions(amqpRetryOptions)
                .sender()
                .topicName(topicName)
                .buildClient();

        // Publish message 100 times with 5 seconds interval
        for (int i = 0; i < 100; i++) {
            sendMessage(new ServiceBusMessage("Hello, World! " + i));
            TimeUnit.SECONDS.sleep(5);
        }

        // Close the sender client
        senderClient.close();
    }

    static void sendMessage(ServiceBusMessage serviceBusMessage) {
        try {
            senderClient.sendMessage(serviceBusMessage);
            System.out.println("Message: '" + serviceBusMessage.getBody()
                    + "' sent to Topic: '" + topicName + "'.");
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
