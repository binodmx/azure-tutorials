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
    static String connectionString = Environment.connectionString1;
    static String topicName = Environment.topicName;

    public static void main(String[] args) throws InterruptedException {
        for (int i = 1; i < 100; i++) {
            sendMessage(i);
            TimeUnit.SECONDS.sleep(10);
        }
    }

    static void sendMessage(int i) {
        // create Retry Options for the Service Bus client
        AmqpRetryOptions amqpRetryOptions = new AmqpRetryOptions();
        amqpRetryOptions.setDelay(Duration.ofSeconds(1));
        amqpRetryOptions.setMaxRetries(120);
        amqpRetryOptions.setMaxDelay(Duration.ofMinutes(5));
        amqpRetryOptions.setMode(AmqpRetryMode.FIXED);
        amqpRetryOptions.setTryTimeout(Duration.ofSeconds(5));

        // create topic if topic does not exist
        ConnectionStringBuilder connectionStringBuilder = new ConnectionStringBuilder(connectionString);
        ManagementClient managementClient = new ManagementClient(connectionStringBuilder);
        try {
            if (!managementClient.topicExists(topicName)) {
                TopicDescription topicDescription = managementClient.createTopic(topicName);
            }
        } catch (ServiceBusException e) {
            System.out.println(e);
        } catch (InterruptedException e) {
            System.out.println(e);
        }

        // create a Service Bus Sender client for the queue
        ServiceBusSenderClient senderClient = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .retryOptions(amqpRetryOptions)
                .sender()
                .topicName(topicName)
                .buildClient();

        // send the message to the topic
        senderClient.sendMessage(new ServiceBusMessage(i + ": Hello, World!\n"));
        System.out.println(i + ": Sent a single message to the topic: " + topicName + "\n");
    }
}
