import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;

import com.microsoft.azure.servicebus.management.ManagementClient;
import com.microsoft.azure.servicebus.management.TopicDescription;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;

import java.util.concurrent.TimeUnit;

public class Publisher {
    static String connectionString = Environment.connectionString1;
    static String topicName = Environment.topicName;
    static ServiceBusSenderClient senderClient;

    public static void main(String[] args) throws InterruptedException {
        // create a Service Bus Sender client for the topic
        senderClient = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .sender()
                .topicName(topicName)
                .buildClient();

        for (int i = 1; i < 100; i++) {
            sendMessage(new ServiceBusMessage(i + ": Hello, World!"));
            TimeUnit.SECONDS.sleep(5);
        }
    }

    static void sendMessage(ServiceBusMessage serviceBusMessage) {
        // create topic if topic does not exist
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

        // send the message to the topic
        try {
            senderClient.sendMessage(serviceBusMessage);
            System.out.println("Message: '" + serviceBusMessage.getBody() + "' sent to Topic: '" + topicName + "'.\n");
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
