import com.azure.core.amqp.AmqpRetryMode;
import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.messaging.servicebus.*;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Subscriber {
    static String connectionString = Environment.connectionString1;
    static String topicName = Environment.topicName;
    static String subName = Environment.subscriptionName;

    public static void main(String[] args) throws InterruptedException {
        receiveMessages();
    }

    // handles received messages
    static void receiveMessages() throws InterruptedException {
        // create Retry Options for the Service Bus client
        AmqpRetryOptions amqpRetryOptions = new AmqpRetryOptions();
        amqpRetryOptions.setDelay(Duration.ofSeconds(1));
        amqpRetryOptions.setMaxRetries(120);
        amqpRetryOptions.setMaxDelay(Duration.ofMinutes(5));
        amqpRetryOptions.setMode(AmqpRetryMode.FIXED);
        amqpRetryOptions.setTryTimeout(Duration.ofSeconds(5));

        CountDownLatch countdownLatch = new CountDownLatch(1);

        // create an instance of the processor through the ServiceBusClientBuilder
        ServiceBusProcessorClient processorClient = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .retryOptions(amqpRetryOptions)
                .processor()
                .topicName(topicName)
                .subscriptionName(subName)
                .processMessage(Subscriber::processMessage)
                .processError(context -> processError(context, countdownLatch))
                .buildProcessorClient();

        System.out.println("Starting the processor");
        try {
            processorClient.start();
        } catch (Exception e) {
            System.out.println(e);
        }

//        TimeUnit.SECONDS.sleep(10);
//        System.out.println("Stopping and closing the processor");
//        processorClient.close();
    }

    private static void processMessage(ServiceBusReceivedMessageContext context) {
        ServiceBusReceivedMessage message = context.getMessage();
        System.out.printf("Processing message. Session: %s, Sequence #: %s. Contents: %s%n", message.getMessageId(),
                message.getSequenceNumber(), message.getBody());
    }

    private static void processError(ServiceBusErrorContext context, CountDownLatch countdownLatch) {
        System.out.printf("Error when receiving messages from namespace: '%s'. Entity: '%s'%n",
                context.getFullyQualifiedNamespace(), context.getEntityPath());

        if (!(context.getException() instanceof ServiceBusException)) {
            System.out.printf("Non-ServiceBusException occurred: %s%n", context.getException());
            return;
        }

        ServiceBusException exception = (ServiceBusException) context.getException();
        ServiceBusFailureReason reason = exception.getReason();

        if (reason == ServiceBusFailureReason.MESSAGING_ENTITY_DISABLED
                || reason == ServiceBusFailureReason.MESSAGING_ENTITY_NOT_FOUND
                || reason == ServiceBusFailureReason.UNAUTHORIZED) {
            System.out.printf("An unrecoverable error occurred. Stopping processing with reason %s: %s%n",
                    reason, exception.getMessage());

            countdownLatch.countDown();
        } else if (reason == ServiceBusFailureReason.MESSAGE_LOCK_LOST) {
            System.out.printf("Message lock lost for message: %s%n", context.getException());
        } else if (reason == ServiceBusFailureReason.SERVICE_BUSY) {
            try {
                // Choosing an arbitrary amount of time to wait until trying again.
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                System.err.println("Unable to sleep for period of time");
            }
        } else {
            System.out.printf("Error source %s, reason %s, message: %s%n", context.getErrorSource(),
                    reason, context.getException());
        }
    }
}
