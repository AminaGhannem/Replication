
package org.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class ClientReader {
    private static Connection connection;
    private static Channel channel;
    private final static String READ_REQUEST_EXCHANGE_NAME = "REX";
    private final static String DELIVERY_EXCHANGE_NAME = "DEX";


    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try  {
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.basicQos(10);
            initialiseRequestProcess(channel);
            String queueNameDelivery= initialiseDeliveryProcess(channel);
            initialiseServer(channel , queueNameDelivery);

        }catch(Exception e){
            e.printStackTrace();
        }
    }

    private static String initialiseDeliveryProcess(Channel channel) throws Exception{
        channel.exchangeDeclare(DELIVERY_EXCHANGE_NAME, "fanout");
        String queueNameDelivery = channel.queueDeclare().getQueue();
        channel.queueBind(queueNameDelivery, DELIVERY_EXCHANGE_NAME, "");
        return queueNameDelivery ;
    }

    private static void initialiseRequestProcess(Channel channel) throws Exception{
        channel.exchangeDeclare(READ_REQUEST_EXCHANGE_NAME, "fanout");
    }
    private static void initialiseServer(Channel channel , String queueNameDelivery ) throws Exception{
        Scanner scanner = new Scanner(System.in);

        String message = "";
        System.out.println("To quit type 'quit'");
        System.out.print("To send a request type :  'Read Last' or 'Read All' \n");
        System.out.print("Enter a choice: \n");

        MutableBoolean messageProcessed = new MutableBoolean(false);

        while (true) {

            message = scanner.nextLine();

            if (message.equals("quit")) {
                break;
            }

            if (message.equals("Read Last")) {
                messageProcessed.value = false;
                channel.basicPublish(READ_REQUEST_EXCHANGE_NAME, "", null, message.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [x] Sent '" + message + "' request");

                System.out.println(" [*] Waiting for message.");

                DeliverCallback deliverCallbackWrite = (consumerTag, delivery) -> {
                    if (!messageProcessed.value) {
                        String messageReceived = new String(delivery.getBody(), StandardCharsets.UTF_8);
                        System.out.println(" [x] Received '" + messageReceived + "' from Write Client");

                        System.out.print("Enter a choice: \n");
                        messageProcessed.value = true ;
                    }
                };
                channel.basicConsume(queueNameDelivery, true, deliverCallbackWrite, consumerTag -> {
                });
            }}
    }
}
class MutableBoolean {
    boolean value;

    MutableBoolean(boolean value) {
        this.value = value;
    }
}
