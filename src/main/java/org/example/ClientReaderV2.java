package org.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.util.Scanner;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class ClientReaderV2 {
    private static Connection connection;
    private static Channel channel;
    private final static String READ_REQUEST_EXCHANGE_NAME = "REX";
    private final static String DELIVERY_EXCHANGE_NAME = "DEX";
    private final static int NUM_REPLICAS = 3;


    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try  {
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.basicQos(10);
            initialiseRequestProcess(channel);
            String queueDelivery= initialiseDeliveryProcess(channel);
            initialiseServer(channel , queueDelivery);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    private static String initialiseDeliveryProcess(Channel channel) throws Exception{
        channel.exchangeDeclare(DELIVERY_EXCHANGE_NAME, "fanout");
        String queueDelivery = channel.queueDeclare().getQueue();
        channel.queueBind(queueDelivery, DELIVERY_EXCHANGE_NAME, "");
        return queueDelivery ;
    }

    private static void initialiseRequestProcess(Channel channel) throws Exception{
        channel.exchangeDeclare(READ_REQUEST_EXCHANGE_NAME, "fanout");
    }

    private static void initialiseServer(Channel channel, String queueDelivery) throws Exception {
        Scanner scanner = new Scanner(System.in);

        String message;
        System.out.println("To quit type 'quit'");
        System.out.println("To send a request type : 'Read All'");
        System.out.println("Enter a choice: ");

        Map<String, Integer> allMessages = new HashMap<>();

        while (true) {
            message = scanner.nextLine();

            if (message.equals("quit")) {
                System.out.println("Quitting");
                break;
            }

            if (message.equals("Read All")) {
                allMessages.clear();
                channel.basicPublish(READ_REQUEST_EXCHANGE_NAME, "", null, message.getBytes("UTF-8"));
                System.out.println(" Reader Sent '" + message + "' request");

                System.out.println(" [*] Waiting for messages.");
                CountDownLatch latch = new CountDownLatch(NUM_REPLICAS);

                DeliverCallback deliverCallbackWrite = (consumerTag, delivery) -> {
                    String messageReceived = new String(delivery.getBody(), "UTF-8");
                    if(messageReceived.equals("Read All finished")){
                        latch.countDown();
                    }
                    else{
                        String line = messageReceived.substring(2);
                        allMessages.put(line,allMessages.getOrDefault(line,0)+1);
                    }

                };
                channel.basicConsume(queueDelivery, true, deliverCallbackWrite, consumerTag -> {
                });
                latch.await();
                System.out.println("Majority lines from all replicas:");
                for (Map.Entry<String, Integer> entry : allMessages.entrySet()) {
                    if (entry.getValue() > 1) {
                        System.out.println(entry.getKey());
                    }
                }
                System.out.println("Enter a choice:");
            }
        }
        System.exit(0);
    }
}

