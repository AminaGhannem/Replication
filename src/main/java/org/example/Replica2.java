
package org.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
//import com.example.FileProcessor;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Replica2 {
    private static  Connection connection;
    private static  Channel channel;
    private final static String WRITE_EXCHANGE_NAME = "WEX";
    private final static String READ_REQUEST_EXCHANGE_NAME = "REX";
    private final static String DELIVERY_EXCHANGE_NAME = "DEX";
    private static String REPLICA_NUMBER ;
    private static String FILE_NAME ;
    private static final String FILE_FOLDER_PATH  = System.getProperty("user.dir") +"\\src\\main\\java\\org\\example\\files";
    private static FileProcessor fileProcessor  ;


    public static void main(String[] args) {

        if(args.length!= 1){
            System.out.println("You must enter a replica number");
            System.exit(1) ;
        }
        REPLICA_NUMBER = args[0];
        initialiseFileProcessor() ;
        System.out.println("Replica " + REPLICA_NUMBER + " is UP");

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try  {
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.basicQos(10);
            String queueNameWrite = initialiseWritingProcess(channel);
            String queueNameRead = initialiseReadingProcess(channel);
            initialiseServer(channel , queueNameWrite,queueNameRead);
        }catch(Exception e){
            e.printStackTrace();
        }

    }

    private static void initialiseServer(Channel channel, String queueNameWrite, String queueNameRead) throws Exception {
        while (true) {

            DeliverCallback deliverCallbackWrite = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.println(" [x] Received '" + message + "' from Write Client");
                try {
                    fileProcessor.writeToFile(message);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            };
            channel.basicConsume(queueNameWrite, true, deliverCallbackWrite, consumerTag -> {
            });

            DeliverCallback deliverCallbackRead = (consumerTag, delivery) -> {
                System.out.println(" [x] Read request from Read Client ");
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                if (message.equals("Read Last")) {
                    String lastMessage = fileProcessor.readLastLine();
                    System.out.println(lastMessage);
                    try {
                        sendMessageToReader(lastMessage, channel);
                        System.out.println(" [x] Sent '" + message + "' to Reader Client");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } else if (message.equals("Read All")) {
                    ArrayList<String> allMessages = fileProcessor.readAll();

                    // Count occurrences of each message
                    Map<String, Integer> messageCount = new HashMap<>();
                    for (String msg : allMessages) {
                        if (msg != null) {
                            messageCount.put(msg, messageCount.getOrDefault(msg, 0) + 1);
                        }
                    }

                    // Send messages that appear in more than one replica
                    for (Map.Entry<String, Integer> entry : messageCount.entrySet()) {
                        String msg = entry.getKey();
                        int count = entry.getValue();
                        if (count > 1) {
                            try {
                                sendMessageToReader(msg, channel);
                                System.out.println(" [x] Sent '" + msg + "' to Reader Client");
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                } else {
                    System.out.println("Invalid request");

                }
            };
            channel.basicConsume(queueNameRead, true, deliverCallbackRead, consumerTag -> {
            });
        }
    }

    private static void initialiseFileProcessor() {
        FILE_NAME = "rep" + REPLICA_NUMBER + ".txt";
        fileProcessor = new FileProcessor(FILE_FOLDER_PATH +"/"+FILE_NAME ) ;
    }
    static String  initialiseWritingProcess(Channel channel) throws Exception{
        channel.exchangeDeclare(WRITE_EXCHANGE_NAME, "fanout");
        String queueNameWrite = channel.queueDeclare().getQueue();
        channel.queueBind(queueNameWrite, WRITE_EXCHANGE_NAME, "");
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
        return queueNameWrite ;

    }

    static String  initialiseReadingProcess(Channel channel) throws Exception{
        channel.exchangeDeclare(READ_REQUEST_EXCHANGE_NAME, "fanout");
        String queueNameRead = channel.queueDeclare().getQueue();
        channel.queueBind(queueNameRead, READ_REQUEST_EXCHANGE_NAME, "");
        System.out.println(" [*] Waiting for Requests. To exit press CTRL+C");
        return queueNameRead ;

    }
    private static void sendMessageToReader(String message, Channel channel) throws Exception {
        channel.exchangeDeclare(DELIVERY_EXCHANGE_NAME, "fanout");
        channel.basicPublish(DELIVERY_EXCHANGE_NAME , "", null, message.getBytes(StandardCharsets.UTF_8));
    }
}
