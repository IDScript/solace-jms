/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Solace JMS 1.1 Examples: QueueConsumer
 */

package com.solace.samples;

import com.solacesystems.jms.SolConnectionFactory;
import com.solacesystems.jms.SolJmsUtility;
import com.solacesystems.jms.SupportedProperty;

import javax.jms.*;
import java.util.concurrent.CountDownLatch;

/**
 * Receives a persistent message from a queue using Solace JMS API implementation.
 * <p>
 * The queue used for messages is created on the message broker.
 */
public class QueueConsumer {
    // Latch used for synchronizing between threads
    final CountDownLatch latch = new CountDownLatch(1);

    public static void main(String... args) throws Exception {
        if (args.length != 4 || args[1].split("@").length != 2) {
            System.out.println("Usage: QueueConsumer <host:port> <client-username@message-vpn> <client-password> <queueName>");
            System.out.println();
            System.exit(-1);
        }
        if (args[1].split("@")[0].isEmpty()) {
            System.out.println("No client-username entered");
            System.out.println();
            System.exit(-1);
        }
        if (args[1].split("@")[1].isEmpty()) {
            System.out.println("No message-vpn entered");
            System.out.println();
            System.exit(-1);
        }
        new QueueConsumer().run(args);
    }

    public void run(String... args) throws Exception {

        String[] split = args[1].split("@");

        String host = args[0];
        String vpnName = split[1];
        String username = split[0];
        String password = args[2];
        String queueName = args[3];

        System.out.printf("QueueConsumer is connecting to Solace messaging at %s...%n", host);

        // Programmatically create the connection factory using default settings
        SolConnectionFactory connectionFactory = SolJmsUtility.createConnectionFactory();
        connectionFactory.setHost(host);
        connectionFactory.setVPN(vpnName);
        connectionFactory.setUsername(username);
        connectionFactory.setPassword(password);

        // Enables persistent queues or topic endpoints to be created dynamically
        // on the router, used when Session.createQueue() is called below
        connectionFactory.setDynamicDurables(true);

        // Create connection to the Solace router
        Connection connection = connectionFactory.createConnection();

        // Create a non-transacted, client ACK session.
        Session session = connection.createSession(false, SupportedProperty.SOL_CLIENT_ACKNOWLEDGE);

        System.out.printf("Connected to the Solace Message VPN '%s' with client username '%s'.%n", vpnName,
                username);

        // Create the queue programmatically and the corresponding router resource
        // will also be created dynamically because DynamicDurables is enabled.
        Queue queue = session.createQueue(queueName);

        // From the session, create a consumer for the destination.
        MessageConsumer messageConsumer = session.createConsumer(queue);

        // Use the anonymous inner class for receiving messages asynchronously
        messageConsumer.setMessageListener(message -> {
            try {
                if (message instanceof TextMessage) {
                    System.out.printf("TextMessage received: '%s'%n", ((TextMessage) message).getText());
                } else {
                    System.out.println("Message received.");
                }
                System.out.printf("Message Content:%n%s%n", SolJmsUtility.dumpMessage(message));

                // ACK the received message manually because of the set SupportedProperty.SOL_CLIENT_ACKNOWLEDGE above
                message.acknowledge();

                latch.countDown(); // unblock the main thread
            } catch (JMSException ex) {
                System.out.println("Error processing incoming message.");
                ex.printStackTrace();
            }
        });

        // Start receiving messages
        connection.start();
        System.out.println("Awaiting message...");
        // the main thread blocks at the next statement until a message received
        latch.await();

        connection.stop();
        // Close everything in the order reversed from the opening order
        // NOTE: as the interfaces below extend AutoCloseable,
        // with them, it's possible to use the "try-with-resources" Java statement
        // see details at https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html
        messageConsumer.close();
        session.close();
        connection.close();
    }
}
