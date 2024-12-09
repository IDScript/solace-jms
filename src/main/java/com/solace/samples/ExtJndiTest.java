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
 *  Solace JMS 1.1 Examples: ExtJndiTest
 */

package com.solace.samples;

import com.solacesystems.jms.SolConnectionFactory;
import com.solacesystems.jms.SupportedProperty;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.Hashtable;
import java.util.concurrent.CountDownLatch;

/**
 * Sends a persistent message to a queue using external JNDI lookup, then reads it back.
 * <p>
 * The queue used for messages must exist on the message broker.
 */
public class ExtJndiTest {

    // JNDI Initial Context Factory
    private static final String JNDI_INITIAL_CONTEXT_FACTORY =
            // "com.ibm.websphere.naming.WsnInitialContextFactory";
            // "com.sun.jndi.ldap.LdapCtxFactory";
            "com.sun.jndi.fscontext.RefFSContextFactory";
    // "com.sun.jndi.rmi.registry.RegistryContextFactory";
    // Latch used for synchronizing between threads
    final CountDownLatch latch = new CountDownLatch(1);
    // The URL to the JNDI server
    private String jndiUrl;
    // Username used to log into the JNDI server
    private String jndiUsername;
    // Password used to log into the JNDI server
    private String jndiPassword;
    // Connection Factory Distinguished Name - Must exist in the JNDI
    private String cfName;
    // Destination Distinguished Name  - Must exist in the JNDI
    private String destinationName;

    public static void main(String[] args) {
        try {
            ExtJndiTest instance = new ExtJndiTest();
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "-jndiUrl" -> {
                        i++;
                        if (i >= args.length) instance.printUsage();
                        instance.jndiUrl = args[i];
                    }
                    case "-jndiUsername" -> {
                        i++;
                        if (i >= args.length) instance.printUsage();
                        instance.jndiUsername = args[i];
                    }
                    case "-jndiPassword" -> {
                        i++;
                        if (i >= args.length) instance.printUsage();
                        instance.jndiPassword = args[i];
                    }
                    case "-cf" -> {
                        i++;
                        if (i >= args.length) instance.printUsage();
                        instance.cfName = args[i];
                    }
                    case "-destination" -> {
                        i++;
                        if (i >= args.length) instance.printUsage();
                        instance.destinationName = args[i];
                    }
                    default -> {
                        instance.printUsage();
                        System.out.println("Illegal argument specified - " + args[i]);
                        return;
                    }
                }
            }

            if (instance.jndiUrl == null) {
                instance.printUsage();
                System.out.println("Please specify \"-jndiUrl\" parameter");
                return;
            }
            if (instance.jndiUsername == null) {
                instance.printUsage();
                System.out.println("Please specify \"-jndiUsername\" parameter");
                return;
            }
            if (instance.jndiPassword == null) {
                instance.printUsage();
                System.out.println("Please specify \"-jndiPassword\" parameter");
                return;
            }
            if (instance.cfName == null) {
                instance.printUsage();
                System.out.println("Please specify \"-cf\" parameter");
                return;
            }
            if (instance.destinationName == null) {
                instance.printUsage();
                System.out.println("Please specify \"-destination\" parameter");
                return;
            }

            instance.run();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }

    private void printUsage() {
        System.out.println("""
                
                Usage:\s
                SExtJndiTest\
                 -jndiUrl URL -jndiUsername USERNAME -jndiPassword PASSWORD -cf CONNECTION_FACTORY_DN -destination DESTINATION_DN
                """);
    }

    private void run() {

        // Initial Context
        InitialContext ctx = null;

        // JMS Connection
        Connection connection = null;

        try {
            // Create the LDAP Initial Context
            Hashtable<String, String> env = new Hashtable<>();
            env.put(Context.INITIAL_CONTEXT_FACTORY, JNDI_INITIAL_CONTEXT_FACTORY);
            env.put(Context.PROVIDER_URL, jndiUrl);
            env.put(Context.REFERRAL, "throw");
            env.put(Context.SECURITY_PRINCIPAL, jndiUsername);
            env.put(Context.SECURITY_CREDENTIALS, jndiPassword);
            ctx = new InitialContext(env);

            // lookup the connection factory
            SolConnectionFactory cf = (SolConnectionFactory) ctx.lookup(cfName);

            // lookup the destination
            Object destination = ctx.lookup(destinationName);

            // Create a JMS Connection instance .
            connection = cf.createConnection();

            // Create a non-transacted, client ACK session.
            Session session = connection.createSession(false, SupportedProperty.SOL_CLIENT_ACKNOWLEDGE);

            // Create the producer and consumer
            MessageConsumer consumer = null;
            MessageProducer producer = null;
            if (destination instanceof Topic topic) {
                consumer = session.createConsumer(topic);
                producer = session.createProducer(topic);
            } else if (destination instanceof Queue queue) {
                consumer = session.createConsumer(queue);
                producer = session.createProducer(queue);
            } else {
                System.out.println("Destination in JNDI must be a topic or a queue");
                System.exit(0);
            }

            // set the consumer's message listener
            consumer.setMessageListener(message -> {
                if (message instanceof TextMessage) {
                    try {
                        System.out.println("*** Received Message with content: " + ((TextMessage) message).getText());
                        // ACK the received message because of the set SupportedProperty.SOL_CLIENT_ACKNOWLEDGE above
                        message.acknowledge();
                        latch.countDown(); // unblock the main thread
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                } else {
                    System.out.println("Received Message: " + message);
                }
            });

            // Start the JMS Connection.
            connection.start();

            // Send a message to the consumer
            TextMessage testMessage = session.createTextMessage("SolJMSJNDITest message");
            producer.send(testMessage);
            System.out.println("*** Sent Message with content: " + testMessage.getText());

            // Block main thread and wait for the message to be received and printed out before exiting
            latch.await();
            connection.stop();
            consumer.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception ignored) {
                }
            }
            if (ctx != null) {
                try {
                    ctx.close();
                } catch (Exception ignored) {
                }
            }
        }
    }
}
