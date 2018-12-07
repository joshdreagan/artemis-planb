package org.apache.activemq.artemis.planb;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.ActiveMQQueueExistsException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.spi.core.protocol.MessagePersister;

public final class PlanBServerHelper {

  public static Message decodeJmsMessage(byte[] encodedData) {
    Message answer;
    MessagePersister persister = MessagePersister.getInstance();
    ActiveMQBuffer buffer = ActiveMQBuffers.wrappedBuffer(encodedData);
    answer = persister.decode(buffer, null /* No idea why this is passed in. It's not used. */);
    return answer;
  }
  
  public static void sendJmsMessages(String brokerUrl, String username, String password, List<Message> jmsMessages) throws Exception {
    try (ServerLocator locator = ActiveMQClient.createServerLocator(brokerUrl)) {
      ClientSessionFactory factory =  locator.createSessionFactory();
      ClientSession session;
      if (username != null && !username.isEmpty()) {
        session = factory.createSession(username, password, false, true, true, true, 0);
      } else {
        session = factory.createSession(false, true, true, true);
      }

      ClientProducer producer = session.createProducer();
      for (Message jmsMessage : jmsMessages) {
        try {
          session.createQueue(jmsMessage.getAddress(), jmsMessage.getRoutingType(), jmsMessage.getAddress(), jmsMessage.isDurable());
        } catch (ActiveMQQueueExistsException e) {
          // log.debug(String.format("Queue '%s' already exists. No biggie...", jmsMessage.getAddress()));
        }
        producer.send(jmsMessage.getAddress(), jmsMessage);
      }
    }
  }
  
  public static Map<String, Object> buildGenericPostResponse(String status, String message) {
    Map<String, Object> answer = new HashMap<>();
    answer.put("status", (status != null && !status.isEmpty())?status:"ok");
    if (message != null && !message.isEmpty()) answer.put("message", message);
    return answer;
  }
  
  public static Map<String, Object> buildGetMessageResponse(String brokerId, String messageId, String messageStatus, String encodedData, Instant messageCreatedTime, Instant messageModifiedTime) {
    Map<String, Object> answer = new HashMap<>();
    answer.put("brokerId", brokerId);
    answer.put("messageId", messageId);
    answer.put("messageStatus", messageStatus);
    answer.put("encodedData", encodedData);
    answer.put("messageCreatedTime", messageCreatedTime);
    answer.put("messageModifiedTime", messageModifiedTime);
    return answer;
  }
}
