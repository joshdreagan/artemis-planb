package org.apache.activemq.artemis.planb;

import java.util.HashMap;
import java.util.Map;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.Message;

public final class PlanBActiveMQServerPluginHelper {

  public static byte[] encodeJmsMessage(Message jmsMessage) {
    byte[] answer;
    ActiveMQBuffer buffer = ActiveMQBuffers.fixedBuffer(jmsMessage.getPersister().getEncodeSize(jmsMessage));
    jmsMessage.getPersister().encode(buffer, jmsMessage);
    answer = buffer.byteBuf().array();
    return answer;
  }
  
  public static Map<String, Object> generatePlanBMessage(String brokerId, String messageId, String messageStatus, String encodedData) {
    Map<String, Object> answer = new HashMap<>();
    answer.put("brokerId", brokerId);
    answer.put("messageId", messageId);
    answer.put("messageStatus", messageStatus);
    if (encodedData != null && !encodedData.isEmpty()) answer.put("encodedData", encodedData);
    return answer;
  }
}
