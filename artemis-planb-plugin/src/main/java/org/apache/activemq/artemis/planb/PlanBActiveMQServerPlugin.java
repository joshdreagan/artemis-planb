package org.apache.activemq.artemis.planb;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.postoffice.RoutingStatus;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerPlugin;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.camel.CamelContext;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.dataformat.JsonLibrary;

public class PlanBActiveMQServerPlugin implements ActiveMQServerPlugin {

  private String planBServerUrl;
  private String brokerId;
  
  private CamelContext camelContext;
  private ProducerTemplate producer;
  
  @Override
  public void init(Map<String, String> properties) {
    planBServerUrl = properties.get("PLANB_SERVER_URL");
    Objects.requireNonNull(planBServerUrl, "Property [PLANB_SERVER_URL] cannot be null.");
    try {
      brokerId = properties.getOrDefault("BROKER_ID", InetAddress.getLocalHost().getHostName());
    } catch (UnknownHostException e) {}
    Objects.requireNonNull(brokerId, "Property [BROKER_ID] cannot be null.");
    
    camelContext = new DefaultCamelContext();
    try {
      camelContext.addRoutes(new RouteBuilder() {
        @Override
        public void configure() throws Exception {
          
          from("direct:producedInDoubt")
            .bean(PlanBActiveMQServerPluginHelper.class, "encodeJmsMessage(${body})")
            .marshal().base64()
            .bean(PlanBActiveMQServerPluginHelper.class, "generatePlanBMessage(${header.BrokerID}, ${header.PlanBMessageID}, 'PRODUCED_IN_DOUBT', ${body})")
            .to("direct:toPlanBServer")
          ;
          
          from("direct:producedConfirmed")
            .bean(PlanBActiveMQServerPluginHelper.class, "generatePlanBMessage(${header.BrokerID}, ${header.PlanBMessageID}, 'PRODUCED_CONFIRMED', ${null})")
            .to("direct:toPlanBServer")
          ;
          
          from("direct:consumedInDoubt")
            .bean(PlanBActiveMQServerPluginHelper.class, "generatePlanBMessage(${header.BrokerID}, ${header.PlanBMessageID}, 'CONSUMED_IN_DOUBT', ${null})")
            .to("direct:toPlanBServer")
          ;
          
          from("direct:consumedConfirmed")
            .bean(PlanBActiveMQServerPluginHelper.class, "generatePlanBMessage(${header.BrokerID}, ${header.PlanBMessageID}, 'CONSUMED_CONFIRMED', ${null})")
            .to("direct:toPlanBServer")
          ;
          
          from("direct:toPlanBServer")
            .setHeader(Exchange.HTTP_METHOD).constant("POST")
            .setHeader(Exchange.CONTENT_TYPE).constant("application/json")
            .marshal().json(JsonLibrary.Jackson, false)
            .toF("netty4-http:%s", planBServerUrl)
          ;
        }
      });
    } catch (Exception e) {
      throw new RuntimeCamelException(e);
    }
    producer = camelContext.createProducerTemplate();
  }

  @Override
  public void registered(ActiveMQServer server) {
    try {
      camelContext.start();
    } catch (Exception e) {
      throw new RuntimeCamelException(e);
    }
  }

  @Override
  public void unregistered(ActiveMQServer server) {
    try {
      camelContext.stop();
    } catch (Exception e) {
      throw new RuntimeCamelException(e);
    }
  }

  @Override
  public void beforeSend(ServerSession session, Transaction tx, Message message, boolean direct, boolean noAutoCreateQueue) throws ActiveMQException {
    if (message.getRoutingType() == RoutingType.MULTICAST) {
      return;
    }
    try {
      String planBMessageId = UUID.randomUUID().toString();

      message.putStringProperty("PlanBMessageID", planBMessageId);
      message.reencode();

      Map<String, Object> headers = new HashMap<>();
      headers.put("PlanBMessageID", planBMessageId);
      headers.put("BrokerID", brokerId);
      producer.sendBodyAndHeaders("direct:producedInDoubt", message, headers);
    } catch (CamelExecutionException e) {
      throw new ActiveMQException(ActiveMQExceptionType.INTERCEPTOR_REJECTED_PACKET, "Error sending 'PRODUCED_IN_DOUBT' message to planb server.", e);
    }
  }

  @Override
  public void afterSend(ServerSession session, Transaction tx, Message message, boolean direct, boolean noAutoCreateQueue, RoutingStatus result) throws ActiveMQException {
    if (message.getRoutingType() == RoutingType.MULTICAST) {
      return;
    }
    try {
      String planBMessageId = message.getStringProperty("PlanBMessageID");
      
      Map<String, Object> headers = new HashMap<>();
      headers.put("PlanBMessageID", planBMessageId);
      headers.put("BrokerID", brokerId);
      producer.sendBodyAndHeaders("direct:producedConfirmed", message, headers);
    } catch (CamelExecutionException e) {
      // TODO: Should just log a warning here instead.
      throw new ActiveMQException(ActiveMQExceptionType.INTERCEPTOR_REJECTED_PACKET, "Error sending 'PRODUCED_CONFIRMED' message to planb server.", e);
    }
  }

  @Override
  public void beforeDeliver(ServerConsumer consumer, MessageReference reference) throws ActiveMQException {
    if (reference.getMessage().getRoutingType() == RoutingType.MULTICAST) {
      return;
    }
    try {
      String planBMessageId = reference.getMessage().getStringProperty("PlanBMessageID");
      
      Map<String, Object> headers = new HashMap<>();
      headers.put("PlanBMessageID", planBMessageId);
      headers.put("BrokerID", brokerId);
      producer.sendBodyAndHeaders("direct:consumedInDoubt", reference.getMessage(), headers);
    } catch (CamelExecutionException e) {
      // TODO: Should just log a warning here instead.
      throw new ActiveMQException(ActiveMQExceptionType.INTERCEPTOR_REJECTED_PACKET, "Error sending 'CONSUMED_IN_DOUBT' message to planb server.", e);
    }
  }

  @Override
  public void afterDeliver(ServerConsumer consumer, MessageReference reference) throws ActiveMQException {
    if (reference.getMessage().getRoutingType() == RoutingType.MULTICAST) {
      return;
    }
    try {
      String planBMessageId = reference.getMessage().getStringProperty("PlanBMessageID");
      
      Map<String, Object> headers = new HashMap<>();
      headers.put("PlanBMessageID", planBMessageId);
      headers.put("BrokerID", brokerId);
      producer.sendBodyAndHeaders("direct:consumedConfirmed", reference.getMessage(), headers);
    } catch (CamelExecutionException e) {
      // TODO: Should just log a warning here instead.
      throw new ActiveMQException(ActiveMQExceptionType.INTERCEPTOR_REJECTED_PACKET, "Error sending 'CONSUMED_CONFIRMED' message to planb server.", e);
    }
  }
}
