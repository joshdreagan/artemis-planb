package org.apache.activemq.artemis.planb;

import java.util.Map;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.apache.camel.processor.validation.PredicateValidationException;
import org.apache.camel.util.toolbox.AggregationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

@Component
public class CamelConfiguration extends RouteBuilder {

  private static final Logger log = LoggerFactory.getLogger(CamelConfiguration.class);
  
  @Override
  public void configure() throws Exception {
    
    rest("/messages")
      .post()
        .consumes(MediaType.APPLICATION_JSON_VALUE)
        .produces(MediaType.APPLICATION_JSON_VALUE)
        .to("direct:handleMessagesPOST")
      .get()
        .produces(MediaType.APPLICATION_JSON_VALUE)
        .to("direct:handleMessagesGET")
    ;
    
    rest("/commands")
      .post()
        .consumes(MediaType.APPLICATION_JSON_VALUE)
        .produces(MediaType.APPLICATION_JSON_VALUE)
        .to("direct:handleCommandsPOST")
    ;
    
    from("direct:handleMessagesPOST")
      .onException(PredicateValidationException.class)
        .handled(true)
        .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(400))
        .bean(PlanBServerHelper.class, "buildGenericPostResponse('error', ${exception.message})")
        .marshal().json(JsonLibrary.Jackson)
      .end()
      .onException(DuplicateKeyException.class)
        .handled(true)
        .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(200))
        .bean(PlanBServerHelper.class, "buildGenericPostResponse('ok', 'Message [${header.MessageID}] already accepted for broker [${header.BrokerID}]. Discarding request...')")
        .marshal().json(JsonLibrary.Jackson)
      .end()
      .transacted()
      .convertBodyTo(String.class)
      .unmarshal().json(JsonLibrary.Jackson, Map.class)
      .validate().simple("${body[brokerId]} != ${null} && ${body[brokerId].isEmpty()} != true").end()
      .validate().simple("${body[messageId]} != ${null} && ${body[messageId].isEmpty()} != true").end()
      .validate().simple("${body[messageStatus]} != ${null} && ${body[messageStatus].isEmpty()} != true").end()
      .validate().simple("${body[messageStatus]} in 'PRODUCED_IN_DOUBT,PRODUCED_CONFIRMED,CONSUMED_IN_DOUBT,CONSUMED_CONFIRMED'").end()
      .filter(simple("${body[messageStatus]} == 'PRODUCED_IN_DOUBT'"))
        .validate().simple("${body[encodedData]} != ${null} && ${body[encodedData].isEmpty()} != true").end()
      .end()
      .setHeader("BrokerID", simple("${body[brokerId]}"))
      .setHeader("MessageID", simple("${body[messageId]}"))
      .setHeader("MessageStatus", simple("${body[messageStatus]}"))
      .choice() 
        .when(simple("${header.MessageStatus} == 'PRODUCED_IN_DOUBT'"))
          .setBody(simple("${body[encodedData]}"))
          .unmarshal().base64()
          .convertBodyTo(byte[].class)
          .to("sql:insert into PLANB_MESSAGE (BROKER_ID, MESSAGE_ID, MESSAGE_STATUS, ENCODED_DATA) values (:#${header.BrokerID}, :#${header.MessageID}, :#${header.MessageStatus}, :#${body})?dataSource=#dataSource")
        .when(simple("${header.MessageStatus} == 'PRODUCED_CONFIRMED'"))
          .to("sql:update PLANB_MESSAGE set MESSAGE_STATUS=:#${header.MessageStatus}, MTIME=CURRENT_TIMESTAMP where (BROKER_ID=:#${header.BrokerID} and MESSAGE_ID=:#${header.MessageID} and MESSAGE_STATUS='PRODUCED_IN_DOUBT')?dataSource=#dataSource")
        .when(simple("${header.MessageStatus} == 'CONSUMED_IN_DOUBT'"))
          .to("sql:update PLANB_MESSAGE set MESSAGE_STATUS=:#${header.MessageStatus}, MTIME=CURRENT_TIMESTAMP where (BROKER_ID=:#${header.BrokerID} and MESSAGE_ID=:#${header.MessageID} and (MESSAGE_STATUS in ('PRODUCED_IN_DOUBT', 'PRODUCED_CONFIRMED')))?dataSource=#dataSource")
        .when(simple("${header.MessageStatus} == 'CONSUMED_CONFIRMED'"))
          .to("sql:delete PLANB_MESSAGE where (BROKER_ID=:#${header.BrokerID} and MESSAGE_ID=:#${header.MessageID})?dataSource=#dataSource")
      .end()
      .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(200))
      .bean(PlanBServerHelper.class, "buildGenericPostResponse('ok', ${null})")
      .marshal().json(JsonLibrary.Jackson)
    ;
    
    from("direct:handleMessagesGET")
      .to("sql:select * from PLANB_MESSAGE?dataSource=#dataSource")
      .split(body(), AggregationStrategies.groupedBody())
        .setHeader("BrokerID", simple("${body[BROKER_ID]}"))
        .setHeader("MessageID", simple("${body[MESSAGE_ID]}"))
        .setHeader("MessageStatus", simple("${body[MESSAGE_STATUS]}"))
        .setHeader("MessageCreatedTime", simple("${body[CTIME]}"))
        .setHeader("MessageModifiedTime", simple("${body[MTIME]}"))
        .setBody(simple("${body[ENCODED_DATA]}"))
        .marshal().base64()
        .bean(PlanBServerHelper.class, "buildGetMessageResponse(${header.BrokerID}, ${header.MessageID}, ${header.MessageStatus}, ${body}, ${header.MessageCreatedTime}, ${header.MessageModifiedTime})")
      .end()
      .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(200))
      .marshal().json(JsonLibrary.Jackson)
    ;
    
    from("direct:handleCommandsPOST")
      .onException(PredicateValidationException.class)
        .handled(true)
        .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(400))
        .bean(PlanBServerHelper.class, "buildGenericPostResponse('error', ${exception.message})")
        .marshal().json(JsonLibrary.Jackson)
      .end()
      .convertBodyTo(String.class)
      .unmarshal().json(JsonLibrary.Jackson, Map.class)
      .validate().simple("${body[command]} != ${null} && ${body[command].isEmpty()} != true").end()
      .validate().simple("${body[command]} in 'DRAIN'").end()
      .choice()
        .when().simple("${body[command]} == 'DRAIN'")
          .validate().simple("${body[destinationUrl]} != ${null} && ${body[destinationUrl].isEmpty()} != true").end()
          .setHeader("DestinationURL", simple("${body[destinationUrl]}"))
          .setHeader("DestinationUsername", simple("${body[destinationUsername]}"))
          .setHeader("DestinationPassword", simple("${body[destinationPassword]}"))
          .setBody(constant(null))
          .to("seda:drainMessages")
          .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(202))
          .bean(PlanBServerHelper.class, "buildGenericPostResponse('ok', ${null})")
          .marshal().json(JsonLibrary.Jackson)
      .end()
    ;
    
    from("seda:drainMessages")
      .to("direct:drainMessages")
    ;
    
    from("direct:drainMessages")
      .transacted()
      .to("sql:select * from PLANB_MESSAGE?dataSource=#dataSource")
      .split(body(), AggregationStrategies.groupedBody())
        .setHeader("BrokerID", simple("${body[BROKER_ID]}"))
        .setHeader("MessageID", simple("${body[MESSAGE_ID]}"))
        .setBody(simple("${body[ENCODED_DATA]}"))
        .bean(PlanBServerHelper.class, "decodeJmsMessage(${body})")
      .end()
      .bean(PlanBServerHelper.class, "sendJmsMessages(${header.DestinationURL}, ${header.DestinationUsername}, ${header.DestinationPassword}, ${body})")
      .to("sql:delete PLANB_MESSAGE?dataSource=#dataSource")
    ;
  }
}
