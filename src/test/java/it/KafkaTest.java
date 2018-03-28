package it;


import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import info.batey.kafka.unit.KafkaUnit;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KafkaTest {

  KafkaUnit kafkaUnitServer;

  @Before
  public void setup() throws Exception {
    kafkaUnitServer = new KafkaUnit();
    kafkaUnitServer.startup();
  }

  @After
  public void tearDown() {
    kafkaUnitServer.shutdown();
  }

  @Test
  public void readMessagesFromTopic() throws Exception {
    final String topicName = "MyTestTopic";
    kafkaUnitServer.createTopic(topicName);

    ProducerRecord<String, String> keyedMessage = new ProducerRecord<>(topicName, "greeting",
        "Hello world from hascode.com :)");
    kafkaUnitServer.sendMessages(keyedMessage);

    List<String> allMessages = kafkaUnitServer.readAllMessages(topicName);
    assertThat("topic should contain only one message", allMessages.size(), equalTo(1));
    assertThat("the message should match the published message", allMessages.get(0), equalTo("Hello world from hascode.com :)"));
  }
}
