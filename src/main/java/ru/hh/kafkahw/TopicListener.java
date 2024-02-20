package ru.hh.kafkahw;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.Service;

import java.util.HashSet;
import java.util.Set;

@Component
public class TopicListener {
  private final static Logger LOGGER = LoggerFactory.getLogger(TopicListener.class);
  private final Service service;
  private final Set<Integer> receivedMessages = new HashSet<>();

  @Autowired
  public TopicListener(Service service) {
    this.service = service;
  }

  /* Для семантики at most once допускается потеря сообщений, поэтому достаточно
  проигонрировать исключение в блоке try
  * */
  @KafkaListener(topics = "topic1", groupId = "group1")
  public void atMostOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    //service.handle("topic1", consumerRecord.value());
    ack.acknowledge();
    try {
      service.handle("topic1", consumerRecord.value());
    } catch(RuntimeException ignored) {
    }
  }


  /*
  для реализации семантики atLeastOnce важно отправить сообщение и неважно, сколько раз.
  Добавлены изменения в producer - отправка сообщения до победного
   */
  @KafkaListener(topics = "topic2", groupId = "group2")
  public void atLeastOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    ack.acknowledge();
    service.handle("topic2", consumerRecord.value());
  }

  /*
  Для реализации exactly once было добавлено множество с хэшами обработанных сообщений
   */
  @KafkaListener(topics = "topic3", groupId = "group3")
  public void exactlyOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    int hash = consumerRecord.value().hashCode();
    if (!receivedMessages.contains(hash)) {
      service.handle("topic3", consumerRecord.value());
      ack.acknowledge();
      receivedMessages.add(hash);
    }
  }
}
