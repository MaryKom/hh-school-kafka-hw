package ru.hh.kafkahw.internal;

import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {
  private final static Logger LOGGER = LoggerFactory.getLogger(KafkaProducer.class);
  private final Random random = new Random();

  private final KafkaTemplate<String, String> kafkaTemplate;

  public KafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  /*
  Для семантик AtLeastOnce и ExactlyOnce проблема была в том, что исключения
  иогли прервать отправку сообщения. Теперь мы повторяем отправку сообщения
  до того момента, пока оно не будет отправлено
  */
  public void send(String topic, String payload) {
    boolean isSent = false;
    while (!isSent) {
      isSent = trySend(topic, payload);
    }
  }

  /*
    Добавила метод, которые пытается отправить сообщение. Исключение, возникющее до отправки
    сообщения, мешало реализации семантик.
    */
  public boolean trySend(String topic, String payload) {
    boolean isSent = false;
    try {
      if (random.nextInt(100) < 10) {
        throw new RuntimeException();
      }
      LOGGER.info("send to kafka, topic {}, payload {}", topic, payload);
      kafkaTemplate.send(topic, payload);
      isSent = true;
      if (random.nextInt(100) < 2) {
        throw new RuntimeException();
      }
    } catch (Exception e) {
    }
    return isSent;
  }
}
