package ru.hh.kafkahw.internal;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

@Component
public class Service {

  private final ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>> counters = new ConcurrentHashMap<>();
  private final Random random = new Random();


  public void handle(String topic, String message) {
    boolean isHandle = false;
    while (!isHandle) {
      isHandle = tryHandle(topic, message);
    }
  }
  /*
  Здесь идея как и в продюсере, используем флаг для обработки сообщения
   */
  public boolean tryHandle(String topic, String message) {
    boolean isHandle = false;
    try {
      if (random.nextInt(100) < 10) {
        throw new RuntimeException();
      }
      counters.computeIfAbsent(topic, key -> new ConcurrentHashMap<>())
              .computeIfAbsent(message, key -> new AtomicInteger(0)).incrementAndGet();
      isHandle = true;
      if (random.nextInt(100) < 2) {
        throw new RuntimeException();
      }
    } catch (Exception e) {

    }
    return isHandle;
  }

  public int count(String topic, String message) {
    return counters.getOrDefault(topic, new ConcurrentHashMap<>()).getOrDefault(message, new AtomicInteger(0)).get();
  }
}
