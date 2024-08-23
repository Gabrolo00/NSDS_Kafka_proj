package it.polimi.middleware.kafka.Backend.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import it.polimi.middleware.kafka.Backend.Services.UserService;
import it.polimi.middleware.kafka.Backend.Services.CourseService;
import it.polimi.middleware.kafka.Backend.Services.ProjectService;
import it.polimi.middleware.kafka.Backend.Course;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class EnrollConsumer extends Thread {
    private KafkaConsumer<String, String> consumer;
    private String topic = "enrollment-events";
    private UserService userService;
    private CourseService courseService;
    private ProjectService projectService;
    private ConsumerRecords<String, String> records;
    private boolean autoCommit = false;
    private Map<TopicPartition, OffsetAndMetadata> offsets;
    private boolean running = true;

    private static Map<TopicPartition, OffsetAndMetadata> consumerOffsetsToMap(KafkaConsumer<String, String> consumer,
            ConsumerRecords<String, String> records) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (TopicPartition partition : records.partitions()) {
            long offset = records.records(partition).get(records.records(partition).size() - 1).offset() + 1;
            offsets.put(partition, new OffsetAndMetadata(offset));
        }
        return offsets;
    }

    public EnrollConsumer(UserService userService, CourseService courseService, ProjectService projectService,
            String server_address,
            String group_id) {
        this.userService = userService;
        this.courseService = courseService;
        this.projectService = projectService;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server_address);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group_id);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public void run() {
        while (running) {
            records = consumer.poll(Duration.of(5, ChronoUnit.SECONDS));
            if (!records.isEmpty()) {
                for (ConsumerRecord<String, String> record : records) {
                    String[] parts = record.value().split(",");
                    String userId = parts[0];
                    String courseId = parts[1];
                    Course course = courseService.getCourse(courseId);
                    if (course != null) {
                        userService.enrollCourse(userId, course);
                    }
                }
                this.offsets = consumerOffsetsToMap(this.consumer, records);
            }
        }
    }

    public void shutdown() {
        running = false;
        commitOffsetAndClose();
    }

    public void commitOffsetAndClose() {
        try {
            commitOffset();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    public ConsumerRecords<String, String> getRecords() {
        return records;
    }

    public KafkaConsumer<String, String> getConsumer() {
        return consumer;
    }

    public void commitOffset() {
        consumer.commitSync(offsets);
        System.out.println("Commit avvenuto con successo");
        return;
    }
}
