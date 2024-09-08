package it.polimi.middleware.kafka.Backend.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import it.polimi.middleware.kafka.Backend.Course;
import it.polimi.middleware.kafka.Backend.Project;
import it.polimi.middleware.kafka.Backend.Services.CourseService;
import it.polimi.middleware.kafka.Backend.Services.RegistrationService;
import it.polimi.middleware.kafka.Backend.Services.UserService;
import it.polimi.middleware.kafka.Backend.Users.Student;
import it.polimi.middleware.kafka.Backend.Users.User;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class CourseConsumer extends Thread {
    private KafkaConsumer<String, String> consumer;
    private String topic = "course-events";
    private CourseService courseService;
    private RegistrationService registrationService;
    private UserService userService;
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

    public CourseConsumer(UserService userService, CourseService courseService, RegistrationService registrationService,
            String server_address,
            String group_id) {

        this.userService = userService;
        this.courseService = courseService;
        this.registrationService = registrationService;

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

                    JSONObject event = new JSONObject(record.value());
                    String eventType = event.getString("type");

                    switch (eventType) {
                        case "CREATE":

                            Course course = Course.fromString(event.getString("data"));
                            courseService.add_course(course);
                            break;

                        case "DELETE":

                            String courseId = event.getString("data");

                            while (courseService.getCourse(courseId) == null) {
                                try {
                                    Thread.sleep(2000);
                                    System.out.println("------ PROVATO DELETE ---------");
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                            courseService.markCourseAsDeleted(courseId);
                            System.out.println("------ DELETE FATTO ---------");
                            break;

                        case "ENROLL":

                            String userid = event.getString("userId");
                            String courseid = event.getString("courseId");

                            while (userService.getUser(userid) == null) {
                                try {
                                    Thread.sleep(2000);
                                    System.out.println("------ PROVATO INSERIMENTO ENROLL ---------");
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }

                            Course course_ = courseService.getCourse(courseid);
                            if (course_ != null) {
                                userService.enrollCourse(userid, course_);
                                System.out.println("------ ENROLL INSERITO ---------");
                            }

                            break;

                        case "COMPLETE":

                            String userId = event.getString("userId");
                            String courseId2 = event.getString("courseId");
                            String projectId = event.getString("projectId");

                            while (userService.getUser(userId) == null
                                    || courseService.getCourse(courseId2) == null) {
                                try {
                                    Thread.sleep(2000);
                                    System.out.println("------ PROVATO INSERIMENTO COMPLETE ---------");
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }

                            while (courseService.getCourse(courseId2).getProject(projectId) == null) {
                                try {
                                    Thread.sleep(2000);
                                    System.out.println("------ PROVATO INSERIMENTO COMPLETE ---------");
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }

                            while (courseService.getCourse(courseId2).getProject(projectId).getVote(userId) < 0) {
                                try {
                                    Thread.sleep(2000);
                                    System.out.println("------ PROVATO INSERIMENTO COMPLETE ---------");
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }

                            Student student = (Student) userService.getUser(userId);

                            if (courseId2 != null) {
                                Course course2 = courseService.getCourse(courseId2);
                                registrationService.registerCompletedCourse(student, course2);
                            }

                            System.out.println("------ INSERIMENTO COMPLETE FATTO ---------");

                            break;
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

    public CourseService getUserService() {
        return courseService;
    }

    public void commitOffset() {
        consumer.commitSync(offsets);
        System.out.println("Commit avvenuto con successo");
        return;
    }
}
