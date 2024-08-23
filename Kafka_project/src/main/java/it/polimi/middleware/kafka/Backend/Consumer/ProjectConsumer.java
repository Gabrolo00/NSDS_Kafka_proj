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
import it.polimi.middleware.kafka.Backend.Services.ProjectService;
import it.polimi.middleware.kafka.Backend.Services.UserService;
import it.polimi.middleware.kafka.Backend.Users.User;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ProjectConsumer extends Thread {
    private KafkaConsumer<String, String> consumer;
    private String topic = "project-events";
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

    public ProjectConsumer(CourseService courseService, ProjectService projectService, String server_address,
            String group_id) {
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
            try {
                records = consumer.poll(Duration.of(5, ChronoUnit.SECONDS));
                if (!records.isEmpty()) {

                    System.out.println("Received records: " + records.count());
                    for (ConsumerRecord<String, String> record : records) {

                        JSONObject event = new JSONObject(record.value());
                        String eventType = event.getString("type");

                        switch (eventType) {
                            case "CREATE":
                                Project project = Project.fromString(event.getString("data"));
                                projectService.addProjectToCourse(courseService.getCourse(project.getCourseId()),
                                        project);
                                break;

                            case "SUBMIT":
                                System.out.println("Processing SUBMIT event: " + event.toString());
                                String studentId = event.getString("userId");
                                String courseId = event.getString("courseId");
                                String projectId = event.getString("projectId");
                                String allegato = event.getString("allegato");
                                Course course = courseService.getCourse(courseId);
                                projectService.submitProject(studentId, course, projectId, allegato);
                                break;

                            case "RATE":

                                String studentid = event.getString("userId");
                                String project_id = event.getString("projectId");
                                String course_id = event.getString("courseId");
                                Integer voto = Integer.parseInt(event.getString("voto"));

                                Project project_ = courseService.getCourse(course_id).getProject(project_id);
                                projectService.rateProject(studentid, project_, voto);
                                break;

                            case "UPDATEMAP":

                                String studentid2 = event.getString("studentId");
                                String courseid = event.getString("courseId");
                                String projectid = event.getString("projectId");

                                Project project2 = courseService.getCourse(courseid).getProject(projectid);

                                projectService.updateMapProject(studentid2, project2);
                                break;
                            /*
                             * case "DELETE":
                             * String projectId = event.getString("data");
                             * projectService.deleteProject(projectId);
                             * break;
                             */
                        }

                    }

                    this.offsets = consumerOffsetsToMap(this.consumer, records);

                }

            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("Error processing records: " + e.getMessage());
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
