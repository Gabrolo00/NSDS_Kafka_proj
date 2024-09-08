package it.polimi.middleware.kafka.Backend;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import it.polimi.middleware.kafka.Backend.Users.User;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Producer {
    private KafkaProducer<String, String> producer;
    private String userTopic = "user-events";
    private String courseTopic = "course-events";
    private String projectTopic = "project-events";
    private String producerTransactionalId = "producer-transactional-id";

    public Producer(String server_address, boolean EOS) { // EOS true garantisce l'EOS e transaction
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server_address);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        if (EOS) {
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(true));
            props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, producerTransactionalId);
        }

        producer = new KafkaProducer<>(props);
    }

    // producer invia utente per registrazione
    public void sendUserRegistration(User user) {
        ProducerRecord<String, String> record = new ProducerRecord<>(userTopic, user.getUserId(),
                user.toString());
        waitMsg(record);
    }

    public void sendEnrollmentEvent(String eventType, String userId, String courseId) {

        JSONObject event = new JSONObject();
        event.put("type", eventType);
        event.put("userId", userId);
        event.put("courseId", courseId);

        ProducerRecord<String, String> record = new ProducerRecord<>(courseTopic, userId, event.toString());
        waitMsg(record);
    }

    // producer invia corso per registrazione
    public void sendCourseRegistration(String eventType, Course course) {

        JSONObject event = new JSONObject();
        event.put("type", eventType);
        event.put("data", course.toString());

        ProducerRecord<String, String> record = new ProducerRecord<>(courseTopic, course.getCourseId(),
                event.toString());
        waitMsg(record);

    }

    public void sendProjectRegistration(String eventType, Project project) {

        JSONObject event = new JSONObject();
        event.put("type", eventType);
        event.put("data", project.toString());

        ProducerRecord<String, String> record = new ProducerRecord<>(projectTopic, project.getProjectId(),
                event.toString());
        waitMsg(record);
    }

    public void sendUpdateMapProject(String eventType, String studentId, Course course) {

        JSONObject event = new JSONObject();

        for (Project project : course.getProjects()) {

            event.put("type", eventType);
            event.put("studentId", studentId);
            event.put("courseId", course.getCourseId());
            event.put("projectId", project.getProjectId());

            ProducerRecord<String, String> record = new ProducerRecord<>(projectTopic, project.getProjectId(),
                    event.toString());
            waitMsg(record);

        }

    }

    public void sendCourseDelete(String eventType, String courseId) {

        JSONObject event = new JSONObject();
        event.put("type", eventType);
        event.put("data", courseId);

        ProducerRecord<String, String> record = new ProducerRecord<>(courseTopic, courseId,
                event.toString());
        waitMsg(record);

    }

    public void sendSubmitEvent(String eventType, String userId, String courseId, String projectId, String allegato) {
        JSONObject event = new JSONObject();
        event.put("type", eventType);
        event.put("userId", userId);
        event.put("courseId", courseId);
        event.put("projectId", projectId);
        event.put("allegato", allegato);

        ProducerRecord<String, String> record = new ProducerRecord<>(projectTopic, userId, event.toString());
        waitMsg(record);
    }

    public void sendRateEvent(String eventType, String userId, String courseId, String projectId, String voto) {
        JSONObject event = new JSONObject();
        event.put("type", eventType);
        event.put("userId", userId);
        event.put("courseId", courseId);
        event.put("projectId", projectId);
        event.put("voto", voto);

        ProducerRecord<String, String> record = new ProducerRecord<>(projectTopic, userId, event.toString());
        System.out.println("ENTRO NEL WAIT");
        waitMsg(record);
    }

    public void sendCheckCompleteCourse(String eventType, String userId, String courseId, String projectId) {
        JSONObject event = new JSONObject();
        event.put("type", eventType);
        event.put("userId", userId);
        event.put("courseId", courseId);
        event.put("projectId", projectId);

        ProducerRecord<String, String> record = new ProducerRecord<>(courseTopic, userId, event.toString());
        waitMsg(record);
    }

    public KafkaProducer<String, String> getProducer() {
        return producer;
    }

    public void waitMsg(ProducerRecord<String, String> record) {

        Future<RecordMetadata> future = producer.send(record);

        System.out.println("ENTRATO NEL WAIT");

        if (true) {
            try {
                RecordMetadata ack = future.get();
                System.out.println("Message sent to partition " + ack.partition() + " with offset " + ack.offset());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

        }

    }
}