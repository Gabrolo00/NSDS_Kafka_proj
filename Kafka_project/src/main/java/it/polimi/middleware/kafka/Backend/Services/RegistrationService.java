package it.polimi.middleware.kafka.Backend.Services;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import it.polimi.middleware.kafka.Backend.Course;
import it.polimi.middleware.kafka.Backend.Users.Student;
import it.polimi.middleware.kafka.Backend.Users.User;

public class RegistrationService {

    private Map<String, List<String>> completedMap = new ConcurrentHashMap<>(); // <userId,lista_courseId> solo se user
                                                                                // ha completato almeno un corso

    public void registerCompletedCourse(Student student, Course course) {
        if (completedMap.containsKey(student.getUserId()) != false) {
            completedMap.get(student.getUserId()).add(course.getCourseId());
        } else {
            List<String> completedCourses = new ArrayList<>();
            completedCourses.add(course.getCourseId());

            completedMap.put(student.getUserId(), completedCourses);
        }
        System.out.println("Corso completato: " + course.getName() + " dallo studente: " + student.getName());
    }

    public Map<String, List<String>> getCompletedMap() {
        return completedMap;
    }

    public void setCompletedMap(Map<String, List<String>> completedMap) {
        this.completedMap = completedMap;
    }

}