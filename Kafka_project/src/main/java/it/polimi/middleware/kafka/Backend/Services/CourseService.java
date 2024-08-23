package it.polimi.middleware.kafka.Backend.Services;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import it.polimi.middleware.kafka.Backend.Users.User;
import it.polimi.middleware.kafka.Backend.Course;

public class CourseService {
    private Map<String, Course> courseMap = new ConcurrentHashMap<>();

    public Course add_course(Course course) {
        return courseMap.put(course.getCourseId(), course);
    }

    public Course getCourse(String courseId) {
        Course course = courseMap.get(courseId);
        if (course != null && course.isDeleted()) {
            return null;
        }
        return course;
    }

    public void markCourseAsDeleted(String courseId) {
        Course course = courseMap.get(courseId);
        if (course != null) {
            course.setDeleted(true);
        }
    }

    public Map<String, Course> getAllCourse() {
        return courseMap;
    }

}