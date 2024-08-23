package it.polimi.middleware.kafka.Backend.Services;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import it.polimi.middleware.kafka.Backend.Course;
import it.polimi.middleware.kafka.Backend.Users.Student;
import it.polimi.middleware.kafka.Backend.Users.Professor;
import it.polimi.middleware.kafka.Backend.Users.Admin;
import it.polimi.middleware.kafka.Backend.Users.User;

public class UserService {
    private Map<String, User> userMap = new ConcurrentHashMap<>();

    public User registerUser(User user) {

        String role = user.getRole();

        if (role.equals("STUDENT")) {
            Student s = new Student(user.getUserId(), user.getName(), user.getEmail(), user.getPassword());
            return userMap.put(user.getUserId(), s);
        }

        else if (role.equals("PROFESSOR")) {
            Professor p = new Professor(user.getUserId(), user.getName(), user.getEmail(), user.getPassword());
            return userMap.put(user.getUserId(), p);
        }

        else {
            Admin a = new Admin(user.getUserId(), user.getName(), user.getEmail(), user.getPassword());
            return userMap.put(user.getUserId(), a);
        }

    }

    public User getUser(String userId) {
        return userMap.get(userId);
    }

    public Map<String, User> getAllUser() {
        return userMap;
    }

    public boolean checkRegister(User user) {

        return (user.getRole().equals("STUDENT") || user.getRole().equals("PROFESSOR")
                || user.getRole().equals("ADMIN"));
    }

    public boolean authenticateUser(String userId, String password) {
        User user = userMap.get(userId);
        return user.getPassword().equals(password);
    }

    public boolean checkRole(String userId, String role) {
        User user = userMap.get(userId);
        return user.getRole().equals(role);
    }

    public boolean enrollCourse(String userId, Course course) {
        User user = userMap.get(userId);
        if (user != null && user.getRole().equals("STUDENT")) {
            Student student = (Student) user;
            student.enrollCourse(course);
            course.addStudent(student);
            return true;
        }
        return false;
    }
}