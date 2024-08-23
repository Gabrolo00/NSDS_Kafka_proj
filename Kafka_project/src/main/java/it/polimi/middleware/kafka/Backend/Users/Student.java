package it.polimi.middleware.kafka.Backend.Users;

import java.util.ArrayList;
import java.util.List;

import it.polimi.middleware.kafka.Backend.Course;
import it.polimi.middleware.kafka.Backend.Project;

public class Student extends User {

    private List<Course> courses;

    public Student(String userId, String name, String email, String password) {
        super(userId, name, email, password, "STUDENT");
        courses = new ArrayList<Course>();
    }

    public void enrollCourse(Course course) {
        if (!courses.contains(course)) {
            courses.add(course);
            return;
        }
    }

    public List<Course> getCourses() {
        return courses;
    }

    public void setCourses(List<Course> courses) {
        this.courses = courses;
    }

    public boolean checkCourseExist(Course course) {
        if (courses.contains(course))
            return true;
        else
            return false;
    }

}
