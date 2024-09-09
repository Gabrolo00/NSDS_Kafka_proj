package it.polimi.middleware.kafka.Backend;

import java.util.ArrayList;
import java.util.List;
import it.polimi.middleware.kafka.Backend.Users.Student;

public class Course {

    private String courseId;
    private String name;
    private String adminId;
    private List<Project> projects;
    private List<Student> enrolledStudents;
    private boolean isDeleted; // Flag di eliminazione

    public Course(String courseId, String name, String adminId) {

        this.courseId = courseId;
        this.name = name;
        this.adminId = adminId;
        this.projects = new ArrayList<Project>();
        this.enrolledStudents = new ArrayList<Student>();
        isDeleted = false;
    }

    public boolean isDeleted() {
        return isDeleted;
    }

    public void setDeleted(boolean isDeleted) {
        this.isDeleted = isDeleted;
    }

    public String getCourseId() {
        return courseId;
    }

    public void setCourseId(String courseId) {
        this.courseId = courseId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAdminId() {
        return adminId;
    }

    public void setAdminId(String adminId) {
        this.adminId = adminId;
    }

    public List<Project> getProjects() {
        return projects;
    }

    public void setProjects(List<Project> projects) {
        this.projects = projects;
    }

    @Override
    public String toString() {
        return courseId + "," + name + "," + adminId + "," + isDeleted;
    }

    public static Course fromString(String courseString) {
        String[] parts = courseString.split(",");
        Course course = new Course(parts[0], parts[1], parts[2]);
        course.setDeleted(Boolean.parseBoolean(parts[3]));
        return course;
    }

    public Project getProject(String projectId) {
        for (Project project : projects) {
            if (project.getProjectId().equals(projectId)) {
                return project;
            }
        }
        return null; // Se non troviamo il progetto, restituiamo null
    }

    public boolean addProject(Project project) {
        if (getProject(project.getProjectId()) == null) { // verifico se esiste già il progetto
            projects.add(project);
            return true;
        } else
            return false;
    }

    public Student getStudent(String studentId) {
        for (Student student : enrolledStudents) {
            if (student.getUserId().equals(studentId)) {
                return student;
            }
        }
        return null; // Se non troviamo lo studente, restituiamo null
    }

    public boolean addStudent(Student student) {
        if (getStudent(student.getUserId()) == null) { // verifico se esiste già lo studente
            enrolledStudents.add(student);
            System.out.println("Studente aggiunto all'array enrolledStudents");

            for (Student student2 : enrolledStudents) {
                System.out.println("StudentId : " + student2.getUserId());
            }
            return true;
        } else
            return false;
    }

    public List<Student> getEnrolledStudents() {
        return enrolledStudents;
    }

    public void setEnrolledStudents(List<Student> enrolledStudents) {
        this.enrolledStudents = enrolledStudents;
    }

    public Boolean isCompletedByStudent(Student student) {
        for (Project project : projects) {
            Integer grade = project.getEvaluationMap().get(student.getUserId());
            if (grade == -2 || grade == -1 || grade < 18) {
                return false; // Il corso non è stato completato
            }
        }
        return true;
    }

}
