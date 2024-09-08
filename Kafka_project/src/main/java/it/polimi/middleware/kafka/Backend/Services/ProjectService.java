package it.polimi.middleware.kafka.Backend.Services;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import it.polimi.middleware.kafka.Backend.Users.Student;
import it.polimi.middleware.kafka.Backend.Users.User;
import it.polimi.middleware.kafka.Backend.Course;
import it.polimi.middleware.kafka.Backend.Project;

public class ProjectService {

    public void addProjectToCourse(Course course, Project project) {
        if (course != null) {
            course.addProject(project);
            initializeProject(course, project);
            return;
        } else
            return;
    }

    public void initializeProject(Course course, Project project) {
        Map<String, Integer> initEvaluationMap = new ConcurrentHashMap<>();
        for (Student student : course.getEnrolledStudents()) {
            initEvaluationMap.put(student.getUserId(), -2);
        }

        project.setEvaluationMap(initEvaluationMap);
    }

    public void submitProject(String userId, Course course, String projectId, String allegato) {

        for (Student student2 : course.getEnrolledStudents()) {
            System.out.println("studentId : " + student2.getUserId());
        }

        if (course.getStudent(userId) != null) { // check se lo studente è iscritto al corso
            course.getProject(projectId).updateEvaluation(userId, -1);
            return;
        } else
            return;
    }

    public void rateProject(String userId, Project project, Integer voto) {

        System.out.println("voto: " + project.getVote(userId));
        if (project.getVote(userId) == -1) {
            project.updateEvaluation(userId, voto);
            return;
        } else
            return;
    }

    public void updateMapProject(String studentId, Project project) {
        project.addStudent(studentId);
    }
}