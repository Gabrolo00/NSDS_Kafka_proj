package it.polimi.middleware.kafka.Backend;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import it.polimi.middleware.kafka.Backend.Users.User;

public class Project {

    private String projectId;
    private String courseId;
    private String profId;
    private Map<String, Integer> evaluationMap = new ConcurrentHashMap<>(); // vote = -2 -> no submit, no evaluation
                                                                            // vote = -1 -> si submit, no evaluation
                                                                            // vote >= 0 -> si submit, si evaluation

    public Project(String projectId, String courseId, String profId) {

        this.projectId = projectId;
        this.courseId = courseId;
        this.profId = profId;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getCourseId() {
        return courseId;
    }

    public void setCourseId(String courseId) {
        this.courseId = courseId;
    }

    public String getProfId() {
        return profId;
    }

    public void setProfId(String profId) {
        this.profId = profId;
    }

    public Map<String, Integer> getEvaluationMap() {
        return evaluationMap;
    }

    public void setEvaluationMap(Map<String, Integer> evaluationMap) {
        this.evaluationMap = evaluationMap;
    }

    public void addStudent(String studentId) {
        if (!evaluationMap.containsKey(studentId))
            this.evaluationMap.put(studentId, -2);
    }

    public void updateEvaluation(String userId, Integer rate) {
        if (evaluationMap.containsKey(userId)) {
            evaluationMap.replace(userId, rate);
        }
    }

    @Override
    public String toString() {
        return projectId + "," + courseId + "," + profId;
    }

    public static Project fromString(String courseString) {
        String[] parts = courseString.split(",");
        Project project = new Project(parts[0], parts[1], parts[2]);
        return project;
    }

    public Integer getVote(String studentId) { //
        return evaluationMap.get(studentId);
    }

}
