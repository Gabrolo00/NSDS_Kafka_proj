package it.polimi.middleware.kafka.Backend.Servlet;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.PrintWriter;

import org.json.JSONObject;

import it.polimi.middleware.kafka.Backend.Services.CourseService;
import it.polimi.middleware.kafka.Backend.Services.JwtUtil;
import it.polimi.middleware.kafka.Backend.Services.ProjectService;
import it.polimi.middleware.kafka.Backend.Services.UserService;
import it.polimi.middleware.kafka.Backend.Users.Professor;
import it.polimi.middleware.kafka.Backend.Users.User;
import it.polimi.middleware.kafka.Backend.Users.Student;
import it.polimi.middleware.kafka.Backend.Course;
import it.polimi.middleware.kafka.Backend.Project;
import it.polimi.middleware.kafka.Backend.Producer;

public class RateProjectServlet extends HttpServlet {

    private final UserService userService;
    private final CourseService courseService;
    private final ProjectService projectService;

    public RateProjectServlet(UserService userService, CourseService courseService, ProjectService projectService) {
        this.userService = userService;
        this.courseService = courseService;
        this.projectService = projectService;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        // Leggi il corpo della richiesta

        String token = req.getHeader("Authorization");
        String profId = JwtUtil.validateToken(token);

        System.out.println(profId);

        if (profId != null) { // check auth valido

            if (userService.checkRole(profId, "PROFESSOR")) { // check ruolo = STUDENT

                StringBuilder sb = new StringBuilder();
                String line;

                Project project;

                while ((line = req.getReader().readLine()) != null) {
                    sb.append(line);
                }
                String jsonString = sb.toString();

                JSONObject json = new JSONObject(jsonString);

                String studentId = json.getString("studentId");
                String courseId = json.getString("courseId");
                String projectId = json.getString("projectId");
                String vote = json.getString("grade");

                Student student = (Student) userService.getUser(studentId);
                Course course = courseService.getCourse(courseId);

                if (student != null) // controllo se studente esiste
                {

                    if (course != null) { // controllo se il corso è inesistente

                        if (student.checkCourseExist(course)) { // controllo se lo studente è iscritto al corso

                            project = course.getProject(projectId);

                            if (project != null) { // controllo se il progetto esiste

                                if (project.getProfId().equals(profId)) { // controllo se il proj è creato dal prof
                                                                          // loggato

                                    Integer submittedVote = project.getVote(studentId);

                                    if (submittedVote == -1) { // controllo se è stato già effettuato il submit

                                        Producer producer = new Producer("localhost:9092", true);
                                        producer.getProducer().initTransactions();

                                        try {

                                            producer.getProducer().beginTransaction();
                                            producer.sendRateEvent("RATE", studentId, courseId, projectId, vote);

                                            Thread.sleep(2000);
                                            Boolean courseCompleted = course.isCompletedByStudent(student);

                                            if (courseCompleted) {
                                                producer.sendCheckCompleteCourse("COMPLETE", studentId, courseId,
                                                        projectId); // si manda projectId per verificare che sia
                                                                    // stato consumato prima l'evento RATE

                                                producer.getProducer().commitTransaction();
                                                producer.getProducer().close();

                                                resp.setContentType("application/json");
                                                resp.setCharacterEncoding("UTF-8");
                                                PrintWriter out = resp.getWriter();
                                                out.print(
                                                        "{\"status\":\"success\", \"message\":\"Project rated. The student has completed this course!\"}");
                                                out.flush();
                                                return;
                                            }

                                            producer.getProducer().commitTransaction();
                                            producer.getProducer().close();

                                        } catch (Exception e) {
                                            producer.getProducer().abortTransaction();
                                            e.printStackTrace();
                                            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);

                                            // messaggio d'errore

                                            resp.setContentType("application/json");
                                            resp.setCharacterEncoding("UTF-8");
                                            PrintWriter out = resp.getWriter();
                                            out.print(
                                                    "{\"status\":\"error\", \"message\":\"Errore durante l'aggiunta del voto\"}");
                                            out.flush();
                                            return;
                                        }

                                        resp.setContentType("application/json");
                                        resp.setCharacterEncoding("UTF-8");
                                        PrintWriter out = resp.getWriter();
                                        out.print("{\"status\":\"success\", \"message\":\"Project rated. \"}");
                                        out.flush();

                                    } else if (submittedVote == null) {
                                        // SUBMIT NON ANCORA EFFETTUATO
                                        resp.setContentType("application/json");
                                        resp.setCharacterEncoding("UTF-8");
                                        PrintWriter out = resp.getWriter();
                                        out.print(
                                                "{\"status\":\"error\", \"message\":\"project has not submitted yet.\"}");
                                        out.flush();
                                    } else {
                                        // PROGETTO GIA VOTATO
                                        resp.setContentType("application/json");
                                        resp.setCharacterEncoding("UTF-8");
                                        PrintWriter out = resp.getWriter();
                                        out.print("{\"status\":\"error\", \"message\":\"Project already rated.\"}");
                                        out.flush();
                                    }
                                } else {
                                    // PROGETTO NON CREATO DAL PROFESSORE
                                    resp.setContentType("application/json");
                                    resp.setCharacterEncoding("UTF-8");
                                    PrintWriter out = resp.getWriter();
                                    out.print(
                                            "{\"status\":\"error\", \"message\":\"You can't evaluate this project.\"}");
                                    out.flush();
                                }

                            } else {
                                // PROGETTO NON ESISTENTE
                                resp.setContentType("application/json");
                                resp.setCharacterEncoding("UTF-8");
                                PrintWriter out = resp.getWriter();
                                out.print("{\"status\":\"error\", \"message\":\"Project not exist.\"}");
                                out.flush();
                            }

                        } else {
                            // ERRORE STUDENTE NON ISCRITTO AL CORSO INDICATO
                            resp.setContentType("application/json");
                            resp.setCharacterEncoding("UTF-8");
                            PrintWriter out = resp.getWriter();
                            out.print("{\"status\":\"error\", \"message\":\"Student not enrolled in the course.\"}");
                            out.flush();
                        }

                    } else {
                        // ERRORE CORSO INESISTENTE
                        resp.setContentType("application/json");
                        resp.setCharacterEncoding("UTF-8");
                        PrintWriter out = resp.getWriter();
                        out.print("{\"status\":\"error\", \"message\":\"Course does not exist.\"}");
                        out.flush();
                    }

                } else {
                    // ERRORE STUDENTE INESISTENTE
                    resp.setContentType("application/json");
                    resp.setCharacterEncoding("UTF-8");
                    PrintWriter out = resp.getWriter();
                    out.print("{\"status\":\"error\", \"message\":\"Student does not exist.\"}");
                    out.flush();
                }

            } else {
                // ERRORE UNAUTHORIZED 401
                resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                resp.setContentType("application/json");
                resp.setCharacterEncoding("UTF-8");
                PrintWriter out = resp.getWriter();
                out.print("{\"status\":\"Error\", \"message\":\" Non hai il permesso di creare un corso \" }");
                out.flush();
            }

        } else {
            // ERRORE TOKEN NON VALIDO
            resp.setContentType("application/json");
            resp.setCharacterEncoding("UTF-8");
            PrintWriter out = resp.getWriter();
            out.print("{\"status\":\"success\", \"message\":\"Token non valido. \" }");
            out.flush();
        }

    }
}