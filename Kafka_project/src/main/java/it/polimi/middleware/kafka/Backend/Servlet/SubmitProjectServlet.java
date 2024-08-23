package it.polimi.middleware.kafka.Backend.Servlet;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.JSONObject;

import it.polimi.middleware.kafka.Backend.Services.CourseService;
import it.polimi.middleware.kafka.Backend.Services.JwtUtil;
import it.polimi.middleware.kafka.Backend.Services.ProjectService;
import it.polimi.middleware.kafka.Backend.Services.RegistrationService;
import it.polimi.middleware.kafka.Backend.Services.UserService;
import it.polimi.middleware.kafka.Backend.Users.Professor;
import it.polimi.middleware.kafka.Backend.Users.User;
import it.polimi.middleware.kafka.Backend.Users.Student;
import it.polimi.middleware.kafka.Backend.Course;
import it.polimi.middleware.kafka.Backend.Project;
import it.polimi.middleware.kafka.Backend.Producer;

public class SubmitProjectServlet extends HttpServlet {

    private final UserService userService;
    private final CourseService courseService;
    private final ProjectService projectService;
    private final RegistrationService registrationService;

    public SubmitProjectServlet(UserService userService, CourseService courseService, ProjectService projectService,
            RegistrationService registrationService) {
        this.userService = userService;
        this.courseService = courseService;
        this.projectService = projectService;
        this.registrationService = registrationService;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        // Leggi il corpo della richiesta

        String token = req.getHeader("Authorization");
        String studentId = JwtUtil.validateToken(token);

        System.out.println(studentId);

        if (studentId != null) { // check auth valido

            if (userService.checkRole(studentId, "STUDENT")) { // check ruolo = STUDENT

                Student student = (Student) userService.getUser(studentId);

                StringBuilder sb = new StringBuilder();
                String line;

                Project project;

                while ((line = req.getReader().readLine()) != null) {
                    sb.append(line);
                }
                String jsonString = sb.toString();

                JSONObject json = new JSONObject(jsonString);

                String courseId = json.getString("courseId");
                String projectId = json.getString("projectId");
                String allegato = json.getString("allegato");

                Course course = courseService.getCourse(courseId);

                if (course != null) { // controllo se il corso è inesistente

                    if (student.checkCourseExist(course)) { // controllo se lo studente è iscritto al corso

                        Map<String, List<String>> regMap = new ConcurrentHashMap<>();
                        regMap = registrationService.getCompletedMap();

                        List<String> listCourse = regMap.get(studentId);
                        Boolean isCompleated = false;
                        if (listCourse != null) {
                            isCompleated = listCourse.contains(course.getCourseId());
                        }

                        if (isCompleated == false) // controllo se lo studente ha completato il corso
                        {
                            project = course.getProject(projectId);

                            if (project != null) { // controllo se il progetto esiste

                                Integer vote = project.getVote(studentId);
                                System.out.println("voto: " + vote);

                                if (vote == -2) { // controllo se è stato già effettuato il submit

                                    Producer producer = new Producer("localhost:9092", true);
                                    producer.getProducer().initTransactions();

                                    try {

                                        producer.getProducer().beginTransaction();
                                        producer.sendSubmitEvent("SUBMIT", studentId, courseId, projectId,
                                                allegato);

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
                                                "{\"status\":\"error\", \"message\":\"Errore durante l'aggiunta del progetto\"}");
                                        out.flush();
                                        return;
                                    }

                                    resp.setContentType("application/json");
                                    resp.setCharacterEncoding("UTF-8");
                                    PrintWriter out = resp.getWriter();
                                    out.print("{\"status\":\"status\", \"message\":\"Project submitted. \"}");
                                    out.flush();

                                } else {
                                    // SUBMIT GIA EFFETTUATO
                                    resp.setContentType("application/json");
                                    resp.setCharacterEncoding("UTF-8");
                                    PrintWriter out = resp.getWriter();
                                    out.print("{\"status\":\"error\", \"message\":\"Project already submitted.\"}");
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
                            out.print(
                                    "{\"status\":\"error\", \"message\":\"Student has already compleated this course.\"}");
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