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
import it.polimi.middleware.kafka.Backend.Course;
import it.polimi.middleware.kafka.Backend.Project;
import it.polimi.middleware.kafka.Backend.Producer;

public class CreateProjectServlet extends HttpServlet {

    private final UserService userService;
    private final CourseService courseService;
    private final ProjectService projectService;

    public CreateProjectServlet(UserService userService, CourseService courseService, ProjectService projectService) {
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

        if (profId != null) {
            // Rispondi al client
            User user = userService.getUser(profId);

            if (userService.checkRole(profId, "PROFESSOR")) {

                StringBuilder sb = new StringBuilder();
                String line;

                Project project;
                while ((line = req.getReader().readLine()) != null) {
                    sb.append(line);
                }
                String jsonString = sb.toString();

                // Crea un oggetto User dal JSON
                JSONObject json = new JSONObject(jsonString);

                Course course = courseService.getCourse(json.getString("courseId"));

                if (course != null) { // controllo se il corso è inesistente

                    if (course.getProject(json.getString("projectId")) == null) { // controllo se il progetto è
                                                                                  // già esistente
                        project = new Project(json.getString("projectId"), json.getString("courseId"), profId);

                        Producer producer = new Producer("localhost:9092", true);
                        producer.getProducer().initTransactions();

                        try {

                            producer.getProducer().beginTransaction();

                            producer.sendProjectRegistration("CREATE", project);

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
                            out.print("{\"status\":\"error\", \"message\":\"Errore durante l'aggiunta del progetto\"}");
                            out.flush();
                            return;
                        }
                        resp.setContentType("application/json");
                        resp.setCharacterEncoding("UTF-8");
                        PrintWriter out = resp.getWriter();
                        out.print("{\"status\":\"success\", \"message\":\"Project created. \"}");
                        out.flush();
                    } else {
                        // ERRORE PROGETTO GIA' ESISTENTE
                        resp.setContentType("application/json");
                        resp.setCharacterEncoding("UTF-8");
                        PrintWriter out = resp.getWriter();
                        out.print("{\"status\":\"error\", \"message\":\"Project already exist.\"}");
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