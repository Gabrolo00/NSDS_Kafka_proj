package it.polimi.middleware.kafka.Backend.Servlet;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.PrintWriter;

import org.json.JSONObject;

import it.polimi.middleware.kafka.Backend.Services.CourseService;
import it.polimi.middleware.kafka.Backend.Services.JwtUtil;
import it.polimi.middleware.kafka.Backend.Services.UserService;
import it.polimi.middleware.kafka.Backend.Users.Professor;
import it.polimi.middleware.kafka.Backend.Users.User;
import it.polimi.middleware.kafka.Backend.Course;
import it.polimi.middleware.kafka.Backend.Producer;

public class CreateCourseServlet extends HttpServlet {

    private final UserService userService;
    private final CourseService courseService;

    public CreateCourseServlet(UserService userService, CourseService courseService) {
        this.userService = userService;
        this.courseService = courseService;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        // Leggi il corpo della richiesta

        String token = req.getHeader("Authorization");
        String adminId = JwtUtil.validateToken(token);

        System.out.println(adminId);

        if (adminId != null) {
            // Rispondi al client
            User user = userService.getUser(adminId);

            if (userService.checkRole(adminId, "ADMIN")) {

                StringBuilder sb = new StringBuilder();
                String line;

                Course course;
                while ((line = req.getReader().readLine()) != null) {
                    sb.append(line);
                }
                String jsonString = sb.toString();

                // Crea un oggetto User dal JSON
                JSONObject json = new JSONObject(jsonString);

                course = new Course(json.getString("courseId"), json.getString("courseName"), adminId);

                Producer producer = new Producer("localhost:9092", true);

                if (courseService.getCourse(course.getCourseId()) == null) { // controllo se il corso è già esistente

                    producer.getProducer().initTransactions();

                    try {

                        producer.getProducer().beginTransaction();
                        producer.sendCourseRegistration("CREATE", course);

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
                        out.print("{\"status\":\"error\", \"message\":\"Errore durante l'aggiunta del corso\"}");
                        out.flush();
                        return;
                    }
                    resp.setContentType("application/json");
                    resp.setCharacterEncoding("UTF-8");
                    PrintWriter out = resp.getWriter();
                    out.print("{\"status\":\"success\", \"message\":\"Course created. \"}");
                    out.flush();

                } else {
                    // ERRORE CORSO GIA ESISTENTE
                    resp.setContentType("application/json");
                    resp.setCharacterEncoding("UTF-8");
                    PrintWriter out = resp.getWriter();
                    out.print("{\"status\":\"error\", \"message\":\"Course already registered.\"}");
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
            out.print("{\"status\":\"error\", \"message\":\"Token non valido. \" }");
            out.flush();
        }

    }
}