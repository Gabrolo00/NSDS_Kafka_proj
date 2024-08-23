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
import it.polimi.middleware.kafka.Backend.Users.User;
import it.polimi.middleware.kafka.Backend.Course;
import it.polimi.middleware.kafka.Backend.Producer;

public class EnrollCourseServlet extends HttpServlet {

    private final UserService userService;
    private final CourseService courseService;

    public EnrollCourseServlet(UserService userService, CourseService courseService) {
        this.userService = userService;
        this.courseService = courseService;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        // Leggi il corpo della richiesta

        String token = req.getHeader("Authorization");
        String studentId = JwtUtil.validateToken(token);

        System.out.println(studentId);

        if (studentId != null) {
            // Rispondi al client
            User user = userService.getUser(studentId);

            if (userService.checkRole(studentId, "STUDENT")) {

                StringBuilder sb = new StringBuilder();
                String line;

                Course course;
                while ((line = req.getReader().readLine()) != null) {
                    sb.append(line);
                }
                String jsonString = sb.toString();

                // Crea un oggetto User dal JSON
                JSONObject json = new JSONObject(jsonString);

                String courseId = json.getString("courseId");

                course = courseService.getCourse(courseId);

                if (course != null) { // controllo se il corso è già esistente

                    if (course.getStudent(studentId) == null) {

                        Producer producer = new Producer("localhost:9092", true);

                        producer.getProducer().initTransactions();

                        try {

                            producer.getProducer().beginTransaction();
                            producer.sendEnrollmentEvent("ENROLL", studentId, courseId);

                            if (!course.getProjects().isEmpty())
                                producer.sendUpdateMapProject("UPDATEMAP", studentId, course);

                            producer.getProducer().commitTransaction();
                            producer.getProducer().close();

                        } catch (Exception e) {

                            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);

                            // Messaggio d'errore
                            resp.setContentType("application/json");
                            resp.setCharacterEncoding("UTF-8");
                            PrintWriter out = resp.getWriter();
                            out.print("{\"status\":\"error\", \"message\":\"Enrollment failed.\"}");
                            out.flush();

                            producer.getProducer().abortTransaction();
                            e.printStackTrace();
                            System.out.println(e.getCause());
                            return;
                        }

                        resp.setContentType("application/json");
                        resp.setCharacterEncoding("UTF-8");
                        PrintWriter out = resp.getWriter();
                        out.print("{\"status\":\"Success\", \"message\":\"Enrolled successfull \"}");
                        out.flush();

                    } else {

                        // ERRORE CORSO INESISTENTE
                        resp.setContentType("application/json");
                        resp.setCharacterEncoding("UTF-8");
                        PrintWriter out = resp.getWriter();
                        out.print("{\"status\":\"Error\", \"message\":\"Student already enrolled\"}");
                        out.flush();

                    }

                } else {
                    // ERRORE CORSO INESISTENTE
                    resp.setContentType("application/json");
                    resp.setCharacterEncoding("UTF-8");
                    PrintWriter out = resp.getWriter();
                    out.print("{\"status\":\"Error\", \"message\":\"Course does not exist\"}");
                    out.flush();
                }

            } else {
                // ERRORE UNAUTHORIZED 401
                resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                resp.setContentType("application/json");
                resp.setCharacterEncoding("UTF-8");
                PrintWriter out = resp.getWriter();
                out.print("{\"status\":\"Error\", \"message\":\" Non hai il permesso di eliminare un corso \" }");
                out.flush();
            }

        } else {
            // ERRORE TOKEN NON VALIDO
            resp.setContentType("application/json");
            resp.setCharacterEncoding("UTF-8");
            PrintWriter out = resp.getWriter();
            out.print("{\"status\":\"Error\", \"message\":\"Token non valido. \" }");
            out.flush();
        }

    }
}