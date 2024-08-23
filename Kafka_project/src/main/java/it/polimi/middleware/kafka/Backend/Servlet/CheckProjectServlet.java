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

public class CheckProjectServlet extends HttpServlet {

    private final UserService userService;
    private final CourseService courseService;
    private final ProjectService projectService;

    public CheckProjectServlet(UserService userService, CourseService courseService, ProjectService projectService) {
        this.userService = userService;
        this.courseService = courseService;
        this.projectService = projectService;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        // Leggi il corpo della richiesta

        String token = req.getHeader("Authorization");
        String studentId = JwtUtil.validateToken(token);

        System.out.println(studentId);

        if (studentId != null) { // check auth valido

            if (userService.checkRole(studentId, "STUDENT")) { // check ruolo = STUDENT

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

                Student student = (Student) userService.getUser(studentId);
                Course course = courseService.getCourse(courseId);

                if (course != null) { // controllo se il corso è inesistente

                    if (student.checkCourseExist(course)) { // controllo se lo studente è iscritto al corso

                        project = course.getProject(projectId);

                        if (project != null) { // controllo se il progetto esiste

                            Integer vote = project.getVote(studentId);

                            if (vote == -2) {

                                resp.setContentType("application/json");
                                resp.setCharacterEncoding("UTF-8");
                                PrintWriter out = resp.getWriter();
                                out.print("{\"status\":\"success\", \"message\":\"Project has not submitted yet. \"}");
                                out.flush();
                                return;

                            } else if (vote == -1) {

                                resp.setContentType("application/json");
                                resp.setCharacterEncoding("UTF-8");
                                PrintWriter out = resp.getWriter();
                                out.print("{\"status\":\"success\", \"message\":\"Project awaiting evaluation. \"}");
                                out.flush();
                                return;

                            } else {

                                resp.setContentType("application/json");
                                resp.setCharacterEncoding("UTF-8");
                                PrintWriter out = resp.getWriter();
                                out.print("{\"status\":\"success\", \"message\":\"Project rating: " + vote + "\"}");
                                out.flush();
                                return;
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
                // ERRORE UNAUTHORIZED 401
                resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                resp.setContentType("application/json");
                resp.setCharacterEncoding("UTF-8");
                PrintWriter out = resp.getWriter();
                out.print("{\"status\":\"Error\", \"message\":\" You have not authorize \" }");
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