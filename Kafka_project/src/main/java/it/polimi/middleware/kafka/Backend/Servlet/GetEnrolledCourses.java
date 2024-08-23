package it.polimi.middleware.kafka.Backend.Servlet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.json.JSONObject;

import it.polimi.middleware.kafka.Backend.Services.CourseService;
import it.polimi.middleware.kafka.Backend.Services.JwtUtil;
import it.polimi.middleware.kafka.Backend.Services.RegistrationService;
import it.polimi.middleware.kafka.Backend.Services.UserService;
import it.polimi.middleware.kafka.Backend.Users.User;
import it.polimi.middleware.kafka.Backend.Users.Student;
import it.polimi.middleware.kafka.Backend.Course;

public class GetEnrolledCourses extends HttpServlet {

    private final UserService userService;
    private final RegistrationService registrationService;

    public GetEnrolledCourses(UserService userService, RegistrationService registrationService) {
        this.userService = userService;
        this.registrationService = registrationService;
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        // Leggi il corpo della richiesta
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = request.getReader().readLine()) != null) {
            sb.append(line);
        }
        String jsonString = sb.toString();

        // Crea un oggetto User dal JSON
        JSONObject json = new JSONObject(jsonString);
        User user = userService.getUser(json.getString("userId"));

        if (user != null) {

            if (userService.checkRole(user.getUserId(), "STUDENT")) {
                List<Course> courses = new ArrayList<Course>();

                Student s = (Student) user;

                courses = s.getCourses();

                JSONObject responseJson = new JSONObject();
                for (Course entry : courses) {
                    if (!entry.isDeleted()) {

                        if (registrationService.getCompletedMap().containsKey(user.getUserId())) {

                            List<String> completedCourses = registrationService.getCompletedMap().get(user.getUserId());

                            if (completedCourses != null && completedCourses.contains(entry.getCourseId())) {

                                JSONObject courseJson = new JSONObject();
                                courseJson.put("Course Id", entry.getCourseId());
                                courseJson.put("Course Name", entry.getName());
                                courseJson.put("Created By", entry.getAdminId());
                                courseJson.put("Status", "COMPLETED");
                                responseJson.put(entry.getCourseId(), courseJson);
                            } else {
                                JSONObject courseJson = new JSONObject();
                                courseJson.put("Course Id", entry.getCourseId());
                                courseJson.put("Course Name", entry.getName());
                                courseJson.put("Created By", entry.getAdminId());
                                courseJson.put("Status", "NOT COMPLETED");
                                responseJson.put(entry.getCourseId(), courseJson);
                            }

                        } else {
                            JSONObject courseJson = new JSONObject();

                            courseJson.put("Course Id", entry.getCourseId());
                            courseJson.put("Course Name", entry.getName());
                            courseJson.put("Created By", entry.getAdminId());
                            courseJson.put("Status", "NOT COMPLETED");
                            responseJson.put(entry.getCourseId(), courseJson);
                        }

                    }

                }

                response.setContentType("application/json");
                response.setCharacterEncoding("UTF-8");
                PrintWriter out = response.getWriter();
                out.print(responseJson.toString());
                out.flush();
            }

        } else {
            // Rispondi al client
            response.setContentType("application/json");
            response.setCharacterEncoding("UTF-8");
            PrintWriter out = response.getWriter();
            out.print("{\"status\":\"error\", \"message\":\"User do not exist.\"}");
            out.flush();
        }

    }
}