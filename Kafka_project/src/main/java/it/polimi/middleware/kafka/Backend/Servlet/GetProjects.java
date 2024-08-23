package it.polimi.middleware.kafka.Backend.Servlet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.json.JSONObject;

import it.polimi.middleware.kafka.Backend.Services.CourseService;
import it.polimi.middleware.kafka.Backend.Services.UserService;
import it.polimi.middleware.kafka.Backend.Users.User;
import it.polimi.middleware.kafka.Backend.Course;
import it.polimi.middleware.kafka.Backend.Project;

public class GetProjects extends HttpServlet {

    private final CourseService courseService;

    public GetProjects(CourseService courseService) {
        this.courseService = courseService;
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse resp)
            throws ServletException, IOException {
        /*
         * Product product = productService.find(request.getParameter("id"));
         * request.setAttribute("product", product); // Will be available as ${product}
         * in JSP
         * request.getRequestDispatcher("/WEB-INF/product.jsp").forward(request,
         * response);
         */

        JSONObject jsonResponse = new JSONObject();

        StringBuilder sb = new StringBuilder();
        String line;
        Course course;
        while ((line = request.getReader().readLine()) != null) {
            sb.append(line);
        }

        String jsonString = sb.toString();

        // Crea un oggetto User dal JSON
        JSONObject json = new JSONObject(jsonString);

        String courseId = json.getString("courseId");

        course = courseService.getCourse(courseId);

        if (course != null && !(course.isDeleted())) {

            for (Project project : course.getProjects()) {

                JSONObject projectJson = new JSONObject();
                projectJson.put("Project Id", project.getProjectId());
                projectJson.put("Course of", project.getCourseId());
                projectJson.put("Prof By", project.getProfId());
                jsonResponse.put(project.getProjectId(), projectJson);
            }

            resp.setContentType("application/json");
            resp.setCharacterEncoding("UTF-8");
            PrintWriter out = resp.getWriter();
            out.print(jsonResponse.toString());
            out.flush();

        } else {
            // ERRORE CORSO INESISTENTE
            resp.setContentType("application/json");
            resp.setCharacterEncoding("UTF-8");
            PrintWriter out = resp.getWriter();
            out.print("{\"status\":\"Error\", \"message\":\"Course does not exist\"}");
            out.flush();
        }

    }
}