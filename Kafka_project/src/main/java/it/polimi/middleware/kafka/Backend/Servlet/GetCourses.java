package it.polimi.middleware.kafka.Backend.Servlet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.json.JSONObject;

import it.polimi.middleware.kafka.Backend.Services.CourseService;
import it.polimi.middleware.kafka.Backend.Services.UserService;
import it.polimi.middleware.kafka.Backend.Users.User;
import it.polimi.middleware.kafka.Backend.Course;

public class GetCourses extends HttpServlet {

    private final CourseService courseService;

    public GetCourses(CourseService courseService) {
        this.courseService = courseService;
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        /*
         * Product product = productService.find(request.getParameter("id"));
         * request.setAttribute("product", product); // Will be available as ${product}
         * in JSP
         * request.getRequestDispatcher("/WEB-INF/product.jsp").forward(request,
         * response);
         */

        Map<String, Course> courseMap = new ConcurrentHashMap<>();

        courseMap = courseService.getAllCourse();

        JSONObject jsonResponse = new JSONObject();
        for (Map.Entry<String, Course> entry : courseMap.entrySet()) {
            if (!(entry.getValue().isDeleted())) {
                Course course = entry.getValue();
                JSONObject courseJson = new JSONObject();
                courseJson.put("Course Id", course.getCourseId());
                courseJson.put("Course Name", course.getName());
                courseJson.put("Admin By", course.getAdminId());
                jsonResponse.put(entry.getKey(), courseJson);
            }
        }

        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        PrintWriter out = response.getWriter();
        out.print(jsonResponse.toString());
        out.flush();
    }
}