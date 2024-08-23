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

import it.polimi.middleware.kafka.Backend.Services.UserService;
import it.polimi.middleware.kafka.Backend.Users.User;

public class GetRegisteredUsersServlet extends HttpServlet {

    private final UserService userService;

    public GetRegisteredUsersServlet(UserService userService) {
        this.userService = userService;
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

        Map<String, User> userMap = new ConcurrentHashMap<>();

        userMap = userService.getAllUser();

        JSONObject jsonResponse = new JSONObject();
        for (Map.Entry<String, User> entry : userMap.entrySet()) {

            JSONObject userJson = new JSONObject();
            User user = entry.getValue();
            userJson.put("userId", user.getUserId());
            userJson.put("name", user.getName());
            userJson.put("email", user.getEmail());
            userJson.put("password", user.getPassword());
            userJson.put("role", user.getRole());
            jsonResponse.put(entry.getKey(), userJson);
        }

        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        PrintWriter out = response.getWriter();
        out.print(jsonResponse.toString());
        out.flush();
    }
}