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

public class GetUserServlet extends HttpServlet {

    private final UserService userService;

    public GetUserServlet(UserService userService) {
        this.userService = userService;
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        String userId = request.getParameter("userId");

        User user = userService.getUser(userId);

        JSONObject userJson = new JSONObject();

        userJson.put("userId", user.getUserId());
        userJson.put("name", user.getName());
        userJson.put("email", user.getEmail());
        userJson.put("password", user.getPassword());
        userJson.put("role", user.getRole());

        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        PrintWriter out = response.getWriter();
        out.print(userJson.toString());
        out.flush();
    }
}