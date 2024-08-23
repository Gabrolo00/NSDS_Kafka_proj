package it.polimi.middleware.kafka.Backend.Servlet;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.PrintWriter;

import org.json.JSONObject;

import it.polimi.middleware.kafka.Backend.Services.JwtUtil;
import it.polimi.middleware.kafka.Backend.Services.UserService;
import it.polimi.middleware.kafka.Backend.Users.User;

public class LoginServlet extends HttpServlet {

    private UserService userService;

    public LoginServlet(UserService userService) {
        this.userService = userService;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        // Leggi il corpo della richiesta
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = req.getReader().readLine()) != null) {
            sb.append(line);
        }
        String jsonString = sb.toString();

        // Crea un oggetto User dal JSON
        JSONObject json = new JSONObject(jsonString);
        User user = userService.getUser(json.getString("userId"));

        if (user != null) {

            System.out.println("password: " + user.getPassword());

            if (userService.authenticateUser(json.getString("userId"), json.getString("password"))) {

                user.setLogin();
                String token = JwtUtil.generateToken(user.getUserId());

                resp.setContentType("application/json");
                resp.setCharacterEncoding("UTF-8");
                PrintWriter out = resp.getWriter();
                out.print("{\"status\":\"success\", \"message\":\"Login effettuato con successo\", \"Token\":\"" + token
                        + " \" " + "}");
                out.flush();
            } else {
                resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                resp.setContentType("application/json");
                resp.setCharacterEncoding("UTF-8");
                PrintWriter out = resp.getWriter();
                out.print("{\"status\":\"error\", \"message\":\"Password errata.\"}");
                out.flush();
            }

        } else {
            // Rispondi al client
            resp.setContentType("application/json");
            resp.setCharacterEncoding("UTF-8");
            PrintWriter out = resp.getWriter();
            out.print("{\"status\":\"error\", \"message\":\"User do not exist.\"}");
            out.flush();
        }

    }
}