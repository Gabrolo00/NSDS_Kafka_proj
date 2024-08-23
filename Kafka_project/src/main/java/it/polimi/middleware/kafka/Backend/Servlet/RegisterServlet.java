package it.polimi.middleware.kafka.Backend.Servlet;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.PrintWriter;

import org.json.JSONObject;

import it.polimi.middleware.kafka.Backend.Producer;
import it.polimi.middleware.kafka.Backend.Services.UserService;
import it.polimi.middleware.kafka.Backend.Users.Professor;
import it.polimi.middleware.kafka.Backend.Users.User;
import it.polimi.middleware.kafka.Backend.Users.Student;

public class RegisterServlet extends HttpServlet {

    private UserService userService;

    public RegisterServlet(UserService userService) {
        this.userService = userService;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        // Leggi il corpo della richiesta
        StringBuilder sb = new StringBuilder();
        String line;
        User user;
        while ((line = req.getReader().readLine()) != null) {
            sb.append(line);
        }
        String jsonString = sb.toString();

        // Crea un oggetto User dal JSON
        JSONObject json = new JSONObject(jsonString);

        if (json.getString("role").equals("PROFESSOR")) {
            user = new Professor(json.getString("userId"), json.getString("name"),
                    json.getString("email"),
                    json.getString("password"));
        } else {
            user = new Student(json.getString("userId"), json.getString("name"), json.getString("email"),
                    json.getString("password"));
        }

        // Invia l'evento di registrazione a Kafka
        Producer producer = new Producer("localhost:9092", true);

        if (userService.getUser(user.getUserId()) == null && userService.checkRegister(user)) {
            producer.getProducer().initTransactions();

            try {

                producer.getProducer().beginTransaction();
                producer.sendUserRegistration(user);

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
                out.print("{\"status\":\"error\", \"message\":\"Errore durante la registrazione\"}");
                out.flush();
                return;
            }
            // Rispondi al client
            resp.setContentType("application/json");
            resp.setCharacterEncoding("UTF-8");
            PrintWriter out = resp.getWriter();
            out.print("{\"status\":\"success\", \"message\":\"User registered successfully.\"}");
            out.flush();
        } else {
            // Rispondi al client
            resp.setContentType("application/json");
            resp.setCharacterEncoding("UTF-8");
            PrintWriter out = resp.getWriter();
            out.print("{\"status\":\"error\", \"message\":\"User already registered.\"}");
            out.flush();
        }

    }
}