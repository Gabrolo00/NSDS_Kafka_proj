package it.polimi.middleware.kafka.Backend.Servlet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import it.polimi.middleware.kafka.Backend.Consumer.UserConsumer;

import java.io.IOException;
import java.io.PrintWriter;

public class Close extends HttpServlet {

    private UserConsumer consumer;

    public Close(UserConsumer consumer) {
        this.consumer = consumer;
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

        try {

            consumer.shutdown();
            response.setContentType("application/json");
            response.setCharacterEncoding("UTF-8");
            PrintWriter out = response.getWriter();
            out.print("{\"status\":\"Success\", \"message\":\"Chiusura applicazione\"}");
            out.flush();

        } catch (Exception e) {

            response.setContentType("application/json");
            response.setCharacterEncoding("UTF-8");
            PrintWriter out = response.getWriter();
            out.print(
                    "{\"status\":\"Error\", \"message\":\"Errore durante chiusura consumer. Chiusura applicazione in corso. \""
                            + e.getMessage() + "\"}");
            out.flush();

        }

    }
}