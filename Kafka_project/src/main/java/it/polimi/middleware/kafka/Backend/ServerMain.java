package it.polimi.middleware.kafka.Backend;

import java.io.PrintWriter;
import java.util.concurrent.CountDownLatch;

import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import it.polimi.middleware.kafka.Backend.Services.UserService;
import it.polimi.middleware.kafka.Backend.Consumer.CourseConsumer;
import it.polimi.middleware.kafka.Backend.Consumer.ProjectConsumer;
import it.polimi.middleware.kafka.Backend.Consumer.UserConsumer;
import it.polimi.middleware.kafka.Backend.Services.CourseService;
import it.polimi.middleware.kafka.Backend.Services.ProjectService;
import it.polimi.middleware.kafka.Backend.Services.RegistrationService;
import it.polimi.middleware.kafka.Backend.Servlet.CheckProjectServlet;
import it.polimi.middleware.kafka.Backend.Servlet.Close;
import it.polimi.middleware.kafka.Backend.Servlet.CreateCourseServlet;
import it.polimi.middleware.kafka.Backend.Servlet.CreateProjectServlet;
import it.polimi.middleware.kafka.Backend.Servlet.DeleteCourseServlet;
import it.polimi.middleware.kafka.Backend.Servlet.EnrollCourseServlet;
import it.polimi.middleware.kafka.Backend.Servlet.GetCourses;
import it.polimi.middleware.kafka.Backend.Servlet.GetEnrolledCourses;
import it.polimi.middleware.kafka.Backend.Servlet.GetProjects;
import it.polimi.middleware.kafka.Backend.Servlet.GetRegisteredUsersServlet;
import it.polimi.middleware.kafka.Backend.Servlet.GetUserServlet;
import it.polimi.middleware.kafka.Backend.Servlet.LoginServlet;
import it.polimi.middleware.kafka.Backend.Servlet.RateProjectServlet;
import it.polimi.middleware.kafka.Backend.Servlet.RegisterServlet;
import it.polimi.middleware.kafka.Backend.Servlet.SubmitProjectServlet;
import it.polimi.middleware.kafka.Backend.Users.Admin;
import it.polimi.middleware.kafka.Frontend.Menu;

public class ServerMain {

    final static UserService userService = new UserService();
    final static CourseService courseService = new CourseService();
    final static ProjectService projectService = new ProjectService();
    final static RegistrationService registrationService = new RegistrationService();
    private static CountDownLatch latch = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {

        Thread serverThread = new Thread(() -> {
            try {
                startServer();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        serverThread.start();

        // Start CLI menu in the main thread

        latch.await();

        Menu.runCLI(userService, courseService, projectService);

    }

    public static void startServer() throws Exception {
        // creazione server per api
        Server server = new Server(8081); // Crea un server in ascolto sulla porta 8081

        // creazione consumer e start thread
        UserConsumer userConsumer = new UserConsumer(userService, "localhost:9092", "groupUser2");

        userConsumer.start();
        addAdmin();

        CourseConsumer courseConsumer = new CourseConsumer(userService,
                courseService, registrationService,
                "localhost:9092", "groupCourse2");
        courseConsumer.start();

        ProjectConsumer projectConsumer = new ProjectConsumer(courseService,
                projectService, userService, "localhost:9092",
                "groupProject2");
        projectConsumer.start();

        ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.SESSIONS);
        handler.setContextPath("/");

        // Aggiungi servlet per la rotta /register
        handler.addServlet(new ServletHolder(new RegisterServlet(userService)), "/register");
        // Aggiungi servlet per la rotta /login
        handler.addServlet(new ServletHolder(new LoginServlet(userService)), "/login");

        // Aggiungi servlet per tornare uno user in base allo userId
        handler.addServlet(new ServletHolder(new GetUserServlet(userService)), "/user");

        // close app -> fa il commit di tutti i log e chiude il consumer
        handler.addServlet(new ServletHolder(new Close(userConsumer)), "/close");

        // Aggiunge un corso da parte di un Admin
        handler.addServlet(new ServletHolder(new CreateCourseServlet(userService, courseService)), "/add-course");
        // Elimina un corso da parte di un Admin
        handler.addServlet(new ServletHolder(new DeleteCourseServlet(userService, courseService)), "/delete-course");

        // Iscrive uno studente a un corso
        handler.addServlet(new ServletHolder(new EnrollCourseServlet(userService, courseService)), "/enroll-course");

        // Crea progetto in un corso
        handler.addServlet(new ServletHolder(new CreateProjectServlet(userService, courseService, projectService)),
                "/add-project");

        // Submit project da studente
        handler.addServlet(
                new ServletHolder(
                        new SubmitProjectServlet(userService, courseService, projectService, registrationService)),
                "/submit-project");

        // Rate project da studente
        handler.addServlet(new ServletHolder(new RateProjectServlet(userService, courseService, projectService)),
                "/rate-project");

        // Check project da studente
        handler.addServlet(new ServletHolder(new CheckProjectServlet(userService, courseService, projectService)),
                "/check-project");

        // GET

        // Fa la get degli utenti registrati
        handler.addServlet(new ServletHolder(new GetRegisteredUsersServlet(userService)), "/registered-users");

        // Fa la get dei Corsi
        handler.addServlet(new ServletHolder(new GetCourses(courseService)), "/registered-courses");

        // Fa la get dei Corsi di uno studente
        handler.addServlet(new ServletHolder(new GetEnrolledCourses(userService, registrationService)),
                "/student-courses");

        // Fa la get dei rpogetti di un corso
        handler.addServlet(new ServletHolder(new GetProjects(courseService)), "/get-projects");

        // Attendi qualche secondo per permettere al consumatore di elaborare l'evento
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        server.setHandler(handler);
        server.start();

        latch.countDown();

        server.join();
    }

    public static void addAdmin() {

        Admin adm1 = new Admin("001", "adm1", "adm1@gmail.com", "123");
        Admin adm2 = new Admin("002", "adm2", "adm2@gmail.com", "123");

        Producer producer = new Producer("localhost:9092", true);

        producer.getProducer().initTransactions();

        try {

            producer.getProducer().beginTransaction();

            producer.sendUserRegistration(adm1);
            producer.sendUserRegistration(adm2);

            producer.getProducer().commitTransaction();
            producer.getProducer().close();
        } catch (Exception e) {
            producer.getProducer().abortTransaction();
            e.printStackTrace();

            System.out.println("Errore aggiunta admin");
            return;
        }
        // Rispondi al client
        System.out.println("Aggiunti admin");
    }

}