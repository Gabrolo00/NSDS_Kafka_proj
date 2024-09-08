package it.polimi.middleware.kafka.Frontend;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Scanner;

import org.json.JSONObject;

public class Menu {
    private static final Scanner scanner = new Scanner(System.in);
    private static JSONObject jsonResponse;
    private static String token;
    private static String role;

    public static void main(String[] args) throws Exception {

        Menu.runCLI();

    }

    public static void runCLI() {

        while (true) {
            System.out.println("1. Register User");
            System.out.println("2. Login");
            System.out.println("3. View Courses");
            System.out.println("4. View Registered Users");
            System.out.println("5. Exit");
            System.out.print("Enter your choice: ");
            int choice = scanner.nextInt();
            scanner.nextLine();

            try {
                switch (choice) {
                    case 1:
                        registerUser();
                        break;
                    case 2:
                        loginUser();
                        break;
                    case 3:
                        viewCourses();
                        break;
                    case 4:
                        viewRegisteredUsers();
                        break;
                    case 5:
                        System.exit(0);
                    default:
                        System.out.println("Invalid choice. Try again.");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void registerUser() throws Exception {
        System.out.print("Enter userId: ");
        String userId = scanner.nextLine();
        System.out.print("Enter name: ");
        String name = scanner.nextLine();
        System.out.print("Enter email: ");
        String email = scanner.nextLine();
        System.out.print("Enter password: ");
        String password = scanner.nextLine();
        System.out.print("Enter role (STUDENT/PROFESSOR/ADMIN): ");
        String role = scanner.nextLine();

        String jsonInputString = String.format(
                "{\"userId\":\"%s\", \"name\":\"%s\", \"email\":\"%s\", \"password\":\"%s\", \"role\":\"%s\"}",
                userId, name, email, password, role);

        String response = sendPostRequest("/register", jsonInputString);
        jsonResponse = new JSONObject(response);
        System.out.println(jsonResponse);
    }

    private static void loginUser() throws Exception {
        System.out.print("Enter userId: ");
        String userId = scanner.nextLine();
        System.out.print("Enter password: ");
        String password = scanner.nextLine();

        String jsonInputString = String.format(
                "{\"userId\":\"%s\", \"password\":\"%s\"}",
                userId, password);

        String response = sendPostRequest("/login", jsonInputString);
        jsonResponse = new JSONObject(response);

        if (jsonResponse.getString("status").equals("success")) {

            token = jsonResponse.getString("Token");

            extractRole(userId); // Implement this function to decode the
                                 // JWT and extract the role
            if (role.equals("ADMIN")) {
                adminMenu();
            } else if (role.equals("STUDENT")) {
                studentMenu(userId);
            } else if (role.equals("PROFESSOR")) {
                professorMenu();
            }

        }

    }

    private static void adminMenu() throws Exception {
        while (true) {
            System.out.println("1. Add Course");
            System.out.println("2. Delete Course");
            System.out.println("3. Logout");
            System.out.print("Enter your choice: ");
            int choice = scanner.nextInt();
            scanner.nextLine();

            switch (choice) {
                case 1:
                    addCourse();
                    break;
                case 2:
                    deleteCourse();
                    break;
                case 3:
                    return;
                default:
                    System.out.println("Invalid choice. Try again.");
            }
        }
    }

    private static void studentMenu(String studentId) throws Exception {
        while (true) {
            System.out.println("1. Enroll in Course");
            System.out.println("2. View Enrolled Courses");
            System.out.println("3. Submit project");
            System.out.println("4. Check project");
            System.out.println("5. Logout");
            System.out.print("Enter your choice: ");
            int choice = scanner.nextInt();
            scanner.nextLine();

            switch (choice) {
                case 1:
                    enrollCourse(studentId);
                    break;
                case 2:
                    viewStudentCourses(studentId);
                    break;
                case 3:
                    submitProject(studentId);
                    break;
                case 4:
                    checkProject(studentId);
                    break;
                case 5:
                    return;
                default:
                    System.out.println("Invalid choice. Try again.");
            }
        }
    }

    private static void professorMenu() throws Exception {
        while (true) {
            System.out.println("1. Add Project to Course");
            System.out.println("2. Grade Project");
            System.out.println("3. Logout");
            System.out.print("Enter your choice: ");
            int choice = scanner.nextInt();
            scanner.nextLine();

            switch (choice) {
                case 1:
                    addProject();
                    break;
                case 2:
                    gradeProject();
                    break;
                case 3:
                    return;
                default:
                    System.out.println("Invalid choice. Try again.");
            }
        }
    }

    private static void addCourse() throws Exception {
        System.out.print("Enter courseId: ");
        String courseId = scanner.nextLine();
        System.out.print("Enter courseName: ");
        String courseName = scanner.nextLine();

        String jsonInputString = String.format(
                "{\"courseId\":\"%s\", \"courseName\":\"%s\"}",
                courseId, courseName);

        String response = sendPostRequestWithAuth("/add-course", jsonInputString, token);
        jsonResponse = new JSONObject(response);
        System.out.println(jsonResponse);
    }

    private static void deleteCourse() throws Exception {
        System.out.print("Enter courseId: ");
        String courseId = scanner.nextLine();

        String jsonInputString = String.format(
                "{\"courseId\":\"%s\"}",
                courseId);

        String response = sendPostRequestWithAuth("/delete-course", jsonInputString, token);
        jsonResponse = new JSONObject(response);
        System.out.println(jsonResponse);
    }

    private static void enrollCourse(String studentId) throws Exception {
        System.out.print("Enter courseId: ");
        String courseId = scanner.nextLine();

        String jsonInputString = String.format(
                "{\"userId\":\"%s\", \"courseId\":\"%s\"}",
                studentId, courseId);

        String response = sendPostRequestWithAuth("/enroll-course", jsonInputString, token);
        jsonResponse = new JSONObject(response);
        System.out.println(jsonResponse);
    }

    private static void viewStudentCourses(String studentId) throws Exception {
        String jsonInputString = String.format(
                "{\"userId\":\"%s\"}",
                studentId);

        String response = sendPostRequestWithAuth("/student-courses", jsonInputString, token);
        jsonResponse = new JSONObject(response);
        System.out.println(jsonResponse);

        while (true) {
            System.out.println("1. View project of course");
            System.out.println("2. Exit");
            System.out.print("Enter your choice: ");
            int choice = scanner.nextInt();
            scanner.nextLine();

            try {
                switch (choice) {
                    case 1:
                        viewProjects();
                        break;
                    case 2:
                        return;
                    default:
                        System.out.println("Invalid choice. Try again.");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void submitProject(String studentId) throws Exception {
        System.out.print("Enter courseId: ");
        String courseId = scanner.nextLine();
        System.out.print("Enter projectId: ");
        String projectId = scanner.nextLine();
        System.out.print("Enter allegato: ");
        String allegato = scanner.nextLine();

        String jsonInputString = String.format(
                "{\"courseId\":\"%s\", \"projectId\":\"%s\", \"allegato\":\"%s\"}",
                courseId, projectId, allegato);

        String response = sendPostRequestWithAuth("/submit-project", jsonInputString, token);
        jsonResponse = new JSONObject(response);
        System.out.println(jsonResponse);
    }

    private static void checkProject(String studentId) throws Exception {
        System.out.print("Enter courseId: ");
        String courseId = scanner.nextLine();
        System.out.print("Enter projectId: ");
        String projectId = scanner.nextLine();

        String jsonInputString = String.format(
                "{\"courseId\":\"%s\", \"projectId\":\"%s\"}",
                courseId, projectId);

        String response = sendPostRequestWithAuth("/check-project", jsonInputString, token);
        jsonResponse = new JSONObject(response);
        System.out.println(jsonResponse);
    }

    private static void addProject() throws Exception {
        System.out.print("Enter courseId: ");
        String courseId = scanner.nextLine();
        System.out.print("Enter projectId: ");
        String projectId = scanner.nextLine();

        String jsonInputString = String.format(
                "{\"courseId\":\"%s\", \"projectId\":\"%s\"}",
                courseId, projectId);

        String response = sendPostRequestWithAuth("/add-project", jsonInputString, token);
        jsonResponse = new JSONObject(response);
        System.out.println(jsonResponse);
    }

    private static void gradeProject() throws Exception {
        System.out.print("Enter courseId: ");
        String courseId = scanner.nextLine();
        System.out.print("Enter projectId: ");
        String projectId = scanner.nextLine();
        System.out.print("Enter studentId: ");
        String studentId = scanner.nextLine();
        System.out.print("Enter grade: ");
        String grade = scanner.nextLine();

        String jsonInputString = String.format(
                "{\"courseId\":\"%s\", \"projectId\":\"%s\", \"studentId\":\"%s\", \"grade\":\"%s\"}",
                courseId, projectId, studentId, grade);

        String response = sendPostRequestWithAuth("/rate-project", jsonInputString, token);
        jsonResponse = new JSONObject(response);
        System.out.println(jsonResponse);
    }

    private static void viewCourses() throws Exception {
        sendGetRequest("/registered-courses");

        while (true) {
            System.out.println("1. View project of course");
            System.out.println("2. Exit");
            System.out.print("Enter your choice: ");
            int choice = scanner.nextInt();
            scanner.nextLine();

            try {
                switch (choice) {
                    case 1:
                        viewProjects();
                        break;
                    case 2:
                        return;
                    default:
                        System.out.println("Invalid choice. Try again.");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void viewProjects() throws Exception {

        System.out.print("Enter courseId: ");
        String courseId = scanner.nextLine();

        String jsonInputString = String.format(
                "{\"courseId\":\"%s\"}",
                courseId);
        String response = sendPostRequest("/get-projects", jsonInputString);
        jsonResponse = new JSONObject(response);
        System.out.println(jsonResponse);
    }

    private static void viewRegisteredUsers() throws Exception {
        sendGetRequest("/registered-users");
    }

    private static String sendPostRequest(String urlString, String jsonInputString) throws Exception {
        URL url = new URL("http://localhost:8081" + urlString);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json; utf-8");
        conn.setRequestProperty("Accept", "application/json");
        conn.setDoOutput(true);

        try (OutputStream os = conn.getOutputStream()) {
            byte[] input = jsonInputString.getBytes("utf-8");
            os.write(input, 0, input.length);
        }

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(conn.getInputStream(), "utf-8"))) {
            StringBuilder response = new StringBuilder();
            String responseLine;
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine.trim());
            }
            return response.toString();
        }
    }

    private static String sendPostRequestWithAuth(String urlString, String jsonInputString, String token)
            throws Exception {
        URL url = new URL("http://localhost:8081" + urlString);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json; utf-8");
        conn.setRequestProperty("Accept", "application/json");
        conn.setRequestProperty("Authorization", token);
        conn.setDoOutput(true);

        try (OutputStream os = conn.getOutputStream()) {
            byte[] input = jsonInputString.getBytes("utf-8");
            os.write(input, 0, input.length);
        }

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(conn.getInputStream(), "utf-8"))) {
            StringBuilder response = new StringBuilder();
            String responseLine;
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine.trim());
            }
            return response.toString();
        }
    }

    private static void sendGetRequest(String urlString) throws Exception {
        URL url = new URL("http://localhost:8081" + urlString);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Accept", "application/json");

        try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), "utf-8"))) {
            StringBuilder response = new StringBuilder();
            String responseLine;

            if (!urlString.contains("?")) {
                while ((responseLine = br.readLine()) != null) {
                    response.append(responseLine.trim());
                }
            } else {
                while ((responseLine = br.readLine()) != null) {
                    response.append(responseLine.trim());
                }

                System.out.println(response.toString());
                String jsonResponseString = response.toString();

                JSONObject jsonResponse = new JSONObject(jsonResponseString);

                System.out.println(jsonResponseString);

                role = jsonResponse.getString("role");
            }

        }
    }

    private static void extractRole(String userId) throws Exception {

        sendGetRequest("/user?userId=" + userId);
    }
}
