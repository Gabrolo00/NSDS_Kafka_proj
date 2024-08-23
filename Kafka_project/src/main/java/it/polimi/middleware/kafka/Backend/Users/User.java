package it.polimi.middleware.kafka.Backend.Users;

public class User {
    private String userId;
    private String name;
    private String email;
    private String password;
    private String role;
    private boolean login = false;

    public User(String userId, String name, String email, String password, String role) {
        this.userId = userId;
        this.name = name;
        this.email = email;
        this.password = password;
        this.role = role;
    }

    // Getters e Setters
    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public boolean getLogin() {
        return login;
    }

    public void setLogin() {
        this.login = true;
    }

    public void setLogout() {
        this.login = false;
    }

    @Override
    public String toString() {
        return userId + "," + name + "," + email + "," + password + "," + role;
    }

    public static User fromString(String userString) {
        String[] parts = userString.split(",");
        return new User(parts[0], parts[1], parts[2], parts[3], parts[4]);
    }
}
