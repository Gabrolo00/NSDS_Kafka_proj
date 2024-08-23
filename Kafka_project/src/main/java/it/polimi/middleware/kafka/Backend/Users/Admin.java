package it.polimi.middleware.kafka.Backend.Users;

public class Admin extends User {

    public Admin(String userId, String name, String email, String password) {
        super(userId, name, email, password, "ADMIN");
    }
}
