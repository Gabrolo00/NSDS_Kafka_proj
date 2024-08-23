package it.polimi.middleware.kafka.Backend.Users;

import it.polimi.middleware.kafka.Backend.Course;

public class Professor extends User {
    public Professor(String userId, String name, String email, String password) {
        super(userId, name, email, password, "PROFESSOR");
    }
}
