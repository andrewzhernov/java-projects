package ru.fizteh.fivt.students.andrewzhernov.database;

public interface Handler {
    Object execute(TableManager database, String[] args) throws Exception;
    void handle(Object object) throws Exception;
}
