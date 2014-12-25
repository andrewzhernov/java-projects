package ru.fizteh.fivt.students.andrewzhernov.database;

public interface Handler {
    Object execute(TableProvider database, String[] args) throws Exception;
    void handle(TableProvider database, Object object) throws Exception;
}
