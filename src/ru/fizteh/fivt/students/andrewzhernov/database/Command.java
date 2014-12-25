package ru.fizteh.fivt.students.andrewzhernov.database;

public class Command {
    private String name;
    private int numArgs;
    private Handler processor;

    public Command(String name, int numArgs, Handler processor) {
        this.name = name;
        this.numArgs = numArgs;
        this.processor = processor;
    }

    public String getName() {
        return name;
    }

    public void execute(TableProvider database, String[] params) throws Exception {
        if (params.length != numArgs) {
            throw new IllegalArgumentException(String.format(name + ": invalid number of arguments: %d expected, %d found.",
                                              numArgs, params.length));
        } else {
            processor.handle(database, processor.execute(database, params));
        }
    }
}
