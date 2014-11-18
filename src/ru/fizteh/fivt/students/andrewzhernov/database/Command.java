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

    public void execute(TableManager database, String[] params) throws Exception {
        if (params.length != numArgs) {
            throw new Exception(String.format("Invalid number of arguments: %d expected, %d found.",
                                              numArgs, params.length));
        } else {
            processor.handle(processor.execute(database, params));
        }
    }
}
