package ru.fizteh.fivt.students.andrewzhernov.database;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class Shell {
    private static final String PROMPT = "$ ";
    private static final String STATEMENT_DELIMITER = ";";
    private static final String PARAM_DELIMITER = "\\s+";

    private TableProvider manager;
    private Map<String, Command> commands;

    public Shell(TableProvider manager, Command[] commands) throws Exception {
        this.manager = manager;
        this.commands = new HashMap<>();
        for (Command command : commands) {
            this.commands.put(command.getName(), command);
        }
    }

    public void run(String[] args) throws Exception {
        if (args == null) {
            throw new IllegalArgumentException("Invalid commands");
        }
        if (args.length == 0) {
            interactiveMode();
        } else {
            batchMode(args);
        }
    }

    public void interactiveMode() {
        Scanner input = new Scanner(System.in);
        while (true) {
            System.out.print(PROMPT);
            try {
                if (!input.hasNextLine()) {
                    break;
                }
                executeLine(input.nextLine());
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }
        input.close(); 
    }

    public void batchMode(String[] args) throws Exception {
        executeLine(Utils.join(";", args));
    }

    public void executeLine(String line) throws Exception {
        String[] statements = line.split(STATEMENT_DELIMITER);
        for (String statement : statements) {
            String[] params = statement.trim().split(PARAM_DELIMITER);

            String cmdName = params[0];
            for (int i = 1; i < params.length && commands.get(cmdName) == null; ++i) {
                cmdName += " " + params[i];
            }

            Command command = commands.get(cmdName);
            if (command == null) {
                if (!cmdName.isEmpty()) {
                    throw new IllegalArgumentException(cmdName + ": command not found");
                }
            } else {
                command.execute(manager, params);
            }
        }
    }
}
