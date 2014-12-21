package ru.fizteh.fivt.students.andrewzhernov.database;

import java.io.*;
import java.nio.file.Paths;
import java.lang.Long;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;

import static java.util.EnumSet.of;
import static ru.fizteh.fivt.students.andrewzhernov.database.PermissionsValidator.Permissions.*;

public class HashMapTableManager implements TableManager {
    private static final String[] invalidCharacters = {".", "|", "\\", "*", "\"", "\'", ":", "/", "?", "<", ">"};
    private static final List<Class<?>> types = Arrays.asList(new Integer().getClass(), new Long().getClass(), Byte, Float, Double, Boolean, String);

    private String dbFolder;
    private Map<String, Table> nameToTableMap;
    private String currentTable;

    public HashMapTableManager(String folder) throws IllegalArgumentException {
        PermissionsValidator.validateDbFolder(folder, of(NOT_NULL));
        PermissionsValidator.validate(folder, of(CREATE_DIRECTORY_IF_NOT_EXISTS, CAN_READ, IS_DIRECTORY));
        dbFolder = folder;
        nameToTableMap = new HashMap<>();
        currentTable = null;
        loadDatabase();
    }

    private boolean isValidName(String tableName) {
        for (String character : invalidCharacters) {
            if (tableName.contains(character)) {
                return false;
            }
        }
        return true;
    }

    private void loadDatabase() throws IllegalArgumentException {
        for (String tableName : new File(dbFolder).list()) {
            if (!isValidName(tableName)) {
                throw new IllegalArgumentException(tableName + ": incorrect table name");
            }
            Table table = new HashMapTable(this, tableName);
            nameToTableMap.put(tableName, table);
            table.loadTable();
        }
    }

    private void checkUnsavedChanges() throws IllegalStateException {
        int count = getCurrentTable().getNumberOfUncommittedChanges();
        if (count > 0) {
            throw new IllegalStateException(Integer.toString(count) + " unsaved changes");
        }
    }

    public String getDbFolder() {
        return dbFolder;
    }

    public Table getTable(String name) throws IllegalArgumentException {
        if (name == null) {
            throw new IllegalArgumentException("no table");
        }
        return nameToTableMap.get(name);
    }

    public Table getCurrentTable() throws IllegalArgumentException {
        return getTable(currentTable);
    }

    public Table createTable(String name, List<Class<?>> columnTypes) throws IllegalArgumentException {
        if (name == null || !isValidName(name)) {
            throw new IllegalArgumentException(name + ": incorrect table name");
        } else if (nameToTableMap.containsKey(name)) {
            throw new IllegalArgumentException(name + " exists");
        }

        Table table = new Table(this, name);
        nameToTableMap.put(name, table);
        PermissionsValidator.validate(table.getTableFolder(), 
                    of(NOT_NULL, CREATE_DIRECTORY_IF_NOT_EXISTS, CAN_WRITE, IS_DIRECTORY));
        return table;
    }

    public void removeTable(String name) throws IllegalArgumentException, IllegalStateException {
        if (name == null || !isValidName(name)) {
            throw new IllegalArgumentException(name + ": incorrect table name");
        } else if (!nameToTableMap.containsKey(name)) {
            throw new IllegalStateException(name + " not exist");
        } else if (name.equals(currentTable)) {
            currentTable = null;
        }
        Utils.removeDir(Paths.get(dbFolder, name));
        nameToTableMap.remove(name);
    }

    public String useTable(String name) throws IllegalArgumentException, IllegalStateException {
        if (name == null || !isValidName(name)) {
            throw new IllegalArgumentException(name + ": incorrect table name");
        } else if (!nameToTableMap.containsKey(name)) {
            throw new IllegalArgumentException(name + " not exist");
        } else if (currentTable != null && !name.equals(currentTable)) {
            checkUnsavedChanges();
        }
        currentTable = name;
        return currentTable;
    }

    public List<String> getTableNames() {
        return new LinkedList<>().addAll(nameToTableMap.keySet());
    }

    public createFor(Table table) {
            
    }

    public void exit() throws IllegalStateException {
        if (currentTable != null) {
            checkUnsavedChanges();
        }
    }
}
