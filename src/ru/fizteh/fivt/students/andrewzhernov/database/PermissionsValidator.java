package ru.fizteh.fivt.students.andrewzhernov.database;

import java.util.EnumSet;
import static ru.fizteh.fivt.students.andrewzhernov.database.PermissionsValidator.Permissions.*;
import java.io.File;
import java.nio.file.Files;
import java.io.IOException;

public class PermissionsValidator {
    public enum Permissions {
        NOT_NULL,
        EXISTS,
        CREATE_FILE_IF_NOT_EXISTS,
        CREATE_DIRECTORY_IF_NOT_EXISTS,
        CAN_READ,
        CAN_WRITE,
        IS_FILE,
        IS_DIRECTORY;
    }

    public static boolean validateTableName(String tableName, EnumSet<Permissions> perms) 
            throws IllegalArgumentException {
        if (perms.contains(NOT_NULL) && tableName == null) {
            throw new IllegalArgumentException("Table name hasn't been specified");
        }
        return true;
    }

    public static boolean validateDbFolder(String dbFolder, EnumSet<Permissions> perms)
            throws IllegalArgumentException {
        if (perms.contains(NOT_NULL) && dbFolder == null) {
            throw new IllegalArgumentException("Database name hasn't been specified");
        }
        return true;
    }

    public static boolean validate(String fileName, EnumSet<Permissions> perms)
            throws IllegalArgumentException {
        if (perms.contains(NOT_NULL) && fileName == null) {
            throw new IllegalArgumentException("File name hasn't been specified");
        }
        File file = new File(fileName);
        if (perms.contains(EXISTS) && !file.exists()) {
            return false;
        }
        if ((perms.contains(CREATE_FILE_IF_NOT_EXISTS)
                    || perms.contains(CREATE_DIRECTORY_IF_NOT_EXISTS))
                    && !file.exists()) {
            try {
                File parentFile = file.getCanonicalFile().getParentFile();
                if (!parentFile.canWrite()) {
                    throw new IllegalArgumentException(fileName
                            + ": don't have permission to create the directory or normal file");
                }
                if (perms.contains(CREATE_FILE_IF_NOT_EXISTS)) {
                    Files.createFile(file.toPath());
                } else {
                    Files.createDirectory(file.toPath());
                }
            } catch (IOException e) {
                System.out.println(file.toString() + ": Input-Output exception");
            }
        }
        if (perms.contains(CAN_READ) && !file.canRead()) {
            throw new IllegalArgumentException(file.getPath()
                    + ": don't have permission to read the file");
        }
        if (perms.contains(CAN_WRITE) && !file.canWrite()) {
            throw new IllegalArgumentException(file.getPath()
                    + ": don't have permission to write the file");
        }
        if (perms.contains(IS_FILE) && !file.isFile()) {
            throw new IllegalArgumentException(file.getPath() + ": isn't a normal file");
        }
        if (perms.contains(IS_DIRECTORY) && !file.isDirectory()) {
            throw new IllegalArgumentException(file.getPath() + ": isn't a directory");
        }
        return true;
    }
}
