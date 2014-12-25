package ru.fizteh.fivt.students.andrewzhernov.database;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.DirectoryStream;
import java.util.ArrayList;
import java.util.List;

public class Utils {
    public static void removeDir(Path directory) throws IllegalStateException {
        try {
            if (Files.isDirectory(directory)) {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
                    for (Path entry : stream) {
                        removeDir(entry);
                    }
                }
            }
            if (!directory.toFile().delete()) {
                throw new IllegalStateException("Can't remove " + directory.toString());
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
    
    // for java 7
    public static String join(String join, String... strings) {
        if (strings == null || strings.length == 0) {
            return "";
        } else if (strings.length == 1) {
            return strings[0];
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(strings[0]);
            for (int i = 1; i < strings.length; i++) {
                sb.append(join).append(strings[i]);
            }
            return sb.toString();
        }
    }
    
    public static List<Class<?>> parseSignature(String signature) throws Exception {
        if (signature.length() < 2) {
            throw new Exception("Wrong signature length"); 
        }
        if (signature.charAt(0) != '(' || signature.charAt(signature.length() - 1) != ')') {
            throw new Exception("Signature must start and end with paranthesis");
        }
        
        signature = signature.substring(1, signature.length() - 1);
        
        String[] types = signature.split(",");
        
        List<Class<?>> result = new ArrayList<>();
        for (String type : types) {
            if (type.equals("int")) {
              result.add(Integer.class);
            } else if (type.equals("long")) {
              result.add(Long.class);
            } else if (type.equals("byte")) {
              result.add(Byte.class);
            } else if (type.equals("float")) {
              result.add(Float.class);
            } else if (type.equals("double")) {
              result.add(Double.class);
            } else if (type.equals("boolean")) {
              result.add(Boolean.class);
            } else if (type.equals("String")) {
              result.add(String.class);
            } else {
            	throw new Exception("Wrong type (" + type + ")");
            }
        }
        
        return result;
    }
    
    public static String makeSignature(List<Class<?>> types) {
    	String result = "(";

    	int i = 0;
        for (Class<?> type : types) {
            if (type.equals(Integer.class)) {
              result += "int";
            } else if (type.equals(Long.class)) {
                result += "long";
            } else if (type.equals(Byte.class)) {
                result += "byte";
            } else if (type.equals(Float.class)) {
                result += "float";
            } else if (type.equals(Double.class)) {
                result += "double";
            } else if (type.equals(Boolean.class)) {
                result += "boolean";
            } else if (type.equals(String.class)) {
                result += "String";
            }
            
            i++;
            if (i != types.size()) {
            	result += ",";
            }
        }
        
        result += ")";
        return result;
    }
    
}
