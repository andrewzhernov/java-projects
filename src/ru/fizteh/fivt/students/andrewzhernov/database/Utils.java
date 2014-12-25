package ru.fizteh.fivt.students.andrewzhernov.database;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.DirectoryStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class Utils {
    private static Map<Class<?>, String> ctsMap = new HashMap<>();
    private static Map<String, Class<?>> stcMap = new HashMap<>();

    static {
        ctsMap.put(Integer.class, "int");
        ctsMap.put(Long.class, "long");
        ctsMap.put(Float.class, "float");
        ctsMap.put(Double.class, "double");
        ctsMap.put(Byte.class, "byte");
        ctsMap.put(Boolean.class, "boolean");
        ctsMap.put(String.class, "String");

        stcMap.put("int", Integer.class);
        stcMap.put("long", Long.class);
        stcMap.put("float", Float.class);
        stcMap.put("double", Double.class);
        stcMap.put("byte", Byte.class);
        stcMap.put("boolean", Boolean.class);
        stcMap.put("String", String.class);
    }

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
        
        List<Class<?>> result = new LinkedList<>();
        for (String type : types) {
            if (stcMap.containsKey(type)) {
                result.add(stcMap.get(type));
            } else {
            	throw new Exception("Wrong type (" + type + ")");
            }
        }
        
        return result;
    }
    
    public static String makeSignature(List<Class<?>> types) {
    	String result = "(";

        List<String> typeList = new LinkedList<>();
    	int i = 0;
        for (Class<?> type : types) {
            if (ctsMap.containsKey(type)) {
                typeList.add(ctsMap.get(type));
            }
            i++;
        }
        
        result += String.join(",", typeList) + ")";
        return result;
    }
    
}
