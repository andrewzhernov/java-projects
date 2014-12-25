package ru.fizteh.fivt.students.andrewzhernov.database;

import static java.util.EnumSet.of;
import static ru.fizteh.fivt.students.andrewzhernov.database.PermissionsValidator.Permissions.CAN_READ;
import static ru.fizteh.fivt.students.andrewzhernov.database.PermissionsValidator.Permissions.CAN_WRITE;
import static ru.fizteh.fivt.students.andrewzhernov.database.PermissionsValidator.Permissions.CREATE_DIRECTORY_IF_NOT_EXISTS;
import static ru.fizteh.fivt.students.andrewzhernov.database.PermissionsValidator.Permissions.EXISTS;
import static ru.fizteh.fivt.students.andrewzhernov.database.PermissionsValidator.Permissions.IS_DIRECTORY;
import static ru.fizteh.fivt.students.andrewzhernov.database.PermissionsValidator.Permissions.IS_FILE;
import static ru.fizteh.fivt.students.andrewzhernov.database.PermissionsValidator.Permissions.NOT_NULL;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class HashMapTableProvider implements TableProvider {
    private static final String[] invalidCharacters = {".", "|", "\\", "*", "\"", "\'", ":", "/", "?", "<", ">"};

	private static final String SIGNATURE_FILE = "signature.tsv";

    private String dbFolder;
    private Map<String, Table> nameToTableMap;
    private Map<String, List<Class<?>>> nameToSignatureMap;
    private String currentTable;

    private ReadWriteLock rwlLock = new ReentrantReadWriteLock();

    public HashMapTableProvider(String folder) throws Exception {
        PermissionsValidator.validateDbFolder(folder, of(NOT_NULL));
        PermissionsValidator.validate(folder, of(CREATE_DIRECTORY_IF_NOT_EXISTS, CAN_READ, IS_DIRECTORY));
        dbFolder = folder;
        nameToTableMap = new HashMap<>();
        nameToSignatureMap = new HashMap<>();
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

    private String getSignatureFilename() {
    	return dbFolder + File.separator + SIGNATURE_FILE;
    }
    
    private void loadSignature() throws Exception {
        String signatureFilename = getSignatureFilename();
        if (!PermissionsValidator.validate(signatureFilename, of(NOT_NULL, EXISTS, IS_FILE))) {
            File signatureFile = new File(signatureFilename);
            signatureFile.createNewFile();
            return;
        }

        if (PermissionsValidator.validateFileEmpty(signatureFilename)) {
            return;
        }

        BufferedReader in = new BufferedReader(new FileReader(signatureFilename));

        while (in.ready()) {
            String s = in.readLine();
            String[] tokens = s.split("\t");

            if (tokens.length != 2) {
                throw new Exception("Wrong line in signatures.tsv: " + s + "\n");
            }
            nameToSignatureMap.put(tokens[0], Utils.parseSignature(tokens[1]));
        }
        in.close();
    }
    
    public void saveSignature() {
    	try (PrintWriter out = new PrintWriter(getSignatureFilename())) {
    		for (String tableName : nameToSignatureMap.keySet()) {
    			String signature = Utils.makeSignature(nameToSignatureMap.get(tableName));
    			out.printf("%s\t%s", tableName, signature);
    		}    		
    	} catch (FileNotFoundException e) {
			System.err.println("Coutldn't save signature: " + e.getMessage());
		}
    }
    
    private void loadDatabase() throws Exception {
        loadSignature();
        for (String tableName : new File(dbFolder).list()) {
        	if (tableName.equals(SIGNATURE_FILE)) {
        		continue;
        	}
            if (!isValidName(tableName)) {
                throw new IllegalArgumentException(tableName + ": incorrect table name");
            }
            Table table = new HashMapTable(this, tableName, nameToSignatureMap.get(tableName));
            nameToTableMap.put(tableName, table);
            rwlLock.readLock().lock(); 
            try {
                table.loadTable();
            } finally {
                rwlLock.readLock().unlock();
            }
        }
    }

    private void checkUnsavedChanges() throws IllegalStateException {
        int count = getCurrentTable().getNumberOfUncommittedChanges();
        if (count > 0) {
            throw new IllegalStateException(Integer.toString(count) + " unsaved changes");
        }
    }

    @Override
    public String getDbFolder() {
        return dbFolder;
    }

    @Override
    public Table getTable(String name) throws IllegalArgumentException {
        if (name == null) {
            throw new IllegalArgumentException("no table");
        }
        return nameToTableMap.get(name);
    }

    @Override
    public Table getCurrentTable() throws IllegalArgumentException {
        return getTable(currentTable);
    }

    @Override
    public Table createTable(String name, List<Class<?>> columnTypes) throws RuntimeException, IOException {
        if (name == null || !isValidName(name)) {
            throw new IllegalArgumentException(name + ": incorrect table name");
        } else if (nameToTableMap.containsKey(name)) {
            throw new IllegalArgumentException(name + " exists");
        }

        Table table = new HashMapTable(this, name, columnTypes);
        nameToTableMap.put(name, table);
        nameToSignatureMap.put(name, columnTypes);
        PermissionsValidator.validate(table.getTableFolder(),
                    of(NOT_NULL, CREATE_DIRECTORY_IF_NOT_EXISTS, CAN_WRITE, IS_DIRECTORY));
        return table;
    }

    @Override
    public void removeTable(String name) throws IllegalArgumentException, IllegalStateException {
        if (name == null || !isValidName(name)) {
            throw new IllegalArgumentException(name + ": incorrect table name");
        } else if (!nameToTableMap.containsKey(name)) {
            throw new IllegalStateException(name + " not exist");
        } else if (name.equals(currentTable)) {
            currentTable = null;
        }
        rwlLock.writeLock().lock(); 
        try {
            Utils.removeDir(Paths.get(dbFolder, name));
        } finally {
            rwlLock.writeLock().unlock();
        }
        nameToTableMap.remove(name);
    }

    @Override
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

    @Override
    public List<String> getTableNames() {
        return new LinkedList<>(nameToTableMap.keySet());
    }

    @Override
    public void exit() throws IllegalStateException {
        if (currentTable != null) {
            checkUnsavedChanges();
        }
        saveSignature();
    }

    @Override
    public Storeable deserialize(Table table, String value) throws ParseException {
        if (value.length() < 2) {
            throw new ParseException(value + ": wrong value length", 0); 
        }
        if (value.charAt(0) != '(') {
            throw new ParseException(value + ": value must start with '('", 0);         	
        }        
        if (value.charAt(value.length() - 1) != ')') {
            throw new ParseException(value + ": value must end with ')'", value.length() - 1);         	
        }

        value = value.substring(1, value.length() - 1);  
        String[] elements = value.split(",");
               
        List<?> expectedTypes = nameToSignatureMap.get(table.getName());
        if (elements.length != expectedTypes.size()) {
            throw new ParseException(
            		String.format("%s: value must contain %d element(s)", value, expectedTypes.size()), 0);         	        	
        }
 
        List<Object> objects = new ArrayList<>();
        
        int i = 0;
        for (String element : elements) {
        	try {
        		if (expectedTypes.get(i).equals(Integer.class)) {
        			objects.add(Integer.parseInt(element));
        		} else if (expectedTypes.get(i).equals(Long.class)) {
        			objects.add(Long.parseLong(element));
        		} else if (expectedTypes.get(i).equals(Byte.class)) {
        			objects.add(Byte.parseByte(element));
        		} else if (expectedTypes.get(i).equals(Float.class)) {
        			objects.add(Float.parseFloat(element));
        		} else if (expectedTypes.get(i).equals(Double.class)) {
        			objects.add(Double.parseDouble(element));
        		} else if (expectedTypes.get(i).equals(Boolean.class)) {
        			objects.add(Boolean.parseBoolean(element));
        		} else if (expectedTypes.get(i).equals(String.class)) {
        			objects.add(element);
        		} else {
        			throw new ParseException("Wrong element type: " + element, 0);
        		}
        	} catch (NumberFormatException e) {
        		throw new ParseException(
                		String.format("Couldn't parse value: %s", e.getMessage()), 0);    
        	}
        	i++;
        }
        
        Storeable result = new JsonStoreable(objects);
        
        return result;
    }

    @Override
    public String serialize(Table table, Storeable value) throws ColumnFormatException {
        List<Class<?>> expectedTypes = nameToSignatureMap.get(table.getName());

        String result = "(";

        List<String> types = new LinkedList<>();
        int i = 0;
        for (Class<?> type : expectedTypes) {
            if (type.equals(Integer.class)) {
                types.add(value.getIntAt(i).toString());
            } else if (type.equals(Long.class)) {
                types.add(value.getLongAt(i).toString());
            } else if (type.equals(Byte.class)) {
                types.add(value.getByteAt(i).toString());
            } else if (type.equals(Float.class)) {
                types.add(value.getFloatAt(i).toString());
            } else if (type.equals(Double.class)) {
                types.add(value.getDoubleAt(i).toString());
            } else if (type.equals(Boolean.class)) {
                types.add(value.getBooleanAt(i).toString());
            } else if (type.equals(String.class)) {
                types.add(value.getStringAt(i));
            }
            i++;
        }
        result += String.join(",", types) + ")";

        return result;
    }

    @Override
    public Storeable createFor(Table table, List<?> values) throws ColumnFormatException,
        IndexOutOfBoundsException {
      return null;
    }

    @Override
    public Storeable createFor(Table table) {
      return null;
    }

    @Override
    public Object showTables() {
      return nameToTableMap;
    }
}
