package ru.fizteh.fivt.students.andrewzhernov.database;

import java.io.*;
import java.lang.Integer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;

import static java.util.EnumSet.of;
import static ru.fizteh.fivt.students.andrewzhernov.database.PermissionsValidator.Permissions.*;

public class Table implements TableInterface {
    private static final int DIRECTORIES_COUNT = 16;
    private static final int FILES_COUNT = 16;
    
    private TableManager manager;
    private String name;
    private int size;
    private Map<String, String> disk;
    private Map<String, String> diff;

    public Table(TableManager tableManager, String tableName) {
        PermissionsValidator.validateTableName(tableName, of(NOT_NULL));
        manager = tableManager;
        name = tableName;
        size = 0;
        disk = new HashMap<>();
        diff = new HashMap<>();
    }

    public String getName() {
        return name;
    }

    public int unsavedSize() {
        return diff.size();
    }

    public int size() {
        return size;
    }

    public String put(String key, String value) throws IllegalArgumentException {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Invalid key/value");
        }
        String diffValue = diff.put(key, value);
        String result = diffValue != null ? diffValue : disk.get(key);
        if (result == null) {
            ++size;
        }
        return result;
    }

    public String get(String key) throws IllegalArgumentException {
        if (key == null) {
            throw new IllegalArgumentException("Invalid key");
        }
        String diffValue = diff.get(key);
        return diffValue != null ? diffValue : disk.get(key);
    }

    public String remove(String key) throws IllegalArgumentException {
        if (key == null) {
            throw new IllegalArgumentException("Invalid key");
        }
        String result = null;
        String diskValue = disk.get(key);
        if (diskValue != null) {
            String diffValue = diff.put(key, null);
            result = diffValue != null ? diffValue : diskValue;
        } else {
            result = diff.remove(key);
        }
        if (result != null) {
            --size;
        }
        return result;
    }

    public List<String> list() {
        List<String> list = new LinkedList<String>();
        for (String key : disk.keySet()) {
            if (!diff.containsKey(key)) {
                list.add(key);
            }
        }
        for (String key : diff.keySet()) {
            if (diff.get(key) != null) {
                list.add(key);
            }
        }
        return list;
    }

    public int commit() {
        int amount = diff.size();
        for (String key : diff.keySet()) {
            String value = diff.get(key);
            if (value != null) {
                disk.put(key, value);
            } else {
                disk.remove(key);
            }
        }
        diff.clear();
        saveTable();
        return amount;
    }

    public int rollback() {
        int amount = diff.size();
        diff.clear();
        size = disk.size();
        return amount;
    }

    public String getTableFolder() {
        return Paths.get(manager.getDbFolder(), name).toString();
    }

    public String getTableBucket(int bucket) {
        return Paths.get(getTableFolder(), new Integer(bucket).toString() + ".dir").toString();
    }

    public String getTableSubBucket(String bucket, int subBucket) {
        return Paths.get(bucket, new Integer(subBucket).toString() + ".dat").toString();
    }

    private static String readItem(RandomAccessFile file) throws Exception {
        int wordSize = file.readInt();
        byte[] word = new byte[wordSize];
        file.read(word, 0, wordSize);
        return new String(word, "UTF-8");
    }

    private void loadSubBucket(String subBucket) throws Exception {
        RandomAccessFile file = new RandomAccessFile(subBucket, "r");
        while (file.getFilePointer() < file.length()) {
            try {
                String key = readItem(file);
                String value = readItem(file);
                disk.put(key, value);
            } catch (Exception | OutOfMemoryError e) {
                throw new Exception(subBucket + ": invalid file format");
            }
        }
        file.close();
    }

    private void loadBucket(String bucket) throws Exception {
        for (int subBucketIndex = 0; subBucketIndex < FILES_COUNT; ++subBucketIndex) {
            String subBucket = getTableSubBucket(bucket, subBucketIndex);
            if (PermissionsValidator.validate(subBucket, of(NOT_NULL, EXISTS, CAN_READ, IS_FILE))) {
                loadSubBucket(subBucket);
            }
        }            
    }

    public void loadTable() {
        try {
            PermissionsValidator.validate(getTableFolder(), of(NOT_NULL, EXISTS, CAN_READ, IS_DIRECTORY));
            disk.clear();
            for (int bucketIndex = 0; bucketIndex < DIRECTORIES_COUNT; ++bucketIndex) {
                String bucket = getTableBucket(bucketIndex);
                if (PermissionsValidator.validate(bucket, of(NOT_NULL, EXISTS, CAN_READ, IS_DIRECTORY))) {
                    loadBucket(bucket);
                }
            }
            size = disk.size();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    private static void writeItem(RandomAccessFile file, String word) throws Exception {
        byte[] byteWord = word.getBytes("UTF-8");
        file.writeInt(byteWord.length);
        file.write(byteWord);
    }

    private boolean saveSubBucket(String subBucket, int bucketIndex, int subBucketIndex) throws Exception {
        boolean isWritten = false;
        RandomAccessFile file = new RandomAccessFile(subBucket, "rw");
        for (String key : disk.keySet()) {
            int hashcode = key.hashCode();
            int dirNumber = hashcode % DIRECTORIES_COUNT;
            int fileNumber = hashcode / DIRECTORIES_COUNT % FILES_COUNT;
            if (bucketIndex == dirNumber && subBucketIndex == fileNumber) {
                writeItem(file, key);
                writeItem(file, disk.get(key));
                isWritten = true;
            }
        }
        file.close();
        return isWritten;
    }

    private void saveBucket(String bucket, int bucketIndex) throws Exception {
        int usedFiles = FILES_COUNT;
        for (int subBucketIndex = 0; subBucketIndex < FILES_COUNT; ++subBucketIndex) {
            String subBucket = getTableSubBucket(bucket, subBucketIndex);
            if (PermissionsValidator.validate(subBucket, of(NOT_NULL, CREATE_FILE_IF_NOT_EXISTS, CAN_WRITE, IS_FILE))) {
                if (!saveSubBucket(subBucket, bucketIndex, subBucketIndex)) {
                    new File(subBucket).delete();
                    --usedFiles;
                }
            }
        }
        if (usedFiles == 0) {
            new File(bucket).delete();
        }
    }

    private void saveTable() {
        try {
            PermissionsValidator.validate(getTableFolder(), of(NOT_NULL, EXISTS, CAN_WRITE, IS_DIRECTORY));
            for (int bucketIndex = 0; bucketIndex < DIRECTORIES_COUNT; ++bucketIndex) {
                String bucket = getTableBucket(bucketIndex);
                if (PermissionsValidator.validate(bucket, of(NOT_NULL, CREATE_DIRECTORY_IF_NOT_EXISTS, CAN_WRITE, IS_DIRECTORY))) {
                    saveBucket(bucket, bucketIndex);
                }
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
}
