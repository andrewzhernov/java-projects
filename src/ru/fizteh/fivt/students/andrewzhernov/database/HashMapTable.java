package ru.fizteh.fivt.students.andrewzhernov.database;

import static java.util.EnumSet.of;
import static ru.fizteh.fivt.students.andrewzhernov.database.PermissionsValidator.Permissions.*;

import java.io.*;
import java.nio.file.Paths;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class HashMapTable implements Table {
    private static final int DIRECTORIES_COUNT = 16;
    private static final int FILES_COUNT = 16;
    private static final String DIR_EXTENSION = ".dir";
    private static final String FILE_EXTENSION = ".dat";
    private static final String ENCODING = "UTF-8";
    
    private TableProvider provider;
    private String name;
    private int size;
    private Map<String, Storeable> disk;
    private ThreadLocal<Map<String, Storeable>> diff;
    private List<Class<?>> columnTypes;

    private ReadWriteLock rwlLock = new ReentrantReadWriteLock();

    public HashMapTable(TableProvider tableProvider, String tableName, List<Class<?>> columnTypes) {
        PermissionsValidator.validateTableName(tableName, of(NOT_NULL));
        provider = tableProvider;
        name = tableName;
        columnTypes = columnTypes;
        size = 0;
        disk = new HashMap<>();
        diff = ThreadLocal.withInitial(() -> new HashMap<String, Storeable>());
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int getNumberOfUncommittedChanges() {
        return diff.get().size();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Storeable put(String key, Storeable value) throws IllegalArgumentException {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Invalid key/value");
        }
        if (!value.equals(disk.get(key))) {
            Storeable diffValue = diff.get().put(key, value);
            Storeable result = diffValue != null ? diffValue : disk.get(key);
            if (result == null) {
                ++size;
            }
            return result;
        }
        return value;
    }

    @Override
    public Storeable get(String key) throws IllegalArgumentException {
        if (key == null) {
            throw new IllegalArgumentException("Invalid key");
        }
        Storeable diffValue = diff.get().get(key);
        return diffValue != null ? diffValue : disk.get(key);
    }

    @Override
    public Storeable remove(String key) throws IllegalArgumentException {
        if (key == null) {
            throw new IllegalArgumentException("Invalid key");
        }
        Storeable result = null;
        Storeable diskValue = disk.get(key);
        if (diskValue != null) {
            Storeable diffValue = diff.get().put(key, null);
            result = diffValue != null ? diffValue : diskValue;
        } else {
            result = diff.get().remove(key);
        }
        if (result != null) {
            --size;
        }
        return result;
    }

    @Override
    public List<String> list() {
        List<String> list = new LinkedList<>();
        for (String key : disk.keySet()) {
            if (!diff.get().containsKey(key)) {
                list.add(key);
            }
        }
        for (String key : diff.get().keySet()) {
            if (diff.get().get(key) != null) {
                list.add(key);
            }
        }
        return list;
    }

    @Override
    public int commit() throws Exception {
        int amount = diff.get().size();
        for (String key : diff.get().keySet()) {
            Storeable value = diff.get().get(key);
            if (value != null) {
                disk.put(key, value);
            } else {
                disk.remove(key);
            }
        }
        diff.get().clear();
        rwlLock.writeLock().lock(); 
        try {
            saveTable();
        } finally {
            rwlLock.writeLock().unlock();
        }
        return amount;
    }

    @Override
    public int rollback() {
        int amount = diff.get().size();
        diff.get().clear();
        size = disk.size();
        return amount;
    }

    @Override
    public String getTableFolder() {
        return Paths.get(provider.getDbFolder(), name).toString();
    }

    public String getTableBucket(int bucket) {
        return Paths.get(getTableFolder(), new Integer(bucket).toString() + DIR_EXTENSION).toString();
    }

    public String getTableSubBucket(String bucket, int subBucket) {
        return Paths.get(bucket, new Integer(subBucket).toString() + FILE_EXTENSION).toString();
    }

    private static String readItem(RandomAccessFile file) throws Exception {
        int wordSize = file.readInt();
        byte[] word = new byte[wordSize];
        file.read(word, 0, wordSize);
        return new String(word, ENCODING);
    }

    private void loadSubBucket(String subBucket) throws Exception {
        try (RandomAccessFile file = new RandomAccessFile(subBucket, "r")) {
            while (file.getFilePointer() < file.length()) {
                String key = readItem(file); 
                String value = readItem(file);
                disk.put(key, provider.deserialize(provider.getTable(name), value));
            }
        } catch (Exception | OutOfMemoryError e) {
            throw new ConnectionInterruptException(subBucket + ": invalid file format");
        }
    }

    private void loadBucket(String bucket) throws Exception {
        for (int subBucketIndex = 0; subBucketIndex < FILES_COUNT; ++subBucketIndex) {
            String subBucket = getTableSubBucket(bucket, subBucketIndex);
            if (PermissionsValidator.validate(subBucket, of(NOT_NULL, EXISTS, CAN_READ, IS_FILE))) {
                loadSubBucket(subBucket);
            }
        }            
    }

    @Override
    public void loadTable() throws Exception {
        PermissionsValidator.validate(getTableFolder(), of(NOT_NULL, EXISTS, CAN_READ, IS_DIRECTORY));
        disk.clear();
        for (int bucketIndex = 0; bucketIndex < DIRECTORIES_COUNT; ++bucketIndex) {
            String bucket = getTableBucket(bucketIndex);
            if (PermissionsValidator.validate(bucket, of(NOT_NULL, EXISTS, CAN_READ, IS_DIRECTORY))) {
                loadBucket(bucket);
            }
        }
        size = disk.size();
    }

    private static void writeItem(RandomAccessFile file, String word) throws Exception {
        byte[] byteWord = word.getBytes(ENCODING);
        file.writeInt(byteWord.length);
        file.write(byteWord);
    }

    private boolean saveSubBucket(String subBucket, int bucketIndex, int subBucketIndex) throws Exception {
        boolean isWritten = false;
        RandomAccessFile file = new RandomAccessFile(subBucket, "rw");
        for (String key : disk.keySet()) {
            int hashcode = key.hashCode();
            int directoryNumber = hashcode % DIRECTORIES_COUNT;
            int fileNumber = hashcode / DIRECTORIES_COUNT % FILES_COUNT;
            if (bucketIndex == directoryNumber && subBucketIndex == fileNumber) {
                String value = provider.serialize(provider.getTable(name), disk.get(key));
                writeItem(file, key);
                writeItem(file, value);
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

    private void saveTable() throws Exception {
        PermissionsValidator.validate(getTableFolder(), of(NOT_NULL, EXISTS, CAN_WRITE, IS_DIRECTORY));
        for (int bucketIndex = 0; bucketIndex < DIRECTORIES_COUNT; ++bucketIndex) {
            String bucket = getTableBucket(bucketIndex);
            if (PermissionsValidator.validate(bucket, 
                        of(NOT_NULL, CREATE_DIRECTORY_IF_NOT_EXISTS, CAN_WRITE, IS_DIRECTORY))) {
                saveBucket(bucket, bucketIndex);
            }
        }
    }

    @Override
    public int getColumnsCount() {
      return columnTypes.size();
    }

    @Override
    public Class<?> getColumnType(int columnIndex) throws IndexOutOfBoundsException {
      return columnTypes.get(columnIndex);
    }
}
