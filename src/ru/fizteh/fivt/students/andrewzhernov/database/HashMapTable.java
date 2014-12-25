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
    
    private TableProvider provider_;
    private String name_;
    private int size_;
    private Map<String, Storeable> disk_;
    private ThreadLocal<Map<String, Storeable>> diff_;
    private List<Class<?>> columnTypes_;

    private ReadWriteLock rwlLock = new ReentrantReadWriteLock();

    public HashMapTable(TableProvider tableProvider, String tableName, List<Class<?>> columnTypes) {
        PermissionsValidator.validateTableName(tableName, of(NOT_NULL));
        provider_ = tableProvider;
        name_ = tableName;
        columnTypes_ = columnTypes;
        size_ = 0;
        disk_ = new HashMap<>();
        diff_ = ThreadLocal.withInitial(() -> new HashMap<String, Storeable>());
    }

    @Override
    public String getName() {
        return name_;
    }

    @Override
    public int getNumberOfUncommittedChanges() {
        return diff_.get().size();
    }

    @Override
    public int size() {
        return size_;
    }

    @Override
    public Storeable put(String key, Storeable value) throws IllegalArgumentException {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Invalid key/value");
        }
        if (!value.equals(disk_.get(key))) {
            Storeable diffValue = diff_.get().put(key, value);
            Storeable result = diffValue != null ? diffValue : disk_.get(key);
            if (result == null) {
                ++size_;
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
        Storeable diffValue = diff_.get().get(key);
        return diffValue != null ? diffValue : disk_.get(key);
    }

    @Override
    public Storeable remove(String key) throws IllegalArgumentException {
        if (key == null) {
            throw new IllegalArgumentException("Invalid key");
        }
        Storeable result = null;
        Storeable diskValue = disk_.get(key);
        if (diskValue != null) {
            Storeable diffValue = diff_.get().put(key, null);
            result = diffValue != null ? diffValue : diskValue;
        } else {
            result = diff_.get().remove(key);
        }
        if (result != null) {
            --size_;
        }
        return result;
    }

    @Override
    public List<String> list() {
        List<String> list = new LinkedList<>();
        for (String key : disk_.keySet()) {
            if (!diff_.get().containsKey(key)) {
                list.add(key);
            }
        }
        for (String key : diff_.get().keySet()) {
            if (diff_.get().get(key) != null) {
                list.add(key);
            }
        }
        return list;
    }

    @Override
    public int commit() {
        int amount = diff_.get().size();
        for (String key : diff_.get().keySet()) {
            Storeable value = diff_.get().get(key);
            if (value != null) {
                disk_.put(key, value);
            } else {
                disk_.remove(key);
            }
        }
        diff_.get().clear();
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
        int amount = diff_.get().size();
        diff_.get().clear();
        size_ = disk_.size();
        return amount;
    }

    @Override
    public String getTableFolder() {
        return Paths.get(provider_.getDbFolder(), name_).toString();
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
        try (RandomAccessFile file = new RandomAccessFile(subBucket, "r")) {
            while (file.getFilePointer() < file.length()) {
                String key = readItem(file); 
                String value = readItem(file);
                disk_.put(key, provider_.deserialize(provider_.getTable(name_), value));
            }
        } catch (Exception | OutOfMemoryError e) {
            throw new Exception(subBucket + ": invalid file format");
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
    public void loadTable() {
        try {
            PermissionsValidator.validate(getTableFolder(), of(NOT_NULL, EXISTS, CAN_READ, IS_DIRECTORY));
            disk_.clear();
            for (int bucketIndex = 0; bucketIndex < DIRECTORIES_COUNT; ++bucketIndex) {
                String bucket = getTableBucket(bucketIndex);
                if (PermissionsValidator.validate(bucket, of(NOT_NULL, EXISTS, CAN_READ, IS_DIRECTORY))) {
                    loadBucket(bucket);
                }
            }
            size_ = disk_.size();
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
        for (String key : disk_.keySet()) {
            int hashcode = key.hashCode();
            int directoryNumber = hashcode % DIRECTORIES_COUNT;
            int fileNumber = hashcode / DIRECTORIES_COUNT % FILES_COUNT;
            if (bucketIndex == directoryNumber && subBucketIndex == fileNumber) {
                String value = provider_.serialize(provider_.getTable(name_), disk_.get(key));
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

    private void saveTable() {
        try {
            PermissionsValidator.validate(getTableFolder(), of(NOT_NULL, EXISTS, CAN_WRITE, IS_DIRECTORY));
            for (int bucketIndex = 0; bucketIndex < DIRECTORIES_COUNT; ++bucketIndex) {
                String bucket = getTableBucket(bucketIndex);
                if (PermissionsValidator.validate(bucket, 
                            of(NOT_NULL, CREATE_DIRECTORY_IF_NOT_EXISTS, CAN_WRITE, IS_DIRECTORY))) {
                    saveBucket(bucket, bucketIndex);
                }
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    @Override
    public int getColumnsCount() {
      return columnTypes_.size();
    }

    @Override
    public Class<?> getColumnType(int columnIndex) throws IndexOutOfBoundsException {
      return columnTypes_.get(columnIndex);
    }
}
