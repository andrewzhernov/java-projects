package ru.fizteh.fivt.students.andrewzhernov.database.tests;

import ru.fizteh.fivt.students.andrewzhernov.database.*;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.util.List;
import java.util.LinkedList;

public class TableTest {
    TableManager manager;
    Table table;

    @Before
    public void setUp() throws Exception {
        manager = new TableManager("data");
        table = manager.createTable("test_table");
    }

    @After
    public void tearDown() throws Exception {
        manager.removeTable("test_table");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTableConstructorThrowsExceptionLoadedInvalidDirectory() {
        new Table(manager, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTablePutInvalidKey() {
        table.put(null, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTablePutInvalidValue() {
        table.put("", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTableGetInvalidKey() {
        table.get(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTableRemoveInvalidKey() {
        table.remove(null);
    }

    @Test
    public void testTableSize() {
        table.put("1", "xyz");
        assertEquals(1, table.size());
        table.put("1", "abc");
        assertEquals(1, table.size());
        table.put("2", "bca");
        assertEquals(2, table.size());
        table.remove("1");
        assertEquals(1, table.size());
        table.remove("2");
        assertEquals(0, table.size());
    }

    @Test
    public void testTablePut() {
        table.put("1", "xyz");
        assertNull(table.put("2", "abc"));
        assertEquals("xyz", table.put("1", "zyx"));
        assertEquals("abc", table.put("2", "cba"));
    }

    @Test
    public void testTableGet() {
        table.put("1", "xyz");
        assertEquals("xyz", table.get("1"));
        assertNull(table.get("2"));
        table.put("2", "abc");
        assertEquals("abc", table.get("2"));
    }

    @Test
    public void testTableRemove() {
        table.put("1", "xyz");
        table.put("2", "abc");
        assertEquals("abc", table.remove("2"));
        assertEquals("xyz", table.remove("1"));
        assertNull("1-error", table.remove("1"));
        assertNull("2-error", table.remove("2"));
    }

    @Test
    public void testTableList() {
        table.put("1", "xyz");
        List<String> list = new LinkedList<>();
        list.add("1");
        assertEquals(list, table.list());
        table.put("2", "abc");
        list.add("2");
        assertEquals(list, table.list());
    }

    @Test
    public void testTableCommit() {
        table.put("1", "abc");
        assertEquals(1, table.commit());
        table.put("2", "efg");
        table.put("1", "bca");
        table.remove("1");
        assertEquals(2, table.commit());
    }

    @Test
    public void testTableRollback() {
        table.put("1", "xyz");
        table.commit();
        table.put("1", "abc");
        assertEquals(1, table.rollback());

        table.put("2", "efg");
        table.remove("1");
        assertEquals(2, table.rollback());
    }
}
