package ru.fizteh.fivt.students.andrewzhernov.database;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class Main {
    public static void main(String[] args) {
        try {
            TableProvider provider = new HashMapTableProvider(System.getProperty("fizteh.db.dir"));
            Shell shell = new Shell(provider, new Command[] {
                new Command("size", 1, new Handler() {
                    @Override
                    public Object execute(TableProvider provider, String[] args) throws Exception {
                        return provider.getCurrentTable().size();
                    }
                    @Override
                    public void handle(TableProvider provider, Object object) throws Exception {
                        System.out.println(object);
                    }
                }),
                new Command("put", 3, new Handler() {
                    @Override
                    public Object execute(TableProvider provider, String[] args) throws Exception {
                        Table table = provider.getCurrentTable();
                        return table.put(args[1], provider.deserialize(table, args[2]));
                    }
                    @Override
                    public void handle(TableProvider provider, Object object) throws Exception {
                        if (object == null) {
                            System.out.println("new");
                        } else {
                            System.out.println("overwrite");
                            System.out.println(provider.serialize(
                                    provider.getCurrentTable(), (Storeable) object));
                        }
                    }
                }),
                new Command("get", 2, new Handler() {
                    @Override
                    public Object execute(TableProvider provider, String[] args) throws Exception {
                        return provider.getCurrentTable().get(args[1]);
                    }
                    @Override
                    public void handle(TableProvider provider, Object object) throws Exception {
                        if (object == null) {
                            System.out.println("not found");
                        } else {
                            System.out.println("found");
                            System.out.println(provider.serialize(
                                    provider.getCurrentTable(), (Storeable) object));
                        }
                    }
                }),
                new Command("remove", 2, new Handler() {
                    @Override
                    public Object execute(TableProvider provider, String[] args) throws Exception {
                        return provider.getCurrentTable().remove(args[1]);
                    }
                    @Override
                    public void handle(TableProvider provider, Object object) throws Exception {
                        if (object == null) {
                            System.out.println("not found");
                        } else {
                            System.out.println("removed");
                        }
                    }
                }),
                new Command("list", 1, new Handler() {
                    @Override
                    public Object execute(TableProvider provider, String[] args) throws Exception {
                        return provider.getCurrentTable().list();
                    }
                    @Override
                    public void handle(TableProvider provider, Object object) throws Exception {
                        @SuppressWarnings("unchecked")
                        List<String> list = (List<String>) object;
                        System.out.println(Utils.join(", ", list.toArray(new String[list.size()])));
                    }
                }),
                new Command("commit", 1, new Handler() {
                    @Override
                    public Object execute(TableProvider provider, String[] args) throws Exception {
                        return provider.getCurrentTable().commit();
                    }
                    @Override
                    public void handle(TableProvider provider, Object object) throws Exception {
                        System.out.println(object);
                    }
                }),
                new Command("rollback", 1, new Handler() {
                    @Override
                    public Object execute(TableProvider provider, String[] args) throws Exception {
                        return provider.getCurrentTable().rollback();
                    }
                    @Override
                    public void handle(TableProvider provider, Object object) throws Exception {
                        System.out.println(object);
                    }
                }),
                new Command("create", 3, new Handler() {
                    @Override
                    public Object execute(TableProvider provider, String[] args) throws Exception {
                        return provider.createTable(args[1], Utils.parseSignature(args[2]));
                    }
                   @Override
                    public void handle(TableProvider provider, Object object) throws Exception {
                        System.out.println("created");
                    }
                }),
                new Command("drop", 2, new Handler() {
                    @Override
                    public Object execute(TableProvider provider, String[] args) throws Exception {
                        provider.removeTable(args[1]);
                        return null;
                    }
                    @Override
                    public void handle(TableProvider provider, Object object) throws Exception {
                        System.out.println("dropped");
                    }
                }),
                new Command("use", 2, new Handler() {
                    @Override
                    public Object execute(TableProvider provider, String[] args) throws Exception {
                        return provider.useTable(args[1]);
                    }
                    @Override
                    public void handle(TableProvider provider, Object object) throws Exception {
                        System.out.println("using " + (String) object);
                    }
                }),
                new Command("show tables", 2, new Handler() {
                    @Override
                    public Object execute(TableProvider provider, String[] args) throws Exception {
                        return provider.showTables();
                    }
                    @Override
                    public void handle(TableProvider provider, Object object) throws Exception {
                        @SuppressWarnings("unchecked")
                        Map<String, Storeable> map = (Map<String, Storeable>) object;
                        for (String tablename : map.keySet()) {
                            System.out.println(tablename);
                        }
                    }
                }),
                new Command("exit", 1, new Handler() {
                    @Override
                    public Object execute(TableProvider provider, String[] args) throws Exception {
                        provider.exit();
                        return null;
                    }
                    @Override
                    public void handle(TableProvider provider, Object object) throws Exception {
                        System.exit(0);
                    }
                })
            });
            shell.run(args);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
}
