package ru.fizteh.fivt.students.andrewzhernov.database;

import java.util.List;

public class JsonStoreable implements Storeable {
    private Object[] columnValues;
   
    public JsonStoreable(List<Object> values) {
        columnValues = values.toArray();
    }
    
    @Override
    public void setColumnAt(int columnIndex, Object value) throws ColumnFormatException, IndexOutOfBoundsException {
        if (value.getClass() != columnValues[columnIndex].getClass()) {
            throw new ColumnFormatException("Invalid column format: expected " +
                    columnValues[columnIndex].getClass().getName() + ", got " + value.getClass().getName());
        }
        columnValues[columnIndex] = value;
    }

    @Override
    public Object getColumnAt(int columnIndex) throws IndexOutOfBoundsException {
        return columnValues[columnIndex];
    }

    @Override
    public Integer getIntAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        if (!(columnValues[columnIndex] instanceof Integer)) {
            throw new ColumnFormatException("Column is not Integer");
        }
        return (Integer) columnValues[columnIndex];
    }

    @Override
    public Long getLongAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        if (!(columnValues[columnIndex] instanceof Long)) {
            throw new ColumnFormatException("Column is not Long");
        }
        return (Long) columnValues[columnIndex];
    }

    @Override
    public Byte getByteAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        if (!(columnValues[columnIndex] instanceof Byte)) {
            throw new ColumnFormatException("Column is not Byte");
        }
        return (Byte) columnValues[columnIndex];
    }

    @Override
    public Float getFloatAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        if (!(columnValues[columnIndex] instanceof Float)) {
            throw new ColumnFormatException("Column is not Float");
        }
        return (Float) columnValues[columnIndex];
    }

    @Override
    public Double getDoubleAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        if (!(columnValues[columnIndex] instanceof Double)) {
            throw new ColumnFormatException("Column is not Double");
        }
        return (Double) columnValues[columnIndex];
    }

    @Override
    public Boolean getBooleanAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        if (!(columnValues[columnIndex] instanceof Boolean)) {
            throw new ColumnFormatException("Column is not Boolean");
        }
        return (Boolean) columnValues[columnIndex];
    }

    @Override
    public String getStringAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        if (!(columnValues[columnIndex] instanceof String)) {
            throw new ColumnFormatException("Column is not String");
        }
        return (String) columnValues[columnIndex];
    }
    
    public int size() {
        return columnValues.length;
    }
}
