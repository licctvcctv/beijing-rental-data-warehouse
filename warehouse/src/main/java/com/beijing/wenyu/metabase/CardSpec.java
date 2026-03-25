package com.beijing.wenyu.metabase;

public class CardSpec {

    private final String name;
    private final String description;
    private final String display;
    private final String query;
    private final int row;
    private final int col;
    private final int sizeX;
    private final int sizeY;

    public CardSpec(String name, String description, String display, String query, int row, int col, int sizeX, int sizeY) {
        this.name = name;
        this.description = description;
        this.display = display;
        this.query = query;
        this.row = row;
        this.col = col;
        this.sizeX = sizeX;
        this.sizeY = sizeY;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getDisplay() {
        return display;
    }

    public String getQuery() {
        return query;
    }

    public int getRow() {
        return row;
    }

    public int getCol() {
        return col;
    }

    public int getSizeX() {
        return sizeX;
    }

    public int getSizeY() {
        return sizeY;
    }
}
