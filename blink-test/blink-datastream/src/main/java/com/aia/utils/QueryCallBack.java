package com.aia.utils;

import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class QueryCallBack {
    public abstract void process(ResultSet rs) throws SQLException;
}
