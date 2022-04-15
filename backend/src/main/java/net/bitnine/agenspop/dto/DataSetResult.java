package net.bitnine.agenspop.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

// **NOTE
// https://github.com/uber/uberscriptquery/blob/master/src/main/java/com/uber/uberscriptquery/jdbc/DataSetResult.java

@Getter @Setter @ToString
public class DataSetResult {

    private List<String> columnNames = new ArrayList<>();
    private List<List<Object>> rows = new ArrayList<>();

    public String getSingleStringValue() {
        if (rows == null || rows.isEmpty()) {
            return "";
        }
        List<Object> values = rows.get(0);
        if (values == null || values.isEmpty()) {
            return "";
        }
        return String.valueOf(values.get(0));
    }

}