package com.atguigu.gmall.realtime.func;

import com.atguigu.gmall.realtime.utils.KeyWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<word String>"))
public class SplitFunction extends TableFunction<Row> {
    public void eval(String keyword) {
        List<String> list = null;

        try {
            list = KeyWordUtil.splitKeyWord(keyword);

            for (String word : list) {
                collect(Row.of(word));
            }
        } catch (IOException e) {
            collect(Row.of(keyword));
        }
    }
}
