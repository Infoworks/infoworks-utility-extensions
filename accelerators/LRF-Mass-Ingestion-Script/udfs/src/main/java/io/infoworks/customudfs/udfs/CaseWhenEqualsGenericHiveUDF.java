package io.infoworks.customudfs.udfs;
import org.apache.hadoop.hive.ql.exec.UDF;

public class CaseWhenEqualsGenericHiveUDF extends UDF {
    public static String evaluate(String input1, String... args) {
        String else_value = args[args.length - 1];
        for (int i = 0; i < args.length - 1; i = i + 2) {
            if (input1.equals(args[i])) {
                return args[i + 1];
            }
        }
        return else_value;
    }
}
