package io.infoworks.customudfs.udfs;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.spark.sql.functions;

public class GetFileNameHiveUDF extends UDF {

  public static String evaluate(String input, String default_value) {
    String file_name = String.valueOf(functions.input_file_name());
    // String file_name="tlg_tax_fees.abc.202207.dat";
    // String file_name="/home/infoworks/tlg_tax_fees.abc.202207.dat";
    if (StringUtils.isEmpty(file_name)) {

      return Utils.coalesce(file_name, default_value);
    }

    return Utils.coalesce(
        SubstrHiveUDF.evaluate(file_name, file_name.indexOf(".") + 1, file_name.indexOf(".") + 4),
        default_value);
  }
}
