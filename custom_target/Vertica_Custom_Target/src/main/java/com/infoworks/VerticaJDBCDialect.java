package com.infoworks;

/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.sql.Types;

import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcType;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.types.MetadataBuilder;
import scala.Option;

import static org.apache.spark.sql.types.DataTypes.*;


public class VerticaJDBCDialect extends JdbcDialect {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean canHandle(String url) {
        return url.toLowerCase().startsWith("jdbc:vertica:");
    }

    @Override
    public Option<JdbcType> getJDBCType(DataType dt) {
        if (IntegerType.equals(dt)) {
            return Option.apply(new JdbcType("INTEGER", Types.INTEGER));
        } else if (LongType.equals(dt)) {
            return Option.apply(new JdbcType("BIGINT", Types.BIGINT));
        } else if (DoubleType.equals(dt)) {
            return Option.apply(new JdbcType("FLOAT8", Types.DOUBLE));
        } else if (FloatType.equals(dt)) {
            return Option.apply(new JdbcType("FLOAT4", Types.FLOAT));
        } else if (ShortType.equals(dt)) {
            return Option.apply(new JdbcType("SMALLINT", Types.SMALLINT));
        } else if (ByteType.equals(dt)) {
            return Option.apply(new JdbcType("SMALLINT", Types.SMALLINT));
        } else if (BooleanType.equals(dt)) {
            return Option.apply(new JdbcType("BOOLEAN", Types.BOOLEAN));
        } else if (StringType.equals(dt)) {
            return Option.apply(new JdbcType("TEXT", Types.CHAR));
        } else if (BinaryType.equals(dt)) {
            return Option.apply(new JdbcType("BYTEA", Types.BINARY));
        } else if (TimestampType.equals(dt)) {
            return Option.apply(new JdbcType("TIMESTAMP", Types.TIMESTAMP));
        } else if (DateType.equals(dt)) {
            return Option.apply(new JdbcType("DATE", Types.DATE));
        } else if (dt instanceof DecimalType) {
            DecimalType t = (DecimalType) dt;
            return Option.apply(new JdbcType("NUMERIC(" + t.precision() + "," + t.scale() + ")", Types.NUMERIC));
        } else {
            return Option.empty();
        }
    }

    @Override
    public Option<DataType> getCatalystType(int sqlType, String typeName, int size, MetadataBuilder md) {
        if (sqlType == Types.REAL) {
            return Option.apply(DataTypes.FloatType);
        } else if (sqlType == Types.SMALLINT) {
            return Option.apply(DataTypes.ShortType);
        } else if (sqlType == Types.BIT && typeName.equals("bit") && size != 1) {
            return Option.apply(DataTypes.BinaryType);
        } else if (sqlType == Types.OTHER) {
            return Option.apply(DataTypes.StringType);
        }  else return Option.empty();
    }

    @Override
    public String getTableExistsQuery(String table) {
        return "SELECT 1 FROM " + table + " LIMIT 1";
    }

    // https://medium.com/@huaxingao/customize-spark-jdbc-data-source-to-work-with-your-dedicated-database-dialect-beec6519af27
    // https://forum.vertica.com/discussion/238839/spark-df-to-vertica-driver-not-capable
}
