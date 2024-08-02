package io.infoworks.customudfs.udfs;

import java.text.ParseException;

public class Test {

  public static void main(String[] args) throws ParseException {
    // String file_name="tlg_tax_fees.abc.202207.dat";
    // System.out.println(GetFileNameHiveUDF.evaluate("test","a"));
    // String input2 = "A09B11A2022 23:59:59";
    // String input="";
    // System.out.println(StringToTimestampHiveUDF.evaluate(input));
    // System.out.println(SubstrHiveUDF.evaluate("AYDITYA",0,2));
    // System.out.println(SysdatetoDateFormatHiveUDF.evaluate(null,"yyMMdd"));
    //    System.out.println(LitHiveUDF.evaluate(input));
    // System.out.println(TrimOReplaceSubstringHiveUDF.evaluate(input, 30, "", ""));
    // System.out.println(DecimalCleanupHiveUDF.evaluate(""));
    // System.out.println(NonASCIICleanupHiveUDF.evaluate("日本人 中國的 ~=[]()%+{}@;’#!$_&- " +" éè
    // ;∞¥₤€ We hopè you find it inform@tiv€" +" Thank You!"));

    // System.out.println(NonASCIICleanupHiveUDF.evaluate(""));

    // System.out.println(CastStringToTimestamp0.evaluate("2022-10-20 00:00:56"));
    // System.out.println(StringToTimestampHiveUDF.evaluate(input2));

    // System.out.println(TrimRegexp_replaceHiveUDF.evaluate("日本人 中國的
    // ~=[]()%+{}@","[^\\x00-\\x7F]","AA"));

    // System.out.println(NvlIntegerHiveUDF.evaluate(null,3));

    // System.out.println(NullIFHiveUDF.evaluate("ADITYA","~"));
    // System.out.println(CoalesceNullIF.evaluate("ADITYA","ADITYA","Latest"));
    // System.out.println(CastNUllIFtoTimestamp0HiveUDF.evaluate("2012/10/04","","yyyy/MM/dd"));
    // System.out.println(CastNUllIFtoTimestamp0HiveUDF.evaluate("","","yyyy/MM/dd"));
    // System.out.println(CastNUllIFtoTimestamp0HiveUDF.evaluate(null,"","yyyy/MM/dd"));

    // System.out.println(ReturnZeroIfParamsEqualHiveUDF.evaluate(0,"4","0"));
    //    System.out.println(DivideHiveUDF.evaluate(600,"100"));
    //    System.out.println(DivideHiveUDF.evaluate("600.2","100"));
    //    System.out.println(DivideHiveUDF.evaluate("704","100"));
    //    System.out.println(DivideHiveUDF.evaluate(402,"100"));
    // System.out.println(ReturnsecondinputHiveUDF.evaluate(2,"test"));
    // System.out.println(ReturnsecondinputHiveUDF.evaluate("2",null));
    // System.out.println(divideHiveUDF.evaluate(0,100));
    // System.out.println(StringToTimestampHiveUDF.evaluate("09B11A2024 22:59:09"));
    // System.out.println(SysdatetoIntHiveUDF.evaluate(2));
    System.out.println(CoalesceNullIFHiveUDF.evaluate("234", "23", "67"));
    System.out.println(CoalesceNullIFHiveUDF.evaluate("2", "2", 67L));

    // System.out.println(NULLIFHiveTimestampUDF.evaluate(Timestamp.valueOf("2022-03-02
    // 22:34:31"),Timestamp.valueOf("2022-03-02 22:34:32")));
    // System.out.println(CaseWhenIsEmptyThenNull.evaluate("2022-03-02"));
    // System.out.println(NULLIFHiveTimestampUDF.evaluate(Timestamp.valueOf("2022-03-02
    // 22:34:30"),null));

    // System.out.println(CastAsdateHiveUDF.evaluate("04/20/2022 00:00:56","MM/dd/yyyy HH:mm:ss"));
    //  System.out.println(StringToTimestampHiveUDF.evaluate("09/03/2022 22:34:31"));
    // System.out.println(NullIFHiveUDF.evaluate("20220219",""));
    // System.out.println(CastNullIFtoDateFormatHiveUDF.evaluate("20240219","~","yyyyMMdd"));
    // System.out.println("09B11B2022 23:59:59".length());
    // System.out.println(CastDateFormatElseAnotherDateHiveUDF.evaluate("20220219","","99991231","yyyyMMdd"));
    // System.out.println(CoalesceCastToTimestamp0.evaluate("2022-10-20 00:00:56"));
    // System.out.println(CoalesceCastToTimestamp0.evaluate(null));
    // System.out.println(CoalesceCastToTimestamp0.evaluate(""));

    // System.out.println(StringtoDate.evaluate("2012/10/09","yyyy/MM/dd"));
    // System.out.println(SysdatetoDateFormatHiveUDF.evaluate(new Date(2022, 12, 12)));
    // System.out.println(SystimetoIntHiveUDF.evaluate(new Time(124505), "HHmmss"));
    // System.out.println(TimestampTransformerHiveUDF.evaluate(null));
    // System.out.println(StringtoDateFormatHiveUDF.evaluate("20220907","yyyyDDmm"));
    // System.out.println(divide.evaluate(26789,10000));

    // System.out.println(caswhen_multiplethen_HiveUDF.evaluate("J"));
    // System.out.println((CastStringToTimestampFormatHiveUDF.evaluate("2021/05/04
    // 23:45:30.05","yyyy/MM/dd HH:mm:ss.SSS")));

    // System.out.println(Calendar.getInstance().getTime());

    // System.out.println(CastNullIFtoDateFormatHiveUDF.evaluate("20210504","~","yyyyMMdd"));

    // System.out.println(SubstrCharlengthTrimTrailHiveUDF.evaluate("abcde      ",1));

    // System.out.println(NvlIntegerHiveUDF.evaluate(4,3));
    // System.out.println(NvlDoubleHiveUDF.evaluate(null,3.5678));

    // System.out.println(StringtoDateFormatHiveUDF.evaluate("220405","19","yyyyMMdd"));
    //  System.out.println(StringtoDateFormatHiveUDF.evaluate("200405","20","yyyyMMdd"));
    // System.out.println(TimeAsString.evaluate(null));
    System.out.println(CaseWhenGenericHiveUDF.evaluate("abc"," ",null,"abc"));
  }
}
