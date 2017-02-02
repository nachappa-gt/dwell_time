/*
 * Test DatesToMillis Pig UDF written in Scala.
 */

REGISTER '$INSIGHTS_JAR'
REGISTER '$JODA_TIME_JAR'
REGISTER '$SCALAJ_TIME_JAR'
REGISTER '$SCALA_LIBRARY_JAR'

DEFINE DateToMillis example.pig.DateToMillis();

-- Load the data

-- A = load '$INPUT' as (date: chararray);
A = load '../data/date_to_millis.txt' as (
        date: chararray
    );

dump A;


B = foreach A generate
        date,
        DateToMillis(date) as millis;

dump B;

