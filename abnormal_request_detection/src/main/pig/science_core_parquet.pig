/*
 * Convert Science Foundation Logs from AVRO to Parquet.
 *
 * Copyright (C). 2016  xAD, Inc.  All rights reserved.
 */

-- Register jar files
REGISTER '$AVRO_JAR'
REGISTER '$JSON_SIMPLE_JAR'
REGISTER '$PARQUET_PIG_BUNDLE_JAR'

-- none,lzo,gzip,snappy
-- SET parquet.compression $(COMP);
-- SET parquet.compression SNAPPY;
-- SET parquet.compression lzo;
SET parquet.compression gzip;
-- SET parquet.compression UNCOMPRESSED;

-------------------
-- Load Data
-------------------
-- Enigma/Camus logs
s  = load '$INPUT' using AvroStorage();

-- Save output
-- store s into '$OUTPUT/gzip' using parquet.pig.ParquetStorer();
store s into '$OUTPUT' using parquet.pig.ParquetStorer();


