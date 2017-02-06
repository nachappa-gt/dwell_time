/*
 * Read parquet files.
 *
 * Copyright (C). 2016  xAD, Inc.  All rights reserved.
 */

-- Register jar files
REGISTER '$PARQUET_PIG_BUNDLE_JAR'

-- none,lzo,gzip,snappy
-- SET parquet.compression snappy;
-- SET parquet.compression lzo;
-- SET parquet.compression gzip;
-- SET parquet.compression UNCOMPRESSED;

-------------------
-- Load Data
-------------------
-- Enigma/Camus logs
s  = load '$INPUT' using parquet.pig.ParquetLoader();

X = LIMIT s 50; dump X;

