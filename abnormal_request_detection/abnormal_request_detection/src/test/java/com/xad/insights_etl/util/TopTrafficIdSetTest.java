package com.xad.insights_etl.util;

import org.junit.Test;

/**
 * Created by victor on 1/18/16.
 */
public class TopTrafficIdSetTest {

    public static String PROJ_ROOT = "../..";
    public static String DATA_PATH = PROJ_ROOT + "/src/main/data/top_traffic_src.tsv";

    @Test
    public void testLoad() throws Exception {
        System.out.println("CWD = " + System.getProperty(("user.dir")));
        System.out.println("DATA_PATH = " + DATA_PATH);
        TopTrafficIdSet set = new TopTrafficIdSet();
        set.load(DATA_PATH);
        set.dump(10);

        System.out.println("Test individuslas:");
        String country = "us";
        String entries[] = {"", "-4914162479796953042", "8718322149654552240", "123456"};
        for (String entry: entries) {
            boolean found = set.has(country, entry);
            System.out.println(" + has(" + country + "," + entry + ") = " + found);
        }
    }

}

