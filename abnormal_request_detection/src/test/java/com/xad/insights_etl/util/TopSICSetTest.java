package com.xad.insights_etl.util;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by victor on 1/18/16.
 */
public class TopSICSetTest {


    public static String PROJ_ROOT = "../..";
    public static String DATA_PATH = PROJ_ROOT + "/src/main/data/top_sic.tsv";

    @Test
    public void testLoad() throws Exception {
        System.out.println("CWD = " + System.getProperty(("user.dir")));
        System.out.println("DATA_PATH = " + DATA_PATH);
        TopSICSet set = new TopSICSet();
        set.load(DATA_PATH);
        set.dump(10);

        System.out.println("Test individuslas:");
        String country = "us";
        String sics[] = {"", "822101", "599963", "123456"};
        for (String sic: sics) {
            boolean found = set.has(country, sic);
            System.out.println(" + has(" + country + "," + sic + ") = " + found);
        }
    }
}