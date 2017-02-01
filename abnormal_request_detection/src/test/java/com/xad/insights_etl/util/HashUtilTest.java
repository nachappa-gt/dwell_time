/**
 * Copyright (C) 2016.  xAd Inc.  All Rights Reserved.
 */
package com.xad.insights_etl.util;

import java.security.NoSuchAlgorithmException;

import org.junit.Test;

/**
 * @author victor
 *
 */
public class HashUtilTest {

	/**
	 * Test method for {@link com.xad.insights_etl.util.HashUtil#sha256(java.lang.String)}.
	 */
	@Test
	public void testSha256() {
		String inputs[] = {
				"003e7ac2-4e19-4b86-93f8-45808f135167",
				"005327a4-be7d-44ba-b1cd-64a8ddf2d071"
		};
		for (String input : inputs) {
            try {
	            String hex = HashUtil.sha256(input);
				System.out.println(input + " => " + hex);
            } catch (NoSuchAlgorithmException e) {
	            e.printStackTrace();
            }
		}
	}
}
