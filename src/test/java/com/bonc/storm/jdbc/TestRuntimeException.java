package com.bonc.storm.jdbc;

import org.junit.Test;

public class TestRuntimeException {
	
	@Test
	public void testException(){
		
		try {
			throw new RuntimeException("runtime exception");
		} catch (Exception e) {
			System.out.println("runtime exception");
		}
		
		System.out.println("end");
		
		
	}
	
}
