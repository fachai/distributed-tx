 package org.opendaylight.controller.sal.restconf.impl.cnsn.to.json.test; 
 
 import org.junit.After;
 import org.junit.AfterClass;
 import org.junit.Before;
 import org.junit.BeforeClass;
 import org.junit.Test;
 
 public class Temp {
 
    @BeforeClass
    public static void staticInit(){
        System.out.println( "Static Init" );
    }
 
    @Before
    public void testInit(){
        System.out.println( "Test Init - " + this );
    }
 
    @Test
    public void testOne(){
        System.out.println( "Test One - " + this );
    }
 
    @Test
    public void testTwo(){
        System.out.println( "Test Two - " + this );
    }

    @After
    public void testCleanUp(){
        System.out.println( "Test Clean Up - " + this );
    }
 
    @AfterClass
    public static void staticCleanUp(){
        System.out.println( "Static Clean Up" );
    }
 
}
