/**
 * Mix Java&Scala : http://ted-gao.blogspot.com/2011/09/mixing-scala-and-java-in-project.html
 */
package feiyu.com.util;

public class JavaCallsScala {
    public static void main(String[] args) {
        ScalaHelloWorld scalaHW = new ScalaHelloWorld();
        scalaHW.greetFromScala();
    }
}
