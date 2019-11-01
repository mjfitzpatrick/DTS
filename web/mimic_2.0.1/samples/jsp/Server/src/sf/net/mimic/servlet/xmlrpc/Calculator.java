package sf.net.mimic.servlet.xmlrpc;

public class Calculator {

	public int add(String n1, String n2) {
		return Integer.valueOf(n1) + Integer.valueOf(n2);
	}

	public int sub(String n1, String n2) {
		return Integer.valueOf(n1) - Integer.valueOf(n2);
	}

	public int mult(String n1, String n2) {
		return Integer.valueOf(n1) * Integer.valueOf(n2);
	}
	
	public int div(String n1, String n2) {
		return Integer.valueOf(n1) / Integer.valueOf(n2);
	}
}
