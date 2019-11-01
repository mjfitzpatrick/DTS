package sf.net.mimic.servlet.xmlrpc;

import java.io.IOException;
import java.io.InputStream;

public class ImageRender  {

	public byte[] getRandomImage() {		
		try {
			int idx = (int) (Math.random() * 10) / 2;			
			InputStream fis = Thread.currentThread().getContextClassLoader().getResourceAsStream("/resources/" + idx + ".jpg");
			byte[] buffer = new byte[fis.available()];
			fis.read(buffer);
			fis.close();
			return buffer;
		} catch (IOException ioe) {
			System.out.println(ioe.getMessage());
		}
		return null;
	}

}
