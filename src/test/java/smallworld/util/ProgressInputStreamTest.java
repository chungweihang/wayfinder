package smallworld.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.junit.Test;

import smallworld.util.ProgressInputStream;

public class ProgressInputStreamTest {

	@Test
	public void test() throws IOException {
		File f = new File("data/dblp.xml");
		long length = f.length();
		
		try (ProgressInputStream mis = new ProgressInputStream(new FileInputStream(f), length,
				new ChangeListener() {

					@Override
					public void stateChanged(ChangeEvent e) {
						ProgressInputStream is = (ProgressInputStream) e.getSource();
						System.out.println(is.getPercentage());
					}
			
		})) {
			byte[] bytes = new byte[1048576];
			while (mis.read(bytes, 0, bytes.length) != -1) {}
		}
	}
}
