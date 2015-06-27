package smallworld.util;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

public class ProgressInputStream extends FilterInputStream {
		
		long length;
		long progress;
		double percentage;
		long threshold;
		long accumulatedChange;
		ChangeListener listener;
		ChangeEvent event;
		
		public ProgressInputStream(InputStream in, long fileLength, ChangeListener listener) {
			super(in);
			this.length = fileLength;
			this.progress = 0;
			this.percentage = 0;
			this.accumulatedChange = 0;
			this.threshold = fileLength / 100;
			this.listener = listener;
			this.event = new ChangeEvent(this);
		}

		private void progressChanged(long change) {
			progress += change;
			accumulatedChange += change;
			if (accumulatedChange > threshold) {
				double newPercentage = (double) progress / length;
				percentage = newPercentage;
				listener.stateChanged(event);
				accumulatedChange = 0;
			}
		}
		
		public double getPercentage() {
			return percentage;
		}
		
		@Override
		public int read() throws IOException {
			final int i = super.read();
			if (i != -1) {
				progressChanged(1);
			}
			return i;
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			final int i = super.read(b, off, len);
			if (i > 0) {
				progressChanged(i);
			}
			return i;
		}

		@Override
		public long skip(long n) throws IOException {
			final long i = super.skip(n);
			if (i > 0) {
				progressChanged(i);
			}
			return i;
		}

	}