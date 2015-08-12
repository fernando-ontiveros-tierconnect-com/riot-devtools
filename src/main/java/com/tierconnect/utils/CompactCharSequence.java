package com.tierconnect.utils;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.stream.IntStream;

/**
 * Created by fernando on 8/11/15.
 */

public class CompactCharSequence implements CharSequence, Serializable
{
	static final long serialVersionUID = 1L;

	private static final String ENCODING = "ISO-8859-1";
	private final int offset;
	private final int end;
	private final byte[] data;

	public CompactCharSequence(String str) {
		try {
			data = str.getBytes(ENCODING);
			offset = 0;
			end = data.length;
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("Unexpected: " + ENCODING + " not supported!");
		}
	}

	private CompactCharSequence(byte[] data, int offset, int end) {
		this.data = data;
		this.offset = offset;
		this.end = end;
	}

	public char charAt(int index) {
		int ix = index+offset;
		if (ix >= end) {
			throw new StringIndexOutOfBoundsException("Invalid index " +
					index + " length " + length());
		}
		return (char) (data[ix] & 0xff);
	}

	@Override public CharSequence subSequence( int start, int end )
	{
		if (start < 0 || end >= (this.end-offset)) {
			throw new IllegalArgumentException("Illegal range " +
					start + "-" + end + " for sequence of length " + length());
		}
		return new CompactCharSequence(data, start + offset, end + offset);
	}

	@Override public IntStream chars()
	{
		return null;
	}

	@Override public IntStream codePoints()
	{
		return null;
	}

	public int length() {
		return end - offset;
	}

	public String toString() {
		try {
			return new String(data, offset, end-offset, ENCODING);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("Unexpected: " + ENCODING + " not supported");
		}
	}

}
