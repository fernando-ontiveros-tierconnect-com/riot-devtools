package com.tierconnect.utils;

import java.util.Iterator;
import java.util.LinkedHashMap;

/**
 * Created by fernando on 9/16/15.
 */
public class BarGraph
{
	private int rows;
	private int cols;

	private char[][] matrix;

	private Long maxX;
	private Long minX;
	private Long maxY;
	private long minY;
	private Long highValue;
	private Long divider;

	private LinkedHashMap<Long, Long> series;

	public BarGraph( int rows, int cols )
	{
		this.rows = rows;
		this.cols = cols;

		this.maxX = 0L;
		this.minX = 0L;
		this.maxY = 0L;
		this.minY = 0L;

		this.matrix = new char[rows][cols];
		//blank canvas
		for (int i = 0; i < rows; i++ ) {
			for (int j = 0; j < cols; j++ ) {
				matrix[i][j] = ' ';
			}
		}

		this.series = new LinkedHashMap<>();

	}

	public void setSeries(Long valX, Long valY)
	{
		series.put( valX, valY );
	}

	public void drawAxis( Long minX, Long maxX, Long minY, Long maxY)
	{
		highValue = 100L;
		divider = 1L;
		int visibleY = rows - 4;

		if ( maxY > 100) {
			highValue = 1000L;
			divider = 1000L;
		}
		if ( maxY > 1000) {
			highValue = 10000L;
			divider = 1000L;
		}
		if ( maxY > 10000) {
			highValue = 100000L;
			divider = 1000L;
		}
		if ( maxY > 100000) {
			highValue = 1000000L;
			divider = 1000000L;
		}
		if ( maxY > 1000000) {
			highValue = 10000000L;
			divider = 1000000L;
		}
		if ( maxY > 10000000) {
			highValue = 100000000L;
			divider = 1000000L;
		}

		//x axis
		for (int j = 0; j < this.cols; j++) {
			this.matrix[ this.rows -4 ][j] = '-';
		}

		//y axis
		for (int i = 0; i < this.rows; i++) {
			this.matrix[ i][3 ] = '|';
		}
		this.matrix[this.rows-4][3] = '+';

		for (int i = 0; i < this.rows-4; i++) {
			double number = (visibleY-i) * highValue/visibleY;
			number = number / divider;

			setXaxisNumber( number, i, 0);
		}


		Iterator<Long> it = series.keySet().iterator();
		Double col = 0.0;
		Double incr = (0.5*(cols -5) )/series.size();

		while (it.hasNext()) {
			Long blinks = it.next();
			Long things = series.get(blinks);

			Double val = 1.0 * things / highValue * visibleY;
			drawBar( col.intValue(), blinks, val.longValue());

			col += incr;
		}
	}

	public void textout(int x, int y, String t)
	{
		for (int j= 0; j<t.length(); j++) {
			if (j+x < cols) {
				matrix[y][j+x]   = t.charAt( j );
			}
		}
	}

	public void textoutVert(int x, int y, String t)
	{
		for (int i= 0; i<t.length(); i++) {
			if (i+y < rows) {
				matrix[i+y][x] = t.charAt( i );
			}
		}
	}

	public void setXaxisNumber(Double v, int i, int j)
	{
		String value = v.toString();

		matrix[i][j]   = value.charAt( 0 );
		matrix[i][j+1] = value.charAt( 1 );
		matrix[i][j+2] = value.charAt( 2 );
	}

	public void drawAxis( int minX, int maxX, int minY, int maxY)
	{
		drawAxis(
				((Integer)minX).longValue(),
				((Integer)maxX).longValue(),
				((Integer)minY).longValue(),
				((Integer)maxY).longValue() );
	}

	public void drawBar( int col, Long key, Long value)
	{
		int val = value.intValue();
		int posX = 4 + col*2;

		int posY = (this.rows - 4) - val;
		if (posY < 0) posY = 0;

		if (posX >= cols ) return;

		for (int i = posY; i < (this.rows-4); i++) {
			this.matrix[ i ][posX] = (char)(173);
		}

		String str =  key.toString() + "  ";
		this.matrix[ rows-3 ][ posX] = str.charAt( 0 );
		this.matrix[ rows-2 ][ posX] = str.charAt( 1 );
		this.matrix[ rows-1 ][ posX] = str.charAt( 2 );
	}

	public void drawBar( int col, int key, int value )
	{
		drawBar( col, ((Integer) key).longValue(), ((Integer)value).longValue());
	}

	public void display( )
	{
		for (int i = 0; i < rows; i++ )
		{
			for( int j = 0; j < cols; j++ ) {
				System.out.print( Character.toString( matrix[i][j] ));
			}
			System.out.println();
		}
	}

	public int getRows()
	{
		return rows;
	}

	public void setRows( int rows )
	{
		this.rows = rows;
	}

	public int getCols()
	{
		return cols;
	}

	public void setCols( int cols )
	{
		this.cols = cols;
	}

	public char[][] getMatrix()
	{
		return matrix;
	}

	public void setMatrix( char[][] matrix )
	{
		this.matrix = matrix;
	}

	public Long getMaxX()
	{
		return maxX;
	}

	public void setMaxX( Long maxX )
	{
		this.maxX = maxX;
	}

	public Long getMinX()
	{
		return minX;
	}

	public void setMinX( Long minX )
	{
		this.minX = minX;
	}

	public Long getMaxY()
	{
		return maxY;
	}

	public void setMaxY( Long maxY )
	{
		this.maxY = maxY;
	}

	public long getMinY()
	{
		return minY;
	}

	public void setMinY( long minY )
	{
		this.minY = minY;
	}

	public Long getHighValue()
	{
		return highValue;
	}

	public Long getDivider()
	{
		return divider;
	}

	@Override public String toString()
	{
		return "BarGraph{" +
				"cols=" + cols +
				", rows=" + rows +
				'}';
	}
}
