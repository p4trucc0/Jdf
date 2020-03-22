package javaDataFrame;
import java.util.Arrays;
import java.util.ArrayList;
import java.lang.Math;
import java.io.PrintStream;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

// Statistic calculations for Jdf and series classes

public class JdfStats
{
	
	public JdfStats()
	{
		// do nothing.
	}

	// Calculates mean of a double series
	public double meanDouble(Jdf.Series s_in)
	{
		double s1 = 0.0;
		double s0 = 0.0;
		double out;
		int i;
		int n = s_in.getLength();
		for (i = 0; i < n; i++)
		{
			s0 += 1.0;
			s1 += s_in.getDouble(i);
		}
		out = s1 / s0;
		return out;
	}

	// Variance
	public double stdDouble(Jdf.Series s_in)
	{
		double s2 = 0.0;
		double s1 = 0.0;
		double s0 = 0.0;
		double out;
		int i;
		int n = s_in.getLength();
		if (n > 0)
		{
			for (i = 0; i < n; i++)
			{
				s0 += 1.0;
				s1 += s_in.getDouble(i);
				s2 += (s_in.getDouble(i)) * (s_in.getDouble(i));
			}
			out = Math.pow(((1/s0) * (s2 - s0*Math.pow((s1/s0),2))), 0.5);
		}
		else
		{
			out = 0.0;
		}
		return out;
	}

	public double varDouble(Jdf.Series s_in)
	{
		double out = Math.pow(this.stdDouble(s_in), 2);
		return out;
	}

}