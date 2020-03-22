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

	// Covariance between two series (double precision values)
	public double covDouble(Jdf.Series se1, Jdf.Series se2)
	{
		double out = 0.0;
		int i;
		int n1 = se1.getLength();
		int n2 = se2.getLength();
		double m1, m2;
		double n;
		double s1 = 0.0;
		if (n1 == n2)
		{
			n = (double)(n1);
			m1 = this.meanDouble(se1);
			m2 = this.meanDouble(se2);
			for (i = 0; i < n1; i++)
			{
				s1 += ((se1.getDouble(i) - m1) * (se2.getDouble(i) - m2));
			}
			out = s1 / n;
		}
		else
		{
			out = 0.0; // not possible to evaluate cov.
		}
		return out;
	}

	// Correlation between two series.
	public double corrDouble(Jdf.Series se1, Jdf.Series se2)
	{
		double out = 0.0;
		out = this.covDouble(se1, se2) / (this.stdDouble(se1) * this.stdDouble(se2));
		return out;
	}

}