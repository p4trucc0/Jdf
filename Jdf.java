import java.util.Arrays;
import java.util.ArrayList;
import java.lang.Math;
import java.io.PrintStream;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;

public class Jdf{

	public int rows;
	public ArrayList<String> ColumnNames;
	public ArrayList<Series> Columns;
	public String name; // for upcoming groupby functions.

	// empty constructror
	public Jdf(){
		rows = 0;
		ColumnNames = new ArrayList<String>();
		Columns = new ArrayList<Series>();
	}

	// add a new column.
	public void addEmptyColumn(String cname, String ctype)
	{
		Series series = new Series(ctype, this.rows);
		Columns.add(series);
		ColumnNames.add(cname);
		return;
	}

	// extract another dataframe with given indices.
	public Jdf cutOnIndex(int[] ind2cut)
	{
		Jdf out = new Jdf();
		int i_col;
		int n_col = this.ColumnNames.size();
		for (i_col = 1; i_col < n_col; i_col++)
		{
			out.ColumnNames.add(this.ColumnNames.get(i_col));
			out.Columns.add(this.Columns.get(i_col).extrInd(ind2cut));
		}
		out.rows = ind2cut.length;
		return out;
	}

	// Raw Representation. TODO improve it in pandas-like style
	public void printRaw()
	{
		int i;
		for (i = 0; i < this.ColumnNames.size(); i++)
		{
			System.out.printf("Column %s:\n", this.ColumnNames.get(i));
			this.Columns.get(i).printRaw();
		}
	}

	// read a csv file and fill the current dataframe on the go
	public void fillFromCsv(String csv_filename) throws IOException
	{
		BufferedReader inb = new BufferedReader(new FileReader(csv_filename));
		String linea = "";
		String elem = "";
		String[] lsep;
		int lines_read = 0; // total read lines.
		int seplen;
		int i;
		int prvInt;
		double prvDouble;
		String currentType = "";
		do
		{
			try
			{
				linea = inb.readLine();
			} 
			catch(IOException e)
			{
				// do absolutely nothing.
			}
			if (linea != null)
			{
				lsep = linea.split(",");
				seplen = lsep.length;
				for (i = 0; i < seplen; i++)
				{
					elem = lsep[i].trim(); // remove whitespace
					if (lines_read == 0) // header
					{
						this.ColumnNames.add(elem);
					}
					else
					{
						if (lines_read == 1) // first row, detect type
						{
							currentType = "Double";
							try
							{
								prvDouble = Double.parseDouble(elem);
							}
							catch (NumberFormatException nfe)
							{
								currentType = "String";
							}
							Columns.add(new Series(currentType));
						}
						Columns.get(i).addParse(elem);
					}
					//System.out.printf("%s\t", elem);
				}
				//System.out.printf("\n");
				lines_read++;
			}
		} while (linea != null);
		this.rows = lines_read - 1;
		return;
	}

	// create an empty series.
	public Series emptySeries(String ntype)
	{
		return new Series(ntype);
	}	

	// Series subclass
	public static class Series{
		public int numel;
		public String type;
		public ArrayList<Double> al_double;
		public ArrayList<Integer> al_int;
		public ArrayList<String> al_string;

		// empty constructor
		public Series(String ntype){
			numel = 0;
			this.type = ntype;
			if (ntype.equals("Double")){
				al_double = new ArrayList<Double>();
			}
			if (ntype.equals("Integer")){
				al_int = new ArrayList<Integer>();
			}
			if (ntype.equals("String")){
				al_string = new ArrayList<String>();
			}
		}

		// construct filled string with meaningless elements.
		public Series(String ntype, int nnumel){
			this.numel = nnumel;
			this.type = ntype;
			int i;
			if (ntype.equals("Double")){
				al_double = new ArrayList<Double>();
				for (i = 0; i < this.numel; i++)
				{
					al_double.add(0.0);
				}
			}
			if (ntype.equals("Integer")){
				al_int = new ArrayList<Integer>();
				for (i = 0; i < this.numel; i++)
				{
					al_int.add(0);
				}
			}
			if (ntype.equals("String")){
				al_string = new ArrayList<String>();
				for (i = 0; i < this.numel; i++)
				{
					al_string.add("");
				}
			}
		}

		// add as string
		public void addParse(String s2add)
		{
			if (this.type.equals("Double"))
			{
				this.al_double.add(Double.parseDouble(s2add));
			}
			if (this.type.equals("Integer"))
			{
				this.al_int.add(Integer.parseInt(s2add));
			}
			if (this.type.equals("String"))
			{
				this.al_string.add(s2add);
			}
			this.numel++;
			return;
		}

		// Add element functions. For cutting.
		public void addInteger(int nint)
		{
			if (this.type.equals("Integer"))
			{
				this.al_int.add(nint);
				this.numel++;
			}
			return;
		}

		public void addDouble(double ndouble)
		{
			if (this.type.equals("Double"))
			{
				this.al_double.add(ndouble);
				this.numel++;
			}
			return;
		}

		public void addString(String nString)
		{
			if (this.type.equals("String"))
			{
				this.al_string.add(nString);
				this.numel++;
			}
			return;
		}

		// print content, raw
		public void printRaw()
		{
			int i;
			for (i = 0; i < this.numel; i++)
			{
				if (this.type.equals("Double"))
				{
					System.out.printf("%f ", this.al_double.get(i));
				}
				if (this.type.equals("Integer"))
				{
					System.out.printf("%d ", this.al_int.get(i));
				}
				if (this.type.equals("String"))
				{
					System.out.printf("%s ", this.al_string	.get(i));
				}
			}
			System.out.printf("\n");
			return;
		}

		public int[] asInteger(){
			int n = al_int.size();
			int[] out = new int[n];
			int i = 0;
			for (i = 0; i < n; i ++){
				out[i] = (int) al_int.get(i);
			}
			return out;
		}

		public double[] asDouble(){
			int n = al_double.size();
			double[] out = new double[n];
			int i = 0;
			for (i = 0; i < n; i ++){
				out[i] = (double) al_double.get(i);
			}
			return out;
		}

		// Extract indices
		public Series extrInd(int[] ind2extr)
		{
			int n = ind2extr.length;
			int i;
			Series out = new Series(this.type);
			for (i = 0; i < n; i++)
			{
				if (this.type == "Double")
				{
					out.addDouble(this.al_double.get(ind2extr[i]));
				}
				else
				{
					if (this.type == "Integer")
					{
						out.addInteger(this.al_int.get(ind2extr[i]));
					}
					else
					{
						out.addString(this.al_string.get(ind2extr[i]));
					}
				}
			}
			return out;
		}

	}

}