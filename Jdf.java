import java.util.Arrays;
import java.util.ArrayList;
import java.lang.Math;
import java.io.PrintStream;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

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

	// remove existing column
	public void removeColumn(String cname)
	{
		int i;
		for (i = 0; i < this.ColumnNames.size(); i++)
		{
			if (this.ColumnNames.get(i).equals(cname))
			{
				this.ColumnNames.remove(i);
				this.Columns.remove(i);
				break;
			}
		}
		return;
	}

	// extract another dataframe with given indices.
	public Jdf cutOnIndex(int[] ind2cut)
	{
		Jdf out = new Jdf();
		int i_col;
		int n_col = this.ColumnNames.size();
		for (i_col = 0; i_col < n_col; i_col++)
		{
			out.ColumnNames.add(this.ColumnNames.get(i_col));
			out.Columns.add(this.Columns.get(i_col).extrInd(ind2cut));
		}
		out.rows = ind2cut.length;
		return out;
	}

	// Append other df. For now, only same columns.
	public void append(Jdf other)
	{
		Boolean permitted = true;
		int i;
		int n_col = this.ColumnNames.size();
		int n_oth = other.ColumnNames.size();
		if (n_col == n_oth)
		{
			for (i = 0; i < n_col; i++)
			{
				if (this.ColumnNames.get(i) != other.ColumnNames.get(i))
				{
					permitted = false;
				}
			}
			if (permitted)
			{
				for (i = 0; i < n_col; i++)
				{
					this.Columns.get(i).append(other.Columns.get(i));
				}
			}
		}
		this.rows += other.rows;
		return;
	}

	// Extract another dataframe based on string value
	public Jdf cutOnValueString(String colname, String colval)
	{
		Jdf out = new Jdf();
		int i;
		int n_col = this.ColumnNames.size();
		int i_f = n_col + 1;
		for (i = 0; i < n_col; i++)
		{
			if (this.ColumnNames.get(i).equals(colname))
			{
				if (this.Columns.get(i).type.equals("String"))
				{
					i_f = i;
				}
				break;
			}
		}
		if (i_f < n_col)
		{
			int[] ind_to_cut = this.Columns.get(i_f).indWhereString(colval);
			out = this.cutOnIndex(ind_to_cut);
		}
		return out;
	}

	// Extract another dataframe based on string value
	public double[] getColAsDouble(String colname)
	{
		double[] out = new double[0];
		int i;
		int n_col = this.ColumnNames.size();
		int i_f = n_col + 1;
		for (i = 0; i < n_col; i++)
		{
			if (this.ColumnNames.get(i).equals(colname))
			{
				if (this.Columns.get(i).type.equals("Double"))
				{
					i_f = i;
				}
				break;
			}
		}
		if (i_f < n_col)
		{
			out = this.Columns.get(i).asDouble();
		}
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

	// Prettier representation
	public String prettyString(int c_width)
	{
		int i_c; // cols
		int i_r; // rows
		int n_c = this.ColumnNames.size();
		String prv = "";
		String out = "";
		for (i_c = 0; i_c < n_c; i_c++)
		{
			prv = this.ColumnNames.get(i_c);
			while (prv.length() < c_width)
			{
				prv = " ".concat(prv);
			}
			out = out.concat(prv.concat(" "));
		}
		out = out.concat("\n");
		// down to rows.
		for (i_r = 0; i_r < this.rows; i_r++)
		{
			for (i_c = 0; i_c < n_c; i_c++)
			{
				prv = this.Columns.get(i_c).retrieveParse(i_r, c_width, true);
				out = out.concat(prv.concat(" "));
			}
			out = out.concat("\n");
		}
		return out;
	}

	// read a csv file and fill the current dataframe on the go
	public void fillFromCsv(String csv_filename) throws IOException
	{
		BufferedReader inb = new BufferedReader(new FileReader(csv_filename));
		this.fillFromBufferReader(inb);
	}

	// read csv file from url
	public void fillFromUrl(String url_str) throws IOException
	{
		URL url = new URL(url_str);
		BufferedReader inb = new BufferedReader(new InputStreamReader(url.openStream()));
		this.fillFromBufferReader(inb);
	}

	public void fillFromBufferReader(BufferedReader inb) throws IOException
	{
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

		// Gets element from specified position and returns it as string (no decimals)
		public String retrieveParse(int ind, int out_len, boolean double_as_int) // out_len: length of the output string.
		{
			String out = "";
			if (ind >= this.numel)
			{
				out = "OOB";
			}
			else
			{
				if (this.type.equals("Double"))
				{
					if (double_as_int)
					{
						out = this.al_double.get(ind).toString().split("\\.")[0];
					}
					else
					{
						out = this.al_double.get(ind).toString();
					}
				}
				if (this.type.equals("Integer"))
				{
					out = this.al_int.get(ind).toString();
				}
				if (this.type.equals("String"))
				{
					out = this.al_string.get(ind);
				}
				while (out.length() < out_len)
				{
					out = " ".concat(out);
				}
			}
			return out;
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

		// retrieve unique values of strings contained in series.
		public String[] uniqueStrings()
		{
			ArrayList<String> str_out = new ArrayList<String>();
			int n;
			int i, j;
			boolean already_there;
			if (this.type == "String")
			{
				for (i = 0; i < this.numel; i++) // cycle on elements of this
				{
					already_there = false;
					for (j = 0; j < str_out.size(); j++)
					{
						if (str_out.get(j).equals(this.al_string.get(i)))
						{
							already_there = true;
							break;
						}
					}
					if (!already_there)
					{
						str_out.add(this.al_string.get(i));
					}
				}
			}
			else
			{
				n = 0;
			}
			n = str_out.size();
			String[] out = new String[n];
			for (i = 0; i < n; i++)
			{
				out[i] = str_out.get(i);
			}
			return out;
		}

		// Return indices corresponding to specified value. For now only String and Integer
		public int[] indWhereString(String cval)
		{
			ArrayList<Integer> ind_al = new ArrayList<Integer>();
			int i; 
			int n;
			if (this.type == "String")
			{
				for (i = 0; i < numel; i++)
				{
					if (al_string.get(i).equals(cval))
					{
						ind_al.add(i);
					}
				}
				n = ind_al.size();
			}
			else 
			{
				n = 0;
			}
			int[] out = new int[n];
			for (i = 0; i < n; i++)
			{
				out[i] = ind_al.get(i);
			}
			return out;
		}

		// Return indices corresponding to specified value. For now only String and Integer
		public int[] indWhereInteger(int cval)
		{
			ArrayList<Integer> ind_al = new ArrayList<Integer>();
			int i; 
			int n;
			if (this.type == "Integer")
			{
				for (i = 0; i < numel; i++)
				{
					if (al_int.get(i) == cval)
					{
						ind_al.add(i);
					}
				}
				n = ind_al.size();
			}
			else 
			{
				n = 0;
			}
			int[] out = new int[n];
			for (i = 0; i < n; i++)
			{
				out[i] = ind_al.get(i);
			}
			return out;
		}

		// Append other
		public void append(Series other)
		{
			int i, n;
			if (this.type == other.type)
			{
				if (this.type == "Integer")
				{
					n = other.numel;
					for (i = 0; i < n; i++)
					{
						this.addInteger(other.al_int.get(i));
					}
				}
				if (this.type == "Double")
				{
					n = other.numel;
					for (i = 0; i < n; i++)
					{
						this.addDouble(other.al_double.get(i));
					}
				}
				if (this.type == "String")
				{
					n = other.numel;
					for (i = 0; i < n; i++)
					{
						this.addString(other.al_string.get(i));
					}
				}
			}
			return;
		}

	}

}