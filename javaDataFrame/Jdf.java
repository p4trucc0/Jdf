package javaDataFrame;
import java.util.Arrays;
import java.util.ArrayList;
import java.lang.Math;
import java.io.PrintStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.StringReader;
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

	// Return identical object
	public Jdf copy()
	{
		Jdf out = new Jdf();
		int ncol = this.ColumnNames.size();
		int i;
		out.rows = this.rows;
		for (i = 0; i < ncol; i++)
		{
			out.addColumn(this.ColumnNames.get(i), this.Columns.get(i).copy());
		}
		return out;
	}

	// add a new column.
	public void addEmptyColumn(String cname, String ctype)
	{
		Series series = new Series(ctype, this.rows);
		Columns.add(series);
		ColumnNames.add(cname);
		return;
	}


	// TODO: Check if the current name is already in names.
	public void addColumn(String cname, Series nse)
	{
		if (nse.numel == this.rows)
		{
			this.ColumnNames.add(cname);
			this.Columns.add(nse);
		}
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

	// get a single element from a column.
	public String getParse(String cname, int ind)
	{
		if (!this.hasColByName(cname))
		{
			return "";
		}
		else
		{
			return this.getColByName(cname).getParse(ind);
		}
	}

	// Safe method.
	public void setParse(String cname, int ind, String s2add)
	{
		if (!this.hasColByName(cname))
		{
			return;
		}
		else
		{
			this.getColByName(cname).setParse(ind, s2add);
			return;
		}
	}

	// Override by default
	public void setParseOverride(String cname, int ind, String s2add)
	{
		if (!this.hasColByName(cname))
		{
			return;
		}
		else
		{
			if (ind < this.rows)
			{
				this.getColByName(cname).setParse(ind, s2add);
			}
			else // must fill other columns, too.
			{
				int n = this.Columns.size(); 
				int i;
				for (i = 0; i < n; i++)
				{
					//System.out.printf("Column %s:\n", this.ColumnNames.get(i));
					if (this.ColumnNames.get(i).equals(cname))
					{
						this.Columns.get(i).setParseOverride(ind, s2add);
					}
					else
					{
						if (this.Columns.get(i).type.equals("String"))
						{
							this.Columns.get(i).setParseOverride(ind, "");
						}
						else
						{
							this.Columns.get(i).setParseOverride(ind, "NaN");
						}
					}
					//this.Columns.get(i).printRaw();
				}
				this.rows = ind + 1;
			}
			return;
		}
	}

	// Checks whether the current jdf has a column with expected name
	public boolean hasColByName(String cname)
	{
		int i;
		boolean hasit = false;
		for (i = 0; i < this.ColumnNames.size(); i++)
		{
			if (this.ColumnNames.get(i).equals(cname))
			{
				hasit = true;
				break;
			}
		}
		return hasit;
	}

	public boolean hasColByNameAndType(String cname, String ctype)
	{
		boolean hasit = false;
		boolean correct_type = false;
		boolean out;
		hasit = this.hasColByName(cname);
		if (hasit)
		{
			Series ts = this.getColByName(cname);
			if (ts.type.equals(ctype))
			{
				correct_type = true;
			}
			else
			{
				correct_type = false;
			}
		}
		out = hasit && correct_type;
		return out;
	}

	public Series getColByName(String cname)
	{
		int i;
		int i_fnd = 0;
		boolean found = false;
		for (i = 0; i < this.ColumnNames.size(); i++)
		{
			if (this.ColumnNames.get(i).equals(cname))
			{
				found = true;
				i_fnd = i;
				break;
			}
		}
		if (found)
		{
			return this.Columns.get(i_fnd);
		}
		else
		{
			return new Series("Double", this.rows);
		}
	}

	// TODO: Error handling is needed here!
	public void mathOpScalar(String dst_name, String src_name, double operand, String operator)
	{
		Series s1 = this.getColByName(src_name).mathOpScalar(operand, operator);
		this.addColumn(dst_name, s1);
		return;
	}

	public void mathOpTwoCol(String dst_name, String src1_name, String src2_name, String operator)
	{
		Series s1 = this.getColByName(src1_name).mathOpSeries(this.getColByName(src2_name), operator);
		this.addColumn(dst_name, s1);
		return;
	}

	public void diffColumn(String dst_name, String src_name)
	{
		Series s1 = this.getColByName(src_name).diff();
		this.addColumn(dst_name, s1);
		return;
	}

	public void csumColumn(String dst_name, String src_name)
	{
		Series s1 = this.getColByName(src_name).csum();
		this.addColumn(dst_name, s1);
		return;
	}

	public void dratColumn(String dst_name, String src_name)
	{
		Series s1 = this.getColByName(src_name).drat();
		this.addColumn(dst_name, s1);
		return;
	}

	public void cmltColumn(String dst_name, String src_name)
	{
		Series s1 = this.getColByName(src_name).cmlt();
		this.addColumn(dst_name, s1);
		return;
	}

	public void sortOnColumn(String cname, boolean ascending)
	{
		int[] sort_ind = getColByName(cname).sort(ascending);
		this.selfCutOnIndex(sort_ind);
		return;
	}

	// Retrieve a list of String (i.e. groupable) fields
	public String[] getStringFields()
	{
		ArrayList<String> str_out = new ArrayList<String>();
		int n;
		int i;
		for (i = 0; i < this.ColumnNames.size(); i++)
		{
			if (this.Columns.get(i).type.equals("String"))
			{
				str_out.add(this.ColumnNames.get(i));
			}
		}
		n = str_out.size();
		String[] out = new String[n];
		for (i = 0; i < n; i++)
		{
			out[i] = str_out.get(i);
		}
		return out;
	}

	// Retrieve a list of Double fields
	public String[] getDoubleFields()
	{
		ArrayList<String> str_out = new ArrayList<String>();
		int n;
		int i;
		for (i = 0; i < this.ColumnNames.size(); i++)
		{
			if (this.Columns.get(i).type.equals("Double"))
			{
				str_out.add(this.ColumnNames.get(i));
			}
		}
		n = str_out.size();
		String[] out = new String[n];
		for (i = 0; i < n; i++)
		{
			out[i] = str_out.get(i);
		}
		return out;
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

	// like previous one but on self.
	public void selfCutOnIndex(int[] ind2cut)
	{
		int i_col;
		int n_col = this.ColumnNames.size();
		for (i_col = 0; i_col < n_col; i_col++)
		{
			Series ns = this.Columns.get(i_col);
			this.Columns.set(i_col, ns.extrInd(ind2cut));
		}
		this.rows = ind2cut.length;
		return;
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
		String out = "   I"; // index.
		String pri = "";
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
			pri = new Integer(i_r).toString();
			while (pri.length() < 4)
			{
				pri = " ".concat(pri);
			}
			out = out.concat(pri);
			for (i_c = 0; i_c < n_c; i_c++)
			{
				prv = this.Columns.get(i_c).retrieveParse(i_r, c_width, false);
				out = out.concat(prv.concat(" "));
			}
			out = out.concat("\n");
		}
		return out;
	}

	// raw csv string representation (for exporting)
	// TODO: Parametrize sepator.
	public String csvString()
	{
		int i_c; // cols
		int i_r; // rows
		int n_c = this.ColumnNames.size();
		String prv = "";
		String out = "";
		for (i_c = 0; i_c < n_c; i_c++)
		{
			prv = this.ColumnNames.get(i_c);
			if (i_c < n_c - 1)
			{
				out = out.concat(prv.concat(","));
			}
			else
			{
				out = out.concat(prv);
			}
		}
		if (n_c > 0)
		{
			out = out.concat("\n");
		}
		// down to rows.
		for (i_r = 0; i_r < this.rows; i_r++)
		{
			for (i_c = 0; i_c < n_c; i_c++)
			{
				prv = this.Columns.get(i_c).retrieveParse(i_r, 0, false);
				if (i_c < n_c - 1)
				{
					out = out.concat(prv.concat(","));
				}
				else
				{
					out = out.concat(prv);
				}
			}
			if (i_r < this.rows - 1) // do not create final newline
			{
				out = out.concat("\n");
			}
		}
		return out;
	}

	public void resetContent()
	{
		rows = 0;
		ColumnNames = new ArrayList<String>();
		Columns = new ArrayList<Series>();
	}

	// Write on csv.
	public void writeCsv(String csv_filename) throws IOException
	{
		String scsv = this.csvString();
		BufferedWriter bwrt = new BufferedWriter(new FileWriter(csv_filename, false));
		bwrt.append(scsv);
		bwrt.close();
		return;
	}

	// read a csv file and fill the current dataframe on the go
	public void fillFromCsv(String csv_filename) throws IOException
	{
		this.resetContent();
		BufferedReader inb = new BufferedReader(new FileReader(csv_filename));
		this.fillFromBufferReader(inb);
	}

	// read csv file from url
	public void fillFromUrl(String url_str) throws IOException
	{
		this.resetContent();
		URL url = new URL(url_str);
		BufferedReader inb = new BufferedReader(new InputStreamReader(url.openStream()));
		this.fillFromBufferReader(inb);
	}

	public void fillFromString(String in_str) throws IOException
	{
		this.resetContent();
		BufferedReader inb = new BufferedReader(new StringReader(in_str));
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
		int n_col;
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
				if (lines_read == 0)
				{
					n_col = seplen;
				}
				else
				{
					n_col = this.ColumnNames.size();
				}
				for (i = 0; i < n_col; i++)
				{
					if (i < seplen)
					{
						elem = lsep[i].trim(); // remove whitespace
					}
					else
					{
						elem = "";
					}
					//System.out.printf("%s\n", elem);
					if (lines_read == 0) // header
					{
						this.ColumnNames.add(elem);
					}
					else
					{
						if (elem.length() > 0)
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
						else
						{
							if (lines_read == 1)
							{
								currentType = "String";
								Columns.add(new Series(currentType));
							}
							Columns.get(i).addParse(elem);
						}
					}
					//System.out.printf("%s\t", elem);
				}
				//System.out.printf("\n");
				lines_read++;
			}
		} while (linea != null);
		if (lines_read > 0)
		{
			this.rows = lines_read - 1;
		} else {
			this.rows = 0;
		}
		return;
	}

	// Add Day value
	public void addDaysFromData()
	{
		int d_year, d_month, d_day;
		boolean data_available = false;
		String prv_str;
		int i;
		int i_d = 0;
		int days;
		int ind_g = this.ColumnNames.size();
		for (i = 0; i < this.ColumnNames.size(); i++)
		{
			if (this.ColumnNames.get(i).equals("data"))
			{
				i_d = i;
				data_available = true;
				break;
			}
		}
		if (data_available)
		{
			this.addEmptyColumn("giorni", "Double");
			for (i = 0; i < this.rows; i++)
			{
				prv_str = this.Columns.get(i_d).retrieveParse(i, 0, false);
				d_year = Integer.parseInt(prv_str.split(" ")[0].split("T|-")[0]);
				d_month = Integer.parseInt(prv_str.split(" ")[0].split("T|-")[1]);
				d_day = Integer.parseInt(prv_str.split(" ")[0].split("T|-")[2]);
				days = (366*(d_year - 2020));
				switch (d_month)
				{
					case 1:
						days += 0;
						break;
					case 2:
						days += 31;
						break;
					case 3:
						days += 60;
						break;
					case 4:
						days += 91;
						break;
					case 5:
						days += 121;
						break;
					case 6:
						days += 152;
						break;
					case 7:
						days += 182;
						break;
					case 8:
						days += 213;
						break;
					case 9:
						days += 244;
						break;
					case 10:
						days += 274;
						break;
					case 11:
						days += 305;
						break;
					case 12:
						days += 335;
						break;
					default:
						break;
				}
				days += d_day;
				this.Columns.get(ind_g).al_double.set(i, new Double(days));
			}
		}
		return;
	}

	public void replaceNaN(String cname, double v2r)
	{
		if (this.hasColByNameAndType(cname, "Double"))
		{
			this.getColByName(cname).replaceNanDouble(v2r);
		}
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

		public Series(String[] stringv)
		{
			this.numel = stringv.length;
			this.type = "String";
			this.al_string = new ArrayList<String>();
			int i;
			for (i = 0; i < this.numel; i++)
			{
				this.al_string.add(stringv[i]);
			}
		}

		// returns an identical object to current.
		public Series copy()
		{
			Series out = new Series(this.type);
			int i;
			int n = this.getLength();
			switch (this.type)
			{
				case "Double":
					for (i = 0; i < n; i++)
						out.addDouble(this.getDouble(i));
					break;
				case "String":
					for (i = 0; i < n; i++)
						out.addString(this.getString(i));
					break;
				case "Integer":
					for (i = 0; i < n; i++)
						out.addInteger(this.getInteger(i));
					break;
				default:
					break;
			}
			return out;
		}

		// Get length
		public int getLength()
		{
			int out = 0;
			if (this.type.equals("Double"))
			{
				out = this.al_double.size();
			}
			if (this.type.equals("String"))
			{
				out = this.al_string.size();
			}
			if (this.type.equals("Integer"))
			{
				out = this.al_int.size();
			}
			return out;
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

		public void setParse(int ind, String s2set)
		{
			if (ind >= this.numel)
			{
				return; // error.
			}
			if (this.type.equals("Double"))
			{
				this.al_double.set(ind, Double.parseDouble(s2set));
			}
			if (this.type.equals("Integer"))
			{
				this.al_int.set(ind, Integer.parseInt(s2set));
			}
			if (this.type.equals("String"))
			{
				this.al_string.set(ind, s2set);
			}
			return;
		}

		// adds element even if outside bounds.
		public void setParseOverride(int ind, String s2set)
		{
			while (ind >= this.numel)
			{
				if (this.type == "String")
				{
					this.addParse(""); // check behavior with Double
				}
				else
				{
					this.addParse("NaN");
				}
			}
			this.setParse(ind, s2set);
		}

		public String getParse(int ind)
		{
			return this.retrieveParse(ind, 0, false);
		}

		// Gets element from specified position and returns it as string (no decimals)
		public String retrieveParse(int ind, int out_len, boolean double_as_int) // out_len: length of the output string.
		{
			String out = "";
			if (ind >= this.numel)
			{
				out = "";
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

		// TODO setParse and getParse
		public void setDouble(int ind, Double nel)
		{
			if (this.type.equals("Double"))
			{
				this.al_double.set(ind, nel);
			}
			return;
		}

		public void setInteger(int ind, Integer nel)
		{
			if (this.type.equals("Integer"))
			{
				this.al_int.set(ind, nel);
			}
			return;
		}

		public void setString(int ind, String nel)
		{
			if (this.type.equals("String"))
			{
				this.al_string.set(ind, nel);
			}
			return;
		}

		public double getDouble(int ind)
		{
			return this.al_double.get(ind);
		}

		public int getInteger(int ind)
		{
			return this.al_int.get(ind);
		}

		public String getString(int ind)
		{
			return this.al_string.get(ind);
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

		public boolean hasString(String cval)
		{
			boolean out;
			int[] ni = this.indWhereString(cval);
			if (ni.length > 0)
			{
				out = true;
			}
			else
			{
				out = false;
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

		// Return an array of integers corresponding to sort order
		public int[] sort(boolean ascending)
		{
			ArrayList<Integer> ind_al = new ArrayList<Integer>();
			int i; 
			int n;
			int j;
			boolean added = false;
			if ((this.type == "Double") || (this.type == "String") || (this.type == "Integer"))
			{
				if (this.type == "Double")
				{
					ArrayList<Double> elem_al = new ArrayList<Double>();
					for (i = 0; i < this.numel; i++)
					{
						added = false;
						if (i == 0)
						{
							elem_al.add(this.al_double.get(i));
							ind_al.add(i);
							added = true;
						}
						else
						{
							for (j = 0; j < ind_al.size(); j++)
							{
								if (ascending)
								{
									if (elem_al.get(j) > this.al_double.get(i))
									{
										elem_al.add(j, this.al_double.get(i));
										ind_al.add(j, i);
										added = true;
										break;
									}
								}	
								else
								{
									if (elem_al.get(j) < this.al_double.get(i))
									{
										elem_al.add(j, this.al_double.get(i));
										ind_al.add(j, i);
										added = true;
										break;
									}
								}
							}
							if (!added)
							{
								elem_al.add(this.al_double.get(i));
								ind_al.add(i);
								added = true;
							}
						}
					}
				}
				if (this.type == "Integer")
				{
					ArrayList<Integer> elem_al = new ArrayList<Integer>();
					for (i = 0; i < this.numel; i++)
					{
						added = false;
						if (i == 0)
						{
							elem_al.add(this.al_int.get(i));
							ind_al.add(i);
							added = true;
						}
						else
						{
							for (j = 0; j < ind_al.size(); j++)
							{
								if (ascending)
								{
									if (elem_al.get(j) > this.al_int.get(i))
									{
										elem_al.add(j, this.al_int.get(i));
										ind_al.add(j, i);
										added = true;
										break;
									}
								}	
								else
								{
									if (elem_al.get(j) < this.al_int.get(i))
									{
										elem_al.add(j, this.al_int.get(i));
										ind_al.add(j, i);
										added = true;
										break;
									}
								}
							}
							if (!added)
							{
								elem_al.add(this.al_int.get(i));
								ind_al.add(i);
								added = true;
							}
						}
					}
				}
				if (this.type == "String")
				{
					ArrayList<String> elem_al = new ArrayList<String>();
					for (i = 0; i < this.numel; i++)
					{
						added = false;
						if (i == 0)
						{
							elem_al.add(this.al_string.get(i));
							ind_al.add(i);
							added = true;
						}
						else
						{
							for (j = 0; j < ind_al.size(); j++)
							{
								if (ascending)
								{
									if (elem_al.get(j).compareTo(this.al_string.get(i)) > 0)
									{
										elem_al.add(j, this.al_string.get(i));
										ind_al.add(j, i);
										added = true;
										break;
									}
								}	
								else
								{
									if (elem_al.get(j).compareTo(this.al_string.get(i)) < 0)
									{
										elem_al.add(j, this.al_string.get(i));
										ind_al.add(j, i);
										added = true;
										break;
									}
								}
							}
							if (!added)
							{
								elem_al.add(this.al_string.get(i));
								ind_al.add(i);
								added = true;
							}
						}
					}
				}
			}
			else 
			{
				for (i = 0; i < this.numel; i++)
				{
					ind_al.add(i);
				}
			}
			int[] out = new int[this.numel];
			for (i = 0; i < this.numel; i++)
			{
				out[i] = ind_al.get(i);
			}
			return out;
		}

		// Math operations on self and a scalar. For double only (for now)
		// TODO: Implement operations for integers
		// TODO: adapt to setters and getters.
		public Series mathOpScalar(double operand, String operator)
		{
			Series out;
			if (this.type == "Double")
			{
				out = new Series(this.type, this.numel);
				int i;
				double prv;
				switch (operator)
				{
					case "+":
						for (i = 0; i < this.numel; i++)
						{
							prv = this.getDouble(i) + operand;
							out.setDouble(i, prv);
						}
						break;
					case "-":
						for (i = 0; i < this.numel; i++)
						{
							prv = this.getDouble(i) - operand;
							out.setDouble(i, prv);
						}
						break;
					case "*":
						for (i = 0; i < this.numel; i++)
						{
							prv = this.getDouble(i) * operand;
							out.setDouble(i, prv);
						}
						break;
					case "/":
						for (i = 0; i < this.numel; i++)
						{
							prv = this.getDouble(i) / operand;
							out.setDouble(i, prv);
						}
						break;
					case "^":
						for (i = 0; i < this.numel; i++)
						{
							prv = Math.pow(this.getDouble(i),operand);
							out.setDouble(i, prv);
						}
						break;
					case "\\":
						for (i = 0; i < this.numel; i++)
						{
							prv = operand / this.getDouble(i);
							out.setDouble(i, prv);
						}
						break;
					default:
						for (i = 0; i < this.numel; i++)
						{
							prv = this.getDouble(i);
							out.setDouble(i, prv);
						}
						break;
				}
			}
			else
			{
				out = new Series(this.type, this.numel);
			}
			return out;
		}

		// element-by-element ratio
		public Series drat()
		{
			Series out;
			int i;
			if (this.type == "Double")
			{
				out = new Series("Double", this.numel);
				for (i = 0; i < this.numel; i++)
				{
					if (i == 0)
					{
						out.setDouble(0, 1.0);
					}
					else
					{
						out.setDouble(i, this.getDouble(i)/this.getDouble(i - 1));
					}
				}
			}
			else
			{
				out = new Series("Double", this.numel);
			}
			return out;
		}

		public Series diff()
		{
			Series out;
			int i;
			if (this.type == "Double")
			{
				out = new Series("Double", this.numel);
				for (i = 0; i < this.numel; i++)
				{
					if (i == 0)
					{
						out.setDouble(0, 0.0);
					}
					else
					{
						out.setDouble(i, this.getDouble(i) - this.getDouble(i - 1));
					}
				}
			}
			else
			{
				out = new Series("Double", this.numel);
			}
			return out;
		}

		// cumulative sum
		public Series csum()
		{
			Series out;
			int i;
			double prv = 0.0;
			if (this.type == "Double")
			{
				out = new Series("Double", this.numel);
				for (i = 0; i < this.numel; i++)
				{
					prv += this.getDouble(i);
					out.setDouble(i, prv);
				}
			}
			else
			{
				out = new Series("Double", this.numel);
			}
			return out;
		}

		// cumulative multiplication
		public Series cmlt()
		{
			Series out;
			int i;
			double prv = 1.0;
			if (this.type == "Double")
			{
				out = new Series("Double", this.numel);
				for (i = 0; i < this.numel; i++)
				{
					prv *= this.getDouble(i);
					out.setDouble(i, prv);
				}
			}
			else
			{
				out = new Series("Double", this.numel);
			}
			return out;
		}

		public Series mathOpSeries(Series other, String operator)
		{
			Series out;
			if ((this.type == "Double") && (other.type == "Double") && (this.numel == other.numel))
			{
				out = new Series(this.type, this.numel);
				int i;
				double prv;
				switch (operator)
				{
					case "+":
						for (i = 0; i < this.numel; i++)
						{
							prv = this.getDouble(i) + other.getDouble(i);
							out.setDouble(i, prv);
						}
						break;
					case "-":
						for (i = 0; i < this.numel; i++)
						{
							prv = this.getDouble(i) - other.getDouble(i);
							out.setDouble(i, prv);
						}
						break;
					case "*":
						for (i = 0; i < this.numel; i++)
						{
							prv = this.getDouble(i) * other.getDouble(i);
							out.setDouble(i, prv);
						}
						break;
					case "/":
						for (i = 0; i < this.numel; i++)
						{
							prv = this.getDouble(i) / other.getDouble(i);
							out.setDouble(i, prv);
						}
						break;
					case "^":
						for (i = 0; i < this.numel; i++)
						{
							prv = Math.pow(this.getDouble(i),other.getDouble(i));
							out.setDouble(i, prv);
						}
						break;
					case "\\":
						for (i = 0; i < this.numel; i++)
						{
							prv = other.getDouble(i) / this.getDouble(i);
							out.setDouble(i, prv);
						}
						break;
					default:
						for (i = 0; i < this.numel; i++)
						{
							prv = this.getDouble(i);
							out.setDouble(i, prv);
						}
						break;
				}
			}
			else
			{
				out = new Series(this.type, this.numel);
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

		//return minimum if double
		public double minDouble()
		{
			double out = 0.0;
			if (this.type == "Double")
			{
				int i;
				for (i = 0; i < this.al_double.size(); i++)
				{
					if (i == 0)
					{
						out = this.al_double.get(i);
					}
					else
					{
						if (this.al_double.get(i) < out)
						{
							out = this.al_double.get(i);
						}
					}
				}
				return out;
			}
			else
			{
				return (double)(0.0);
			}
		}

		//return maximum if double
		public double maxDouble()
		{
			double out = 0.0;
			if (this.type == "Double")
			{
				int i;
				for (i = 0; i < this.al_double.size(); i++)
				{
					if (i == 0)
					{
						out = this.al_double.get(i);
					}
					else
					{
						if (this.al_double.get(i) > out)
						{
							out = this.al_double.get(i);
						}
					}
				}
				return out;
			}
			else
			{
				return (double)(0.0);
			}
		}

		// replaces NaN with other value...
		public void replaceNanDouble(double v2r)
		{
			int i;
			if (this.type == "Double")
				{
				for (i = 0; i < this.numel; i++)
				{
					if (Double.isNaN(this.al_double.get(i)))
					{
						this.al_double.set(i, v2r);
					}
				}
			}
			return;
		}

	}


}