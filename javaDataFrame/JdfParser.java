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


// ========== FILTERING OPERATION ==========\n

// Parser per esecuzione di comandi.
public class JdfParser{

	public String cmd = "";
	public Jdf jdf; // Jdf to be manipulated
	public String out_str = ""; //output string
	public boolean isparsed = false;
	public int err_level = 0;
	public int err_code = 0;
	public boolean has_keyword = false;
	public String keyword = "";
	public String keyarg = "";
	public int cmdlen = 0;
	public double double_output = 0.0; // for stat calculations
	// List of possible key-words.
	public Jdf.Series cmd_dict = new Jdf.Series("String");

	// Constructor
	public JdfParser(Jdf njdf, String ncmd)
	{
		this.jdf = njdf;
		this.cmd = ncmd;
		this.out_str = this.out_str.concat("COMMAND: ").concat(this.cmd).concat("\n");
		this.initKeyDict();
	}

	public void parse()
	{
		this.getKeyword();
		if (!this.has_keyword)
		{
			this.out_str = this.out_str.concat("ERROR: Unrecognized keyword.\n");
			this.err_level = 1;
			this.err_code = 1; // unrecognized keyword.
			return;
		}
		String[] key_split;
		String col_name;
		String dst_col_name;
		String src_col_name;
		String math_expr;
		String[] math_expr_split;
		boolean valid_col_name;
		switch (keyword)
		{
			// f: FILTERING
			case "f":
				this.out_str = this.out_str.concat("========== FILTERING OPERATION ==========\n");
				key_split = this.keyarg.split("=|>|<|!");
				if (key_split.length < 2)
				{
					this.out_str = this.out_str.concat("ERROR: wrong number of arguments.\n");
					this.err_level = 1;
					this.err_code = 2; // wrong number of args.
					return;
				} 
				else
				{
					// TODO: Implement different kinds of filtering. For now, everything works as "="
					// TODO: Implement filtering for Double and Integer.
					col_name = key_split[0];
					String col_val = key_split[1];
					this.out_str = this.out_str.concat("Filtering where column ").concat(col_name).concat(" is equal to ").concat(col_val).concat(".\n");
					valid_col_name = this.jdf.hasColByNameAndType(col_name, "String");
					if (!valid_col_name)
					{
						this.out_str = this.out_str.concat("ERROR: Invalid column name.\n");
						this.err_level = 1;
						this.err_code = 3; // invalid column name
						return;
					}
					else
					{
						Jdf jdf_p = this.jdf.cutOnValueString(col_name, col_val);
						if (jdf_p.rows == 0)
						{
							this.out_str = this.out_str.concat("ERROR: Output table is empty. Reverting.\n");
							this.err_level = 1;
							this.err_code = 4; // empty output table.
							return;
						}
						else
						{
							this.jdf = jdf_p;
							this.out_str = this.out_str.concat("DONE.\n");
							this.err_level = 0;
							this.err_code = 0; //no error.
							return;
						}
					}
				}
			// fr: filter by row (initial, final.)
			case "fr":
				this.out_str = this.out_str.concat("========== RAW FILTERING OPERATION ==========\n");
				key_split = this.keyarg.split(";");
				if (key_split.length < 2)
				{
					this.out_str = this.out_str.concat("ERROR: wrong number of arguments.\n");
					this.err_level = 1;
					this.err_code = 2; // wrong number of args.
					return;
				} 
				else
				{
					// TODO: Error checking for Integer parsing
					int i_int, n_int;
					String r0s = key_split[0];
					String r1s = key_split[1];
					int r0i = Integer.parseInt(r0s);
					int r1i = Integer.parseInt(r1s);
					this.out_str = this.out_str.concat("Cutting tables for rows >=").concat(new Integer(r0i).toString());
					this.out_str = this.out_str.concat(" and <").concat(new Integer(r1i).toString()).concat(".\n");
					if (r1i <= r0i)
					{
						this.out_str = this.out_str.concat("ERROR: Empty interval selected.\n");
						this.err_level = 1;
						this.err_code = 12; // Empty interval
						return;
					}
					if (r1i >= this.jdf.rows)
					{
						this.out_str = this.out_str.concat("ERROR: End index is out of bounds.\n");
						this.err_level = 1;
						this.err_code = 13; // End index out of bounds.
						return;
					}
					n_int = r1i - r0i;
					int[] arr_ind = new int[n_int];
					for (i_int = 0; i_int < n_int; i_int++)
					{
						arr_ind[i_int] = r0i + i_int;
					}
					this.jdf.selfCutOnIndex(arr_ind);
					this.out_str = this.out_str.concat("DONE.\n");
					this.err_level = 0;
					this.err_code = 0;
					return;
				}
			// rmc: remove column
			case "rmc":
				this.out_str = this.out_str.concat("========== REMOVE COLUMN ==========\n");
				col_name = this.keyarg;
				this.out_str = this.out_str.concat("Removing column ").concat(col_name).concat(" from current table.\n");
				if (!this.jdf.hasColByName(col_name))
				{
					this.out_str = this.out_str.concat("ERROR: Invalid column name.\n");
					this.err_level = 1;
					this.err_code = 3; // invalid column name
					return;
				}
				this.jdf.removeColumn(col_name);
				this.out_str = this.out_str.concat("DONE.\n");
				this.err_level = 0;
				this.err_code = 0;
				return;
			// fillna: replace values with specified amount
			case "fillna":
				this.out_str = this.out_str.concat("========== REPLACE NAN VALUES ==========\n");
				String[] ka_split = this.keyarg.split(";");
				if (ka_split.length != 2)
				{
					this.out_str = this.out_str.concat("ERROR: wrong number of arguments.\n");
					this.err_level = 1;
					this.err_code = 2; // wrong number of args.
					return;
				}
				else
				{
					col_name = ka_split[0];
					String val2repl = ka_split[1];
					this.out_str = this.out_str.concat("Replacing NaNs in column ").concat(col_name).concat(" with value ").concat(val2repl);
					if (!this.jdf.hasColByNameAndType(col_name, "Double"))
					{
						this.out_str = this.out_str.concat("ERROR: Invalid column name.\n");
						this.err_level = 1;
						this.err_code = 3; // invalid column name
						return;
					}
					else
					{
						double v2r;
						try
						{
							v2r = Double.parseDouble(val2repl);
						} catch (NumberFormatException e)
						{
							v2r = 0.0;
						}
						this.jdf.replaceNaN(col_name, v2r);
						this.out_str = this.out_str.concat("DONE.\n");
						this.err_level = 0;
						this.err_code = 0;
						return;
					}
				}
			case "ms":
				this.out_str = this.out_str.concat("========== MATH OP. WITH SCALAR ==========\n");
				key_split = this.keyarg.split("=");
				if (key_split.length < 2) //ok
				{
					this.out_str = this.out_str.concat("ERROR: wrong number of arguments.\n");
					this.err_level = 1;
					this.err_code = 2; // wrong number of args.
					return;
				} 
				else
				{
					col_name = key_split[0];
					math_expr = key_split[1];
					math_expr_split = math_expr.split("\\+|/|\\*|\\^|\\-");
					valid_col_name = !this.jdf.hasColByName(col_name); // Check if column already there.
					if (!valid_col_name) //ok.
					{
						this.out_str = this.out_str.concat("ERROR: Invalid target column name (Already taken!).\n");
						this.err_level = 1;
						this.err_code = 6; // invalid column name - already taken.
						return;
					}
					else
					{
						if (math_expr_split.length != 2) // ok
						{
							this.out_str = this.out_str.concat("ERROR: Wrong number of operands in expression.\n");
							this.err_level = 1;
							this.err_code = 5; // wrong number of operands.
							return;
						}
						else
						{
							// TODO: Simplify this and recycle code for "mc" command!
							src_col_name = math_expr_split[0];
							String sec_operand = math_expr_split[1];
							valid_col_name = this.jdf.hasColByNameAndType(src_col_name, "Double");
							if (!valid_col_name) // ok
							{
								this.out_str = this.out_str.concat("ERROR: Invalid column name.\n");
								this.err_level = 1;
								this.err_code = 3; // invalid column name
								return;
							}
							else
							{
								String opc = ""; // operation character
								boolean found_opc = false;
								if (math_expr.contains("+"))
								{
									opc = "+";
									found_opc = true;
								}
								if (math_expr.contains("-"))
								{
									opc = "-";
									found_opc = true;
								}
								if (math_expr.contains("/"))
								{
									opc = "/";
									found_opc = true;
								}
								if (math_expr.contains("*"))
								{
									opc = "*";
									found_opc = true;
								}
								if (math_expr.contains("^"))
								{
									opc = "^";
									found_opc = true;
								}
								if (!found_opc) // will never get there as it won't be recognized as a regex separator in first place...
								{
									this.out_str = this.out_str.concat("ERROR: Unrecognized/unsupported operator.\n");
									this.err_level = 1;
									this.err_code = 7; // Unsupported operator.
									return;
								}
								else
								{
									// TODO: Check if conversion has been performed successfully.
									double sop = Double.parseDouble(sec_operand);
									this.out_str = this.out_str.concat("Defining new column ").concat(col_name).concat(" as ").concat(src_col_name);
									this.out_str = this.out_str.concat(" ").concat(opc).concat(" ").concat(sec_operand).concat(".\n");
									this.out_str = this.out_str.concat("DONE.\n");
									this.jdf.mathOpScalar(col_name, src_col_name, sop, opc);
									this.err_level = 0;
									this.err_code = 0;
									return;
								}
							}
						}
					}
				}
			// MC: mathematical operations between columns.
			case "mc":
				this.out_str = this.out_str.concat("========== MATH OP. (2 COLUMNS) ==========\n");
				key_split = this.keyarg.split("=");
				if (key_split.length < 2)
				{
					this.out_str = this.out_str.concat("ERROR: wrong number of arguments.\n");
					this.err_level = 1;
					this.err_code = 2; // wrong number of args.
					return;
				} 
				else
				{
					col_name = key_split[0];
					math_expr = key_split[1];
					math_expr_split = math_expr.split("\\+|/|\\*|\\^|\\-");
					valid_col_name = !this.jdf.hasColByName(col_name); // Check if column already there.
					if (!valid_col_name)
					{
						this.out_str = this.out_str.concat("ERROR: Invalid target column name (Already taken!).\n");
						this.err_level = 1;
						this.err_code = 6; // invalid column name - already taken.
						return;
					}
					else
					{
						if (math_expr_split.length != 2)
						{
							this.out_str = this.out_str.concat("ERROR: Wrong number of operands in expression.\n");
							this.err_level = 1;
							this.err_code = 5; // wrong number of operands.
							return;
						}
						else
						{
							String src_col1_name = math_expr_split[0];
							String src_col2_name = math_expr_split[1];
							valid_col_name = ((this.jdf.hasColByNameAndType(src_col1_name, "Double")) && (this.jdf.hasColByNameAndType(src_col2_name, "Double")));
							if (!valid_col_name)
							{
								this.out_str = this.out_str.concat("ERROR: At least one of the specified columns does not exist.\n");
								this.err_level = 1;
								this.err_code = 3; // invalid column name
								return;
							}
							else
							{
								String opc = ""; // operation character
								boolean found_opc = false;
								if (math_expr.contains("+"))
								{
									opc = "+";
									found_opc = true;
								}
								if (math_expr.contains("-"))
								{
									opc = "-";
									found_opc = true;
								}
								if (math_expr.contains("/"))
								{
									opc = "/";
									found_opc = true;
								}
								if (math_expr.contains("*"))
								{
									opc = "*";
									found_opc = true;
								}
								if (math_expr.contains("^"))
								{
									opc = "^";
									found_opc = true;
								}
								if (!found_opc)
								{
									this.out_str = this.out_str.concat("ERROR: Unrecognized/unsupported operator.\n");
									this.err_level = 1;
									this.err_code = 7; // Unsupported operator.
									return;
								}
								else
								{
									this.out_str = this.out_str.concat("Defining new column ").concat(col_name).concat(" as ").concat(src_col1_name);
									this.out_str = this.out_str.concat(" ").concat(opc).concat(" ").concat(src_col2_name).concat(".\n");
									this.out_str = this.out_str.concat("DONE.\n");
									this.jdf.mathOpTwoCol(col_name, src_col1_name, src_col2_name, opc);
									this.err_level = 0;
									this.err_code = 0;
									return;
								}
							}
						}
					}
				}
			// TODO: This, csum and drat should become the same function with different arguments.
			// diff: differentiate on column
			case "diff":
				this.out_str = this.out_str.concat("========== DIFF OPERATION ==========\n");
				key_split = this.keyarg.split(";");
				if (key_split.length < 2)
				{
					this.out_str = this.out_str.concat("ERROR: wrong number of arguments.\n");
					this.err_level = 1;
					this.err_code = 2; // wrong number of args.
					return;
				} 
				else
				{
					dst_col_name = key_split[0];
					src_col_name = key_split[1];
					this.out_str = this.out_str.concat("Defining new column ").concat(dst_col_name).concat(" as diff of column ").concat(src_col_name).concat(".\n");
					//boolean valid_col_name = ((!this.jdf.hasColByName(dst_col_name)) && (this.jdf.hasColByNameAndType(src_col1_name, "Double")));
					boolean valid_dst_col_name = !this.jdf.hasColByName(dst_col_name);
					if (!valid_dst_col_name) 
					{
						this.out_str = this.out_str.concat("ERROR: Invalid target column name (Already taken!).\n");
						this.err_level = 1;
						this.err_code = 6; // invalid column name - already taken.
						return;
					}
					else
					{
						boolean valid_src_col_name = this.jdf.hasColByNameAndType(src_col_name, "Double");
						if (!valid_src_col_name) 
						{
							this.out_str = this.out_str.concat("ERROR: Invalid source column name (non-existent or invalid type).\n");
							this.err_level = 1;
							this.err_code = 3; // invalid column name
							return;
						}
						else
						{
							this.jdf.diffColumn(dst_col_name, src_col_name);
							this.out_str = this.out_str.concat("DONE.\n");
							this.err_level = 0;
							this.err_code = 0;
							return;
						}
					}
				}
			// csum: cumulated sum.
			case "csum":
				this.out_str = this.out_str.concat("========== CUMSUM OPERATION ==========\n");
				key_split = this.keyarg.split(";");
				if (key_split.length < 2)
				{
					this.out_str = this.out_str.concat("ERROR: wrong number of arguments.\n");
					this.err_level = 1;
					this.err_code = 2; // wrong number of args.
					return;
				} 
				else
				{
					dst_col_name = key_split[0];
					src_col_name = key_split[1];
					this.out_str = this.out_str.concat("Defining new column ").concat(dst_col_name).concat(" as cumulated sum of column ").concat(src_col_name).concat(".\n");
					//boolean valid_col_name = ((!this.jdf.hasColByName(dst_col_name)) && (this.jdf.hasColByNameAndType(src_col1_name, "Double")));
					boolean valid_dst_col_name = !this.jdf.hasColByName(dst_col_name);
					if (!valid_dst_col_name) 
					{
						this.out_str = this.out_str.concat("ERROR: Invalid target column name (Already taken!).\n");
						this.err_level = 1;
						this.err_code = 6; // invalid column name - already taken.
						return;
					}
					else
					{
						boolean valid_src_col_name = this.jdf.hasColByNameAndType(src_col_name, "Double");
						if (!valid_src_col_name) 
						{
							this.out_str = this.out_str.concat("ERROR: Invalid source column name (non-existent or invalid type).\n");
							this.err_level = 1;
							this.err_code = 3; // invalid column name
							return;
						}
						else
						{
							this.jdf.csumColumn(dst_col_name, src_col_name);
							this.out_str = this.out_str.concat("DONE.\n");
							this.err_level = 0;
							this.err_code = 0;
							return;
						}
					}
				}
			// STAT: Calcoli statistici
			case "stat":
				this.out_str = this.out_str.concat("========== STATISTIC CALCULATION ==========\n");
				JdfStats js = new JdfStats(); // create statistical calc object.
				key_split = this.keyarg.split("\\-");
				if (key_split.length < 2)
				{
					this.out_str = this.out_str.concat("ERROR: wrong number of arguments.\n");
					this.err_level = 1;
					this.err_code = 2; // wrong number of args.
					return;
				}
				else
				{
					String statfcn = key_split[0];
					String statarg = key_split[1];
					switch (statfcn)
					{
						case "mean":
							col_name = statarg;
							this.out_str = this.out_str.concat("Calculating mean of column ").concat(col_name).concat(".\n");
							valid_col_name = this.jdf.hasColByNameAndType(col_name, "Double");
							if (!valid_col_name)
							{
								this.out_str = this.out_str.concat("ERROR: Invalid source column name (non-existent or invalid type).\n");
								this.err_level = 1;
								this.err_code = 3; // invalid column name
								return;
							}
							else
							{
								this.double_output = js.meanJdfCol(this.jdf, col_name); 
								this.err_level = 0;
								this.err_code = 0;
							}
							break;
						case "std":
							col_name = statarg;
							this.out_str = this.out_str.concat("Calculating standard deviation of column ").concat(col_name).concat(".\n");
							valid_col_name = this.jdf.hasColByNameAndType(col_name, "Double");
							if (!valid_col_name)
							{
								this.out_str = this.out_str.concat("ERROR: Invalid source column name (non-existent or invalid type).\n");
								this.err_level = 1;
								this.err_code = 3; // invalid column name
								return;
							}
							else
							{
								this.double_output = js.stdJdfCol(this.jdf, col_name); 
								this.err_level = 0;
								this.err_code = 0;
							}
							break;
						case "var":
							col_name = statarg;
							this.out_str = this.out_str.concat("Calculating variance of column ").concat(col_name).concat(".\n");
							valid_col_name = this.jdf.hasColByNameAndType(col_name, "Double");
							if (!valid_col_name)
							{
								this.out_str = this.out_str.concat("ERROR: Invalid source column name (non-existent or invalid type).\n");
								this.err_level = 1;
								this.err_code = 3; // invalid column name
								return;
							}
							else
							{
								this.double_output = js.varJdfCol(this.jdf, col_name); 
								this.err_level = 0;
								this.err_code = 0;
							}
							break;
						case "corr":
							String[] statargsplit = statarg.split(";");
							String col1_name = statargsplit[0];
							String col2_name = statargsplit[1];
							this.out_str = this.out_str.concat("Calculating correlation between ").concat(col1_name).concat(" and ").concat(col2_name).concat(".\n");
							valid_col_name = ((this.jdf.hasColByNameAndType(col1_name, "Double")) && (this.jdf.hasColByNameAndType(col2_name, "Double")));
							if (!valid_col_name)
							{
								this.out_str = this.out_str.concat("ERROR: At least one of the specified columns does not exist.\n");
								this.err_level = 1;
								this.err_code = 3; // invalid column name
								return;
							}
							else
							{
								this.double_output = js.corrJdfCols(this.jdf, col1_name, col2_name); 
								this.err_level = 0;
								this.err_code = 0;
							}
							break;
						default:
							this.out_str = this.out_str.concat("ERROR: Invalid/unrecognized statistical function.\n");
							this.err_level = 1;
							this.err_code = 8; // invalid stat function
							return;

					}
					this.out_str = this.out_str.concat("Result = ").concat(new Double(this.double_output).toString()).concat(".\n");
					this.out_str = this.out_str.concat("DONE.\n");
					return;
				}
				// Load from url or from csv
				case "load":
					this.out_str = this.out_str.concat("========== DATA LOAD ==========\n");
					key_split = this.keyarg.split(" ", 2);
					String data_src  = key_split[0];
					String data_name = key_split[1];
					switch (data_src)
					{
						case "url":
							this.out_str = this.out_str.concat("Loading data from URL: ").concat(data_name).concat(".\n");
							try
							{
								this.jdf.fillFromUrl(data_name);
								this.out_str = this.out_str.concat("DONE.\n");
								this.err_level = 0;
								this.err_code = 0;
								return;
							}
							catch (IOException e)
							{
								this.out_str = this.out_str.concat("ERROR: Invalid URL.\n");
								this.err_level = 2; // internal java error.
								this.err_code = 9; // invalid url.
								return;
							}
						case "csv":
							this.out_str = this.out_str.concat("Loading data from CSV: ").concat(data_name).concat(".\n");
							try
							{
								this.jdf.fillFromCsv(data_name);
								this.out_str = this.out_str.concat("DONE.\n");
								this.err_level = 0;
								this.err_code = 0;
								return;
							}
							catch (IOException e)
							{
								this.out_str = this.out_str.concat("ERROR: Invalid file name.\n");
								this.err_level = 2; // internal java error.
								this.err_code = 10; // invalid fname.
								return;
							}
						default:
							this.out_str = this.out_str.concat("ERROR: Invalid data source key-word.\n");
							this.err_level = 1; // jdf error.
							this.err_code = 11; // invalid data source option.
							return;
					}
				// Save to csv (or other format in a far future.)
				case "save":
					this.out_str = this.out_str.concat("========== DATA SAVE ==========\n");
					key_split = this.keyarg.split(" ", 2);
					data_src  = key_split[0];
					data_name = key_split[1];
					switch (data_src)
					{
						case "csv":
							this.out_str = this.out_str.concat("Saving current data to CSV: ").concat(data_name).concat(".\n");
							try
							{
								this.jdf.writeCsv(data_name);
								this.out_str = this.out_str.concat("DONE.\n");
								this.err_level = 0;
								this.err_code = 0;
								return;
							}
							catch (IOException e)
							{
								this.out_str = this.out_str.concat("ERROR: Internal java I/O error.\n");
								this.err_level = 2; // internal java error.
								this.err_code = 10; // invalid fname.
								return;
							}
						default:
							this.out_str = this.out_str.concat("ERROR: Invalid output file format.\n");
							this.err_level = 1; // jdf error.
							this.err_code = 12; // invalid output file format.
							return;
					}
				case "reset":
					this.out_str = this.out_str.concat("========== RESET OPERATION ==========\n");
					this.out_str = this.out_str.concat("Resetting current table to an empty one.\n");
					this.out_str = this.out_str.concat("DONE.\n");
					this.jdf = new Jdf();
					this.err_code = 0;
					this.err_level = 0;
					return;
				// nc: create new column.
				case "nc":
					this.out_str = this.out_str.concat("========== NEW EMPTY COLUMN ==========\n");
					key_split = this.keyarg.split(";");
					if (key_split.length < 2)
					{
						this.out_str = this.out_str.concat("ERROR: wrong number of arguments.\n");
						this.err_level = 1;
						this.err_code = 2; // wrong number of args.
						return;
					} 
					else
					{
						col_name = key_split[0];
						String col_type = key_split[1];
						this.out_str = this.out_str.concat("Creating new column of type ").concat(col_type).concat(", named ").concat(col_name).concat(".\n");
						valid_col_name = !this.jdf.hasColByName(col_name); // Check if column already there.
						if (!valid_col_name)
						{
							this.out_str = this.out_str.concat("ERROR: Invalid target column name (Already taken!).\n");
							this.err_level = 1;
							this.err_code = 6; // invalid column name - already taken.
							return;
						}
						else
						{
							if (col_type.equalsIgnoreCase("double"))
							{
								this.jdf.addEmptyColumn(col_name, "Double");
								this.err_level = 0;
								this.err_code = 0;
								this.out_str = this.out_str.concat("Done.\n");
								return;
							}
							else
							{
								if (col_type.equalsIgnoreCase("integer"))
								{
									this.jdf.addEmptyColumn(col_name, "Integer");
									this.err_level = 0;
									this.err_code = 0;
									this.out_str = this.out_str.concat("Done.\n");
									return;
								}
								else
								{
									if (col_type.equalsIgnoreCase("string"))
									{
										this.jdf.addEmptyColumn(col_name, "String");
										this.err_level = 0;
										this.err_code = 0;
										this.out_str = this.out_str.concat("Done.\n");
										return;
									}
									else
									{
										this.out_str = this.out_str.concat("ERROR: Unknown or unrecognized type.\n");
										this.err_level = 1;
										this.err_code = 20; // unrecognized type
										return;
									}
								}
							}
						}
					}
				// get element
				case "get":
				this.out_str = this.out_str.concat("========== GET ELEMENT ==========\n");
				key_split = this.keyarg.split(";");
				if (key_split.length < 2) //ok
				{
					this.out_str = this.out_str.concat("ERROR: wrong number of arguments.\n");
					this.err_level = 1;
					this.err_code = 2; // wrong number of args.
					return;
				} 
				else
				{
					col_name = key_split[0];
					String str_ind = key_split[1]; // index.
					this.out_str = this.out_str.concat("Retrieving element from column ").concat(col_name);
					this.out_str = this.out_str.concat(" at index ").concat(str_ind).concat(".\n");
					valid_col_name = this.jdf.hasColByName(col_name); // Check if column already there.
					if (!valid_col_name) //ok.
					{
						this.out_str = this.out_str.concat("ERROR: Invalid target column name.\n");
						this.err_level = 1;
						this.err_code = 3; // invalid source column name.
						return;
					}
					else
					{
						int ind_to_get = 0;
						try
						{
							ind_to_get = Integer.parseInt(str_ind);
						} catch (NumberFormatException e)
						{
							this.out_str = this.out_str.concat("ERROR: Invalid index.\n");
							this.err_level = 2;
							this.err_code = 32; // invalid source column name.
							return;
						}
						if ((ind_to_get >= 0) && (ind_to_get < this.jdf.rows))
						{
							String res_get = this.jdf.getColByName(col_name).getParse(ind_to_get);
							this.out_str = this.out_str.concat("Result: ").concat(res_get).concat("\n");
							this.out_str = this.out_str.concat("DONE.\n");
							this.err_level = 0;
							this.err_code = 0;
							return;
						}
						else
						{
							this.out_str = this.out_str.concat("ERROR: Index is out of bounds.\n");
							this.err_level = 1;
							this.err_code = 31; // index out of bounds.
							return;
						}
					}
				}
				// set element
				case "set":
				this.out_str = this.out_str.concat("========== SET ELEMENT ==========\n");
				key_split = this.keyarg.split(";");
				if (key_split.length < 3) //ok
				{
					this.out_str = this.out_str.concat("ERROR: wrong number of arguments.\n");
					this.err_level = 1;
					this.err_code = 2; // wrong number of args.
					return;
				} 
				else
				{
					col_name = key_split[0];
					String str_ind = key_split[1]; // index.
					String str_val = key_split[2]; // value.
					this.out_str = this.out_str.concat("Setting element from column ").concat(col_name);
					this.out_str = this.out_str.concat(" at index ").concat(str_ind);
					this.out_str = this.out_str.concat(" to ").concat(str_val).concat(".\n");
					valid_col_name = this.jdf.hasColByName(col_name); // Check if column already there.
					if (!valid_col_name) //ok.
					{
						this.out_str = this.out_str.concat("ERROR: Invalid target column name.\n");
						this.err_level = 1;
						this.err_code = 3; // invalid source column name.
						return;
					}
					else
					{
						int ind_to_set = 0;
						try
						{
							ind_to_set = Integer.parseInt(str_ind);
						} catch (NumberFormatException e)
						{
							this.out_str = this.out_str.concat("ERROR: Invalid index.\n");
							this.err_level = 2;
							this.err_code = 32; // invalid source column name.
							return;
						}
						if ((ind_to_set >= 0))
						{
							this.jdf.setParseOverride(col_name, ind_to_set, str_val);
							this.out_str = this.out_str.concat("DONE.\n");
							this.err_level = 0;
							this.err_code = 0;
							return;
						}
						else
						{
							this.out_str = this.out_str.concat("ERROR: Index is out of bounds.\n");
							this.err_level = 1;
							this.err_code = 31; // index out of bounds.
							return;
						}
					}
				}
				// print "pretty string" - NO typical output; special case.
				case "print":
					int pslen = 25;
					if (this.cmdlen > 1)
					{
						pslen = Integer.parseInt(this.keyarg); // customizable length
					}
					this.out_str = this.jdf.prettyString(pslen);
					this.err_code = 0;
					this.err_level = 0;
					return;
			default:
				break;
		}
	}

	// separate first key-word. Should become private.
	public void getKeyword()
	{
		String[] kk = this.cmd.split(" ", 2);
		this.cmdlen = kk.length;
		this.keyword = kk[0];
		if (this.cmdlen > 1)
		{
			this.keyarg = kk[1];
		}
		if (this.cmd_dict.hasString(this.keyword))
		{
			this.has_keyword = true; // validated.
		}
		return;
	}

	public void initKeyDict()
	{
		this.cmd_dict.addString("f");          // filter by string value
		this.cmd_dict.addString("fr");         // filter rows
		this.cmd_dict.addString("rmc");	       // remove column by name
		this.cmd_dict.addString("fillna");     // replace NaN values with arbitrary values.
		this.cmd_dict.addString("ms");         // math operation between a column and a scalar
		this.cmd_dict.addString("mc");		   // math operation involving two columns
		this.cmd_dict.addString("diff");	   // diff operation over one column
		this.cmd_dict.addString("csum");	   // cumulated sum of one column
		this.cmd_dict.addString("stat");	   // statistical calculations
		this.cmd_dict.addString("load");
		this.cmd_dict.addString("save");
		this.cmd_dict.addString("reset");      // reset to empty table.
		this.cmd_dict.addString("nc");         // create new (empty) column.
		this.cmd_dict.addString("get");		   // get element from a column at a specific index
		this.cmd_dict.addString("set");		   // set element of a column at a specific index to a given value.
		// special case!
		this.cmd_dict.addString("print");
	}

}