echo Compiling javaDataFrame related classes into its package
javac -cp .. javaDataFrame/*.java --release 8
jar cf jdf_v_x_x_x.jar javaDataFrame/*.class