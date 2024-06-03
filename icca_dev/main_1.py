"""Modulo para ejecucion de modulo de ICCA"""

import icca_analysis as icc

FILE_PATH = "./data/icca_file_2.csv"
FILE_PATH_NEW = "./data/new_icca.csv"

df = icc.build_dataframe.get_dataframe(FILE_PATH)
df_test = icc.build_dataframe.read_test_df(FILE_PATH_NEW)
