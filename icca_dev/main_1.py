"""Modulo para ejecucion de modulo de ICCA"""

import icca_analysis as icc

FILE_PATH = "./data/icca_file_2.csv"

df = icc.build_dataframe.get_dataframe(FILE_PATH)