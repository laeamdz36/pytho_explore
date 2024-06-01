"""Module for load data and return dataframe"""
from . import decorators
from . import icca_logger
from pathlib import Path
import pandas as pd
import os

def build_path(path : str) -> Path:
    """ Build path object with the path argrument
        return Path object
    """
    file_path = Path(path)
    if file_path.exists():
        return file_path
    print(f"Path -> {path} does exists")
    return None

@decorators.print_header("Construccion de dataframe")
def get_dataframe(file_path : str) -> pd.DataFrame:
    """Build dataframe"""
    logger = icca_logger.build_main_logger()
    path = build_path(file_path)
    logger.info("Ejecucion de codigo iniciada")
    if path != None:
        df = pd.read_csv(Path(path))
        # need to create a logger function to log and notify process
        # print(space,f"----> Dataframe creado exitosamente path {file_path}",space)
        os.system("echo Construccion de dataframe exitoso...")
        # -----------------------Dataframe informations
        df_info(df)
        df_head(df)
        # -----------------------Dataframe analisis
        total_count(df)
        unique_registers(df)
        # -----------------------Dataframe set datetime index
        df_count = count_by_date(df)
        print(df_count) # print dataframe dairy total count by day
        # Limpie consola
        # os.system("cls")

    return df

# ---------------------------------------------------------------------- Data preparation
def set_index_datetime(df):
    """Set index as datetime passin a column to be setted
        Column to datetime es 'production_count_timestamp'
    """

    df["production_count_timestamp"] = pd.to_datetime(df["production_count_timestamp"])
    df = df.set_index(["production_count_timestamp"])
    df = df.iloc[:,1:]

    return df

# ---------------------------------------------------------------------- Start for processing

@decorators.print_header("Impresion de info de dataframe")
def df_info(df):
    """Return dataframe info, styandar methode from pandas"""
    print(df.info())

@decorators.print_header("Header de dataframe")
def df_head(df):
    """Print header of dataframe with pandas methode standar"""
    print(df.head())

# ---------------------------------------------------------------------- Data analisis

# @decorators.print_header("Enumeracion de lineas")
def get_lines(df) -> list:
    """Get unique lines for data
        Return array of lines
    """
    lines = df["special_comments"].unique()

    return lines

@decorators.print_header("Total conteo por linea")
def total_count(df):
    """Get total cpunt by line"""
    # Group by lines and get totla count
    df_lines_group = df.groupby(["special_comments"])["serial_number"].count()
    df_lines_group.index.name = "Lineas"
    df_lines_group.name = "Total"
    print(df_lines_group)

@decorators.print_header("Registros unicos por linea")
def unique_registers(df):
    """registros unicos por linea"""

    # get lines
    lines = get_lines(df)
    # iterate lines and calculate len of registers uniques by line
    for line in lines:
        registers = len(df[df['special_comments'] == line]['serial_number'].unique())
        print(f"{line} - {registers}")

@decorators.print_header("Total por fecha")
def count_by_date(df):
    """Build table for count total by date"""
    # set index date as datetime
    df = set_index_datetime(df)
    # divide dataframe by lines to perform resample by day
    lines = get_lines(df) # get lines frmo columns special_comments
    # build dataframes, and finally concatenate dataframes
    df_list = []
    for line in lines:
        df_aux = df[df["special_comments"] == line]
        df_aux = df_aux.resample("D").count().loc[:,"special_comments"].rename(f"{line}")
        df_list.append(df_aux)
    df_result = pd.concat(df_list,axis=1)
    dates = df_result.index
    # by dates
    df_dates = []
    for date in dates:
        df_dates_aux = df[df.index.date == date.date()]
        # print(df_dates_aux.head())
        df_dates.append(df_dates_aux)
    print(f"Total lenf of dates_aux {len(df_dates)}")
    
    # Iteration over all lines in master dataframe
    for line in lines:
        # iteration on every unique date in master dataframe
        for df_date in df_dates:
            df_by_day = df_date[df_date["special_comments"] == line]
            df_by_day = df_by_day.resample("5min").count().loc[:,"special_comments"].rename(f"{line}")
            print(df_by_day.head())
        # df_dates.append(df_dates_aux)
    # '''
    df_result.index.name = "Date"

    return df_result
