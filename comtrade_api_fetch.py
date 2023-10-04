import comtradeapicall
import time
import pandas as pd
import os
import json
import plotly.express as px
import plotly.graph_objects as go



os.chdir('/Users/fedelopez/Library/CloudStorage/OneDrive-Personal/Documents/UDESA/06/ECON_INTER/INCAKOLA/COMTRADE')
partners_list = {}
with open('partners.json', 'r') as file:
    partners_temp = json.load(file)
    for i in partners_temp["results"]:
        partners_list[i["text"].lower()] = i["id"]

reporters_list = {}
with open('reporters.json', 'r') as file:
    reporters_temp = json.load(file)
    for i in reporters_temp["results"]:
        reporters_list[i["text"].lower()] = i["id"]

hs = pd.read_csv("harmonized-system.csv")
hs_dict = hs.set_index('hscode').to_dict()['description']


def save_comtrade(
        reporters:list,
        partners:list,
        años:list,
        bienes:list,
        flujo="X",
        mensual = False,
        carpeta = "",
        wait=5
        ):
    '''
        reporters,       : str o lista de str (nombres de paises) / int o lista de int (codigos ONU)
        partners,        : str o lista de str (nombres de paises) / int o lista de int (codigos ONU)
        años,           : lista de años de los que se quieren datos
        bienes,         : int o lista de int
        flujo= "X"      : puede ser X (Exportaciones) o M (importaciones)
        mensual = False : si se quiere en frecuencia mensual, por default trae anual
        carpeta = ""    : nombre de la carpeta donde se guardan los datos
                        Por default va a ser "reporter_flujo_partner_bien_año[0]_año[-1]"
        wait = 5        : tiempo de espera entre limites de velocidad    
    '''

    # FORMAT - Crea la carpeta
    if carpeta != "":
        pass
    else:
        carpeta = f"{reporters}_{flujo}_{partners}_{años[0]}_{años[-1]}(1)"
    
    for ver in range(2, 10):
        if not os.path.exists(carpeta):
            os.makedirs(carpeta)
            break
        else:
            carpeta = carpeta + f"({ver})"

    # FORMAT - Si se ingresa un solo año
    if isinstance(años, (int, float)):
        años = [años]

    # FORMAT - Si se ingresa un solo pais reporter
    if isinstance(reporters, (str)):
        reporters = [reporters.lower()]
    else:
        reporters = [i.lower() for i in reporters]

    # FORMAT - Si se ingresa un solo pais partner
    if isinstance(partners, (str)):
        partners = [partners.lower()]
    else:
        partners = [i.lower() for i in partners]

    # FORMAT - Si se ingresa un solo producto
    if isinstance(bienes, (str, int, float)):
        bienes = [bienes]


    # MENSUALES
    if mensual:
        for reporter in reporters:
            for partner in partners:
                    for bien in bienes:
                        for y in años:
                            for m in range(1,13):
                                while True:
                                    data = comtradeapicall.previewFinalData( 
                                        typeCode='C', freqCode='M', clCode='HS', 
                                        period=f"{y}{m:02}",
                                        reporterCode=reporters_list[reporter], 
                                        partnerCode=partners_list[partner],
                                        cmdCode=f"{bien}", 
                                        flowCode=flujo, maxRecords=2500, 
                                        format_output='JSON', 
                                        aggregateBy=None, breakdownMode='plus', 
                                        countOnly=None, 
                                        includeDesc=True, partner2Code=None, 
                                        customsCode=None, motCode=None
                                    )
                                    if data is None:
                                        time.sleep(wait)
                                    else:
                                        df_temp = pd.DataFrame(data)
                                        df_temp.to_csv(os.path.join(carpeta, 
                                                f"{reporter}_{flujo}_{partner}_{bien}_{y}{m:02}.csv"), 
                                                index=False) 
                                        break

    # MISMO PERO NO MENSUALES
    else:
        for reporter in reporters:
            for partner in partners:
                    for bien in bienes:
                        for y in años:
                            for m in range(1,13):
                                while True:
                                    data = comtradeapicall.previewFinalData( 
                                        typeCode='C', freqCode='A', clCode='HS', 
                                        period=f"{y}",
                                        reporterCode=reporters_list[reporter], 
                                        partnerCode=partners_list[partner],
                                        cmdCode=f"{bien}", 
                                        flowCode=flujo, maxRecords=2500, 
                                        format_output='JSON', 
                                        aggregateBy=None, breakdownMode='plus', 
                                        countOnly=None, 
                                        includeDesc=True, partner2Code=None, 
                                        customsCode=None, motCode=None
                                    )
                                    if data is None:
                                        time.sleep(wait)
                                    else:
                                        df_temp = pd.DataFrame(data)
                                        df_temp.to_csv(os.path.join(carpeta, 
                                                f"{reporter}_{flujo}_{partner}_{bien}_{y}.csv"), 
                                                index=False) 
                                        break
    
 
    
    # Combina todo en un csv
    collapsed = [f for f in os.listdir(carpeta) if f.endswith('.csv')]
    dfs = []

    for data in collapsed:
        filepath = os.path.join(carpeta, data)
        try:
            df = pd.read_csv(filepath)
            if df.empty:
                continue
            dfs.append(df)
        except pd.errors.EmptyDataError:
            continue
    
    combined_df = pd.concat(dfs, ignore_index=True)
    output = os.path.join(carpeta, f"{carpeta}-FINAL.csv")
    combined_df.to_csv(output, index=False)

def plot(data_path, column_y):
    hs['two_digit'] = hs['hscode'].astype(str).str[:2]
    hs_dict = hs.drop_duplicates(subset='two_digit', keep='first').set_index('two_digit').to_dict()['description']
    df = pd.read_csv(data_path)
    
    df['main_category'] = df['cmdCode'].astype(str).str[:2]
    df['main_category_description'] = df['main_category'].map(hs_dict)

    pivot_df = df.pivot_table(index='refYear', columns='main_category_description', values=column_y, aggfunc='sum').reset_index()

    long_df = pivot_df.melt(id_vars=['refYear'], 
                            value_vars=pivot_df.columns.difference(['refYear']),
                            var_name='HS Main Category',
                            value_name=column_y)

    fig = px.bar(long_df, x='refYear', y=column_y, color='HS Main Category', 
                 labels={'refYear': 'Year', column_y: 'Value'},
                 title=f"Yearly {column_y} based on Main Category of HS Code")

    fig.update_layout(showlegend=False)

    fig.show()

def plot_sector(data_path, sector, column_y):
    df = pd.read_csv(data_path)
    
    sector_df = df[df['cmdCode'] == sector]

    pivot_df = sector_df.pivot_table(index='refYear', values=column_y, aggfunc='sum').reset_index()

    fig = px.bar(pivot_df, x='refYear', y=column_y, 
                  title=f"Yearly {column_y} for HS Code: {sector}")

    fig.show()
    

if __name__ == "__main__":
    

    # # Exportaciones totales de peru a argentina
    # save_comtrade(
    #     reporters = "peru",
    #     partners = "argentina",
    #     años = list(range(2000,2024)),
    #     bienes = 'AG6',
    #     flujo= "X",
    #     mensual = False,
    #     carpeta = "peru_arg_total_2000-2024"
    #     )

    # # Exportaciones de agua mineral de peru a argentina
    # save_comtrade(
    #         reporters = "peru",
    #         partners = "argentina",
    #         años = list(range(2002,2024)),
    #         bienes = 220210,
    #         flujo= "X",
    #         mensual = False,
    #         carpeta = "peru_arg_220210"
    #         )
    
    # # Exportaciones de agua mineral de peru a argentina mensual
    # save_comtrade(
    #         reporters = "peru",
    #         partners = "argentina",
    #         años = list(range(2002,2024)),
    #         bienes = 220210,
    #         flujo= "X",
    #         mensual = True,
    #         carpeta = "peru_arg_220210"
    #         )

    # Exportaciones totales de agua mineral de peru al mundo
    # save_comtrade(
    #         reporters = "peru",
    #         partners = "world",
    #         años = list(range(2002,2024)),
    #         bienes = 220210,
    #         flujo= "X",
    #         mensual = False,
    #         carpeta = ""
    #         )

    #plot("peru_arg_total_2000-2024/peru_arg_total_2000-2024-FINAL.csv", "primaryValue")
    plot_sector("peru_arg_total_2000-2024/peru_arg_total_2000-2024-FINAL.csv", 220210, "primaryValue")