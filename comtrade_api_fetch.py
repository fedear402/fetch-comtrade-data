import comtradeapicall
import time
import pandas as pd
import os
import json
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

def save_comtrade(
        reporters:list,
        partners:list,
        años:list,
        bienes:list,
        flujo="X",
        mensual = False,
        carpeta = "",
        wait=4
        ):
    '''
        reporter,       : str o lista de str (nombres de paises) / int o lista de int (codigos ONU)
        partner,        : str o lista de str (nombres de paises) / int o lista de int (codigos ONU)
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
        carpeta = f"{reporters}_{flujo}_{partners}_{años[0]}_{años[-1]}"
    if not os.path.exists(carpeta):
        os.makedirs(carpeta)

    # FORMAT - Si se ingresa un solo año
    if isinstance(años, (int, float)):
        años = [años]

    # FORMAT - Si se ingresa un solo pais
    if isinstance(reporters, (str)):
        reporters = [reporters.lower()]
    else:
        reporters = [i.lower() for i in reporters]

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
    

    collapsed = [f for f in os.listdir(carpeta) if f.endswith('.csv')]
    dfs = []
    for data in collapsed:
        filepath = os.path.join(carpeta, data)
        df = pd.read_csv(filepath)
        dfs.append(df)
    
    combined_df = pd.concat(dfs, ignore_index=True)
    output = os.path.join(carpeta, f"{carpeta}-FINAL.csv")
    combined_df.to_csv(output, index=False)


if __name__ == "__main__":

    # Exportaciones totales de peru a argentina
    save_comtrade(
        reporters = "peru",
        partners = "argentina",
        años = list(range(2000,2024)),
        bienes = 'AG6',
        flujo= "X",
        mensual = False,
        carpeta = "peru_arg_total_2000-2024"
        )

    # Exportaciones de agua mineral de peru a argentina
    save_comtrade(
            reporters = "peru",
            partners = "argentina",
            años = list(range(2002,2024)),
            bienes = 220210,
            flujo= "X",
            mensual = False,
            carpeta = "peru_arg_220210"
            )
    
    save_comtrade(
            reporters = "peru",
            partners = "argentina",
            años = list(range(2002,2024)),
            bienes = 220210,
            flujo= "X",
            mensual = True,
            carpeta = "peru_arg_220210"
            )

    # Exportaciones totales de agua mineral de peru al mundo
    save_comtrade(
            reporters = "peru",
            partners = "world",
            años = list(range(2002,2024)),
            bienes = 220210,
            flujo= "X",
            mensual = False,
            carpeta = ""
            )


'''
import comtradeapicall
data_peru_argentina_total = comtradeapicall.previewFinalData( 
    typeCode='C', freqCode='A', clCode='HS', period="2001",
    reporterCode=604, partnerCode=32, cmdCode='ALL', 
    flowCode='X', maxRecords=2500, format_output='JSON', 
    aggregateBy=None, breakdownMode='classic', countOnly=None, 
    includeDesc=True, partner2Code=None, customsCode=None, motCode=None
)
'''
