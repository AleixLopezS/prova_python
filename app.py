# --- LLIBRERIES ---
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import BlobServiceClient
from flask import Flask
from io import StringIO
import os
import csv
import pandas as pd
import numpy as np
from datetime import datetime
from solver import *

app = Flask(__name__)

# Este controla la pagina inicial de nuestra Web App
@app.route('/')
def index():
   return "¡La app está activa!"

# Creación de un fichero en blob storage
@app.route('/calcul_enun', methods=['GET'])
def calcul_enun():    

    conn_str = 'DefaultEndpointsProtocol=https;AccountName=stfwehqdcdes;AccountKey=gPydOfNRuyS5spnv80AXBue7fJwbC5DqyPTRzx5Djc8ZxvkVgo11CAvtfv8IE3P1BJQ0TXPiXXac+AStT8N+UQ==;EndpointSuffix=core.windows.net'
    container_entrada = 'entrada'
    container_salida = 'salida'
    # --- ORIGENS DE DADES ---
    # Dades d'exàmens
    
    blob_dades_exams = 'entrada/Dades_examens_PAU.csv'    
    #PATH_EXTRACCIO = 'c:\\Users\\53339164w\\Albert_Jupyter\\Datasets\\REU Exàmens PAU\\Dades\\Dades_examens_PAU.xlsx'
    #SHEET_EXTRACCIO = 'Extraccio 03052022'

    # Dades Seus-Tribunals
    blob_seus_tribunal = 'entrada/seustribunal.csv'
    #PATH_SEUS_TRIBUNALS = PATH_EXTRACCIO
    #SHEET_SEUS_TRIBUNALS = 'SeusTribunals'

    # Caixes
    blob_caixes = 'entrada/caixes.json'
    #PATH_CAIXES = 'c:\\Users\\53339164w\\Albert_Jupyter\\Datasets\\REU Exàmens PAU\\Dades\\caixes.json'

    # Informació Assignatures
    blob_asignatures = 'entrada/Dades_assignatures.csv'
    #PATH_ASSIGNATURES = 'c:\\Users\\53339164w\\Albert_Jupyter\\Datasets\\REU Exàmens PAU\\Dades\\Dades_assignatures.xlsx'


    # --- IMPORTACIÓ DE DADES --- 

    # Previsió
    data_raw_previsio = pd.read_csv(f"abfs://{blob_dades_exams}",
                                    storage_options={"connection_string": conn_str}, 
                                    header=0)
    #data_raw_previsio = pd.read_excel(PATH_EXTRACCIO, sheet_name=SHEET_EXTRACCIO)

    # Mestre Trubinal-Seu
    data_raw_tribunals = pd.read_csv(f"abfs://{blob_seus_tribunal}",
                                    storage_options={"connection_string": conn_str}, 
                                    header=0)
    #data_raw_tribunals = pd.read_excel(PATH_SEUS_TRIBUNALS, sheet_name=SHEET_SEUS_TRIBUNALS)

    # Assignatures
    data_raw_assignatures = pd.read_csv(f"abfs://{blob_asignatures}",
                                    storage_options={"connection_string": conn_str}, 
                                    header=0)
    #data_raw_assignatures = pd.read_excel(PATH_ASSIGNATURES)

    # Caixes
    data_raw_caixes = pd.read_json(f"abfs://{blob_caixes}",
                                    storage_options={"connection_string": conn_str}
                                    )
    #with open(PATH_CAIXES, encoding="utf8") as json_file:
    #    data_raw_caixes = json.load(json_file)


    # --- PRE-PROCESSAT DE DADES ---

    # Canvi de tipus de dades
    data_raw_previsio['TRIBUNAL'] = data_raw_previsio['TRIBUNAL'].astype(str)
    data_raw_tribunals['TRIBUNAL'] = data_raw_tribunals['TRIBUNAL'].astype(str)

    # Any de la convocatòria
    ANY_CONVOC = data_raw_previsio['ANY_CONVOC'].iloc[0]

    # Diccionaris de relacions TRIBUNAL - SEU - UNI
    dic_trib_seu = dict(zip(data_raw_tribunals['TRIBUNAL'], data_raw_tribunals['SEU']))
    dic_trib_centre = dict(zip(data_raw_tribunals['TRIBUNAL'], data_raw_tribunals['CENTRE-EXAMEN']))
    dic_seu_uni = dict(zip(data_raw_tribunals['SEU'], data_raw_tribunals['SIGLA_UNIV']))

    # Join de dades de previsió i mestre tribunal-seu
    data_raw = pd.merge(data_raw_previsio, data_raw_tribunals,  how='left', left_on=['TRIBUNAL'], right_on = ['TRIBUNAL'])

    # Selecció de variables
    data = data_raw[['TRIBUNAL','NOM_MATERIA','PREVISIO_PREMATRICULA']].copy()

    # Matriu de previsió per Matèria i Tribunal
    data_matrix = data.groupby(by=['NOM_MATERIA','TRIBUNAL']).sum().unstack().fillna(0).astype(int)

    # Llistat de Tribunals
    tribunals = list(data_matrix.columns.levels[1])

    # Dades d'assignatures
    data_assignatures = data_raw_assignatures.copy()

    def A4_to_A3(A4):
        if A4 % 4 == 0:
            return A4 // 4
        else:
            return (A4 // 4) + 1

    data_assignatures['Fulls A3'] = data_assignatures['Fulls A4'].apply(A4_to_A3)

    # Diccionaris d'assignatures
    dic_codi_assig = dict(zip(data_assignatures['Codi'], data_assignatures['Assignatura']))
    dic_assig_codi = dict(zip(data_assignatures['Assignatura'], data_assignatures['Codi']))
    dic_assig_fulls_A3 = dict(zip(data_assignatures['Assignatura'], data_assignatures['Fulls A3']))
    dic_assig_idioma = dict(zip(data_assignatures['Assignatura'], data_assignatures['Idioma']))
    dic_assig_marge = dict(zip(data_assignatures['Assignatura'], data_assignatures['Nombre d\'exàmens de marge de seguretat per tribunal']))
    dic_assig_perc_castella = dict(zip(data_assignatures['Assignatura'], data_assignatures['Percentatge d\'examens addicionals en castellà']))

    # Dades de les caixes
    dic_caixes = {k:v["Assignatures"] for k,v in data_raw_caixes.items()}

    for caixa, codis in dic_caixes.items():
        assignatures = []
        for codi in codis:
            assignatures.append(dic_codi_assig[codi])
        dic_caixes[caixa] = assignatures

    dic_caixes_dia = {k:v["Dia"] for k,v in data_raw_caixes.items()}



    # --- OPTIMITZACIÓ ---

    caixes = {}   # Dict of dicts. Caixa és un dict i Caixes és un dict of dicts.

    for nom_caixa in dic_caixes:
        
        previsio_original = data_matrix[data_matrix.index.isin(dic_caixes[nom_caixa])]
        
        marge = previsio_original*0

        for assig in dic_caixes[nom_caixa]:
            marge.loc[assig] = dic_assig_marge[assig]

        previsio_marge = previsio_original + marge
        
        caixes[nom_caixa] = {
            'Previsió original': previsio_original, 
            'Marge': marge,
            'Previsió amb marge': previsio_marge,
            'Assignatures': dic_caixes[nom_caixa],
            'Tribunals': tribunals,
            'Fulls per assignatura': {assig: dic_assig_fulls_A3.get(assig) for assig in dic_caixes[nom_caixa]},
            'Idiomes': {assig: dic_assig_idioma.get(assig) for assig in dic_caixes[nom_caixa]}}

    for i, nom_caixa in enumerate(caixes):

        # Optimitzem la caixa
        n,x = solver.optimizar_caixa(caixes[nom_caixa])

        # Guardem els resultats al diccionari de la caixa
        caixes[nom_caixa]['n']=n    # Nombre d'examens en català de cada assignatura en la caixa
        caixes[nom_caixa]['x']=x    # Nombre de caixes repartides a cada tribunal


    # --- VALIDACIÓ DE RESULTATS ---

    # imprimir -> Matriu de nombre d'examens en català resultants del càlcul per assignatura i tribunal
    # sobrants -> Matriu de nombre d'examens en català sobrants per assigntaura i tribunal

    for nom_caixa in caixes:

        imprimir = np.zeros(caixes[nom_caixa]['Previsió original'].shape)

        for i in range(imprimir.shape[0]):
            for j in range(imprimir.shape[1]):
                imprimir[i,j] = caixes[nom_caixa]['n'][i]*caixes[nom_caixa]['x'][j]

        caixes[nom_caixa]['Imprimir'] = pd.DataFrame(imprimir, index=caixes[nom_caixa]['Previsió original'].index, columns=caixes[nom_caixa]['Previsió original'].columns).astype(int)
        caixes[nom_caixa]['Sobrants'] = caixes[nom_caixa]['Imprimir'] - caixes[nom_caixa]['Previsió original']

    # Validació de la cobertura de la previsió
    for nom_caixa in caixes:
        if (caixes[nom_caixa]['Sobrants']<0).values.any():
            previsio_coberta = False
            break
        else:
            previsio_coberta = True

    resultat_validacio = 'Tota la previsió ha estat coberta' if previsio_coberta else 'ERROR! Hi ha previsions no cobertes'


    # --- EXÀMENS EN CASTELLÀ ---

    for nom_caixa in caixes:
        caixes[nom_caixa]['n_cat'] = caixes[nom_caixa]['n']*0       # Nombre d'examens en català de cada assignatura en la caixa
        caixes[nom_caixa]['n_cast'] = caixes[nom_caixa]['n']*0      # Nombre d'examens en castellà de cada assignatura en la caixa
        
        for i, assig in enumerate(caixes[nom_caixa]['Assignatures']):
            idioma = dic_assig_idioma[assig]

            if idioma == 'Català':
                caixes[nom_caixa]['n_cat'][i] = caixes[nom_caixa]['n'][i]
            elif idioma == 'Castellà':
                caixes[nom_caixa]['n_cast'][i] = caixes[nom_caixa]['n'][i]
            elif idioma == 'Mixt':
                caixes[nom_caixa]['n_cat'][i] = caixes[nom_caixa]['n'][i]
                caixes[nom_caixa]['n_cast'][i] = np.ceil(caixes[nom_caixa]['n'][i]*dic_assig_perc_castella[assig]/100)


    # --- CAIXES DE RESERVA ---

    N_caixes_reserva = 2

    for nom_caixa in caixes:
        caixes[nom_caixa]['Tribunal reserves'] = '000000'
        caixes[nom_caixa]['N Caixes reserva'] = N_caixes_reserva


    # --- CÀLCUL DE SUB-TOTALS ---
    for nom_caixa in caixes:
       k = np.array(list(caixes[nom_caixa]['Fulls per assignatura'].values()))
       n_cat = caixes[nom_caixa]['n_cat']
       n_cast = caixes[nom_caixa]['n_cast']
       x = caixes[nom_caixa]['x']
       r = caixes[nom_caixa]['N Caixes reserva']

       caixes[nom_caixa]['Fulls']=np.inner(n_cat+n_cast,k)*(sum(x)+r)    # Fulls A3 de totes les caixes d'aquest tipus
       caixes[nom_caixa]['Fulls per caixa']=np.inner(n_cat+n_cast,k)    # Fulls A3 que conté la caixa
       caixes[nom_caixa]['Caixes a encarregar']=sum(x)+r    # Nombre de caixes d'aquest tipus en concret a encarregar
       caixes[nom_caixa]['Examens']= (sum(n_cat)+sum(n_cast))*(sum(x)+r)
       caixes[nom_caixa]['Examens Català']= sum(n_cat)*(sum(x)+r)
       caixes[nom_caixa]['Examens Castellà']= sum(n_cast)*(sum(x)+r)
       caixes[nom_caixa]['Examens per caixa']= sum(n_cat)+sum(n_cast)
       caixes[nom_caixa]['Examens Català per caixa']= sum(n_cat)
       caixes[nom_caixa]['Examens Castellà per caixa']= sum(n_cat)

    # --- CÀLCUL DE TOTALS ---
    total_fulls = 0
    total_examens = 0
    total_caixes = 0
    total_examens_cat = 0
    total_examens_cast = 0

    for nom_caixa in caixes:
        total_fulls = total_fulls +  caixes[nom_caixa]['Fulls']
        total_examens = total_examens + caixes[nom_caixa]['Examens']
        total_caixes = total_caixes + caixes[nom_caixa]['Caixes a encarregar']
        total_examens_cat = total_examens_cat + caixes[nom_caixa]['Examens Català']
        total_examens_cast = total_examens_cast + caixes[nom_caixa]['Examens Castellà']

    # --- DATAFRAMES A EXPORTAR ---
    execucio_datetime = datetime.now()

    # LK_ANY
    df_LK_ANY = pd.DataFrame([[ANY_CONVOC]], columns =['ANY_CONVOC'])

    # LK_VERSIO
    df_LK_VERSIO = pd.DataFrame([[execucio_datetime.strftime("%Y-%m-%dT%H:%M:%S")]], columns =['DIA_CALCUL'])

    # LK_CAIXA
    CAIXA_ID_values = list(dic_caixes.keys())
    DIA_PAU_values = [dic_caixes_dia.get(c) for c in list(dic_caixes.keys())]

    df_LK_CAIXA = pd.DataFrame({'CAIXA_ID': CAIXA_ID_values, 'DIA_PAU': DIA_PAU_values})

    # LK_UNIVERSITAT
    UNIVERSITAT_ID_values = []

    for value in dic_seu_uni.values():
        if value not in UNIVERSITAT_ID_values:
            UNIVERSITAT_ID_values.append(value)

    UNIVERSITAT_ID_values.append('Oficina d\'Accés a la Universitat (OAU)')

    df_LK_UNIVERSITAT = pd.DataFrame({'UNIVERSITAT_ID': UNIVERSITAT_ID_values})

    # LK_SEU
    SEU_ID_values = []

    for value in dic_trib_seu.values():
        if value not in SEU_ID_values:
            SEU_ID_values.append(value)

    UNIVERSITAT_ID_values = [dic_seu_uni.get(i) for i in SEU_ID_values]

    dic_seu_ntribs = {}
    for value in dic_trib_seu.values():
        if value in dic_seu_ntribs:
            dic_seu_ntribs[value] += 1
        else:
            dic_seu_ntribs[value] = 1

    NRE_TRIBUNALS_values = [dic_seu_ntribs.get(i) for i in SEU_ID_values]

    SEU_ID_values.append('Oficina d\'Accés a la Universitat (OAU)')
    UNIVERSITAT_ID_values.append('Oficina d\'Accés a la Universitat (OAU)')
    NRE_TRIBUNALS_values.append(0)

    df_LK_SEU = pd.DataFrame({'SEU_ID': SEU_ID_values, 
                              'UNIVERSITAT_ID': UNIVERSITAT_ID_values,
                              'NRE_TRIBUNALS': NRE_TRIBUNALS_values}).sort_values(by='SEU_ID')

    # LK_TRIBUNAL
    TRIBUNAL_ID_values = list(dic_trib_seu.keys())
    SEU_ID_values = [dic_trib_seu.get(c) for c in TRIBUNAL_ID_values]

    TRIBUNAL_ID_values.append('000000')
    SEU_ID_values.append('Oficina d\'Accés a la Universitat (OAU)')

    df_LK_TRIBUNAL = pd.DataFrame({'TRIBUNAL_ID': TRIBUNAL_ID_values, 'SEU_ID': SEU_ID_values})

    # LK_CENTRE
    TRIBUNAL_ID_values = list(dic_trib_seu.keys())
    CENTRE_ID_values = [dic_trib_centre.get(c) for c in TRIBUNAL_ID_values]

    TRIBUNAL_ID_values.append('000000')
    CENTRE_ID_values.append('Oficina d\'Accés a la Universitat (OAU)')

    df_LK_CENTRE = pd.DataFrame({ 'CENTRE_ID': CENTRE_ID_values,'TRIBUNAL_ID': TRIBUNAL_ID_values})


    # LK_MATERIA
    DES_MATERIA_values = list(data_assignatures['Assignatura'])
    ID_MATERIA_values = [dic_assig_codi.get(c) for c in DES_MATERIA_values]

    df_LK_MATERIA = pd.DataFrame({'ID_MATERIA': ID_MATERIA_values,'DES_MATERIA': DES_MATERIA_values})

    # LK_IDIOMA
    df_LK_IDIOMA = pd.DataFrame([['Català'], ['Castellà']], columns =['ID_IDIOMA'])

    # REL_DIA_CAIXA_MATERIA
    ANY_CONVOC_values = [] 
    DIA_CALCUL_values = [] 
    CAIXA_ID_values = []
    MATERIA_ID_values = []
    IDIOMA_ID_values = []
    CONTINGUT_CAIXA_values = []

    for nom_caixa in caixes: 
        for i, assig in enumerate(caixes[nom_caixa]['Assignatures']):
            ANY_CONVOC_values.append(ANY_CONVOC)
            DIA_CALCUL_values.append(execucio_datetime.strftime("%Y-%m-%dT%H:%M:%S"))
            CAIXA_ID_values.append(nom_caixa)
            MATERIA_ID_values.append(dic_assig_codi[assig])
            IDIOMA_ID_values.append('Català')
            CONTINGUT_CAIXA_values.append(caixes[nom_caixa]['n_cat'][i])

        for i, assig in enumerate(caixes[nom_caixa]['Assignatures']):
            ANY_CONVOC_values.append(ANY_CONVOC)
            DIA_CALCUL_values.append(execucio_datetime.strftime("%Y-%m-%dT%H:%M:%S"))
            CAIXA_ID_values.append(nom_caixa)
            MATERIA_ID_values.append(dic_assig_codi[assig])
            IDIOMA_ID_values.append('Castellà')
            CONTINGUT_CAIXA_values.append(caixes[nom_caixa]['n_cast'][i])

    df_REL_DIA_CAIXA_MATERIA = pd.DataFrame({'ANY_CONVOC': ANY_CONVOC_values, 
                                            'DIA_CALCUL': DIA_CALCUL_values, 
                                            'CAIXA_ID': CAIXA_ID_values,
                                            'MATERIA_ID': MATERIA_ID_values,
                                            'IDIOMA_ID': IDIOMA_ID_values,
                                            'CONTINGUT_CAIXA': CONTINGUT_CAIXA_values})


    # REL_CAIXES_PER_TRIBUNAL
    ANY_CONVOC_values = [] 
    DIA_CALCUL_values = [] 
    CAIXA_ID_values = []
    TRIBUNAL_ID_values = []
    NRE_CAIXES_values = []

    for nom_caixa in caixes: 
        for i, trib in enumerate(caixes[nom_caixa]['Tribunals']):
            ANY_CONVOC_values.append(ANY_CONVOC)
            DIA_CALCUL_values.append(execucio_datetime.strftime("%Y-%m-%dT%H:%M:%S"))
            CAIXA_ID_values.append(nom_caixa)
            TRIBUNAL_ID_values.append(trib)
            NRE_CAIXES_values.append(caixes[nom_caixa]['x'][i])

        # Tribunal reserva
        ANY_CONVOC_values.append(ANY_CONVOC)
        DIA_CALCUL_values.append(execucio_datetime.strftime("%Y-%m-%dT%H:%M:%S"))
        CAIXA_ID_values.append(nom_caixa)
        TRIBUNAL_ID_values.append('000000')
        NRE_CAIXES_values.append(caixes[nom_caixa]['N Caixes reserva'])


    df_REL_CAIXES_PER_TRIBUNAL = pd.DataFrame({'ANY_CONVOC': ANY_CONVOC_values, 
                                            'DIA_CALCUL': DIA_CALCUL_values, 
                                            'CAIXA_ID': CAIXA_ID_values,
                                            'TRIBUNAL_ID': TRIBUNAL_ID_values,
                                            'NRE_CAIXES': NRE_CAIXES_values})



    # REL_EXAMENS_PER_TRIBUNAL
    ANY_CONVOC_values = [] 
    DIA_CALCUL_values = [] 
    MATERIA_ID_values = []
    IDIOMA_ID_values = []
    TRIBUNAL_ID_values = []
    NRE_EXAMENS_DEMANDA_values = []
    NRE_EXAMENS_IMPRIMIR_values = []
    NRE_EXAMENS_SOBRANTS_values = []
    VALIDACIO_values = []


    for nom_caixa in caixes: 
        for i, assig in enumerate(caixes[nom_caixa]['Assignatures']):
            idiomes_assig = dic_assig_idioma[assig]
            for idioma_impresio in ['Català','Castellà']:
                for j, trib in enumerate(caixes[nom_caixa]['Tribunals']):
                    ANY_CONVOC_values.append(ANY_CONVOC)
                    DIA_CALCUL_values.append(execucio_datetime.strftime("%Y-%m-%dT%H:%M:%S"))
                    MATERIA_ID_values.append(dic_assig_codi[assig])
                    IDIOMA_ID_values.append(idioma_impresio)
                    TRIBUNAL_ID_values.append(trib)
                    
                    if idioma_impresio == 'Català':
                        if idiomes_assig == 'Català':
                            NRE_EXAMENS_DEMANDA_values.append(caixes[nom_caixa]['Previsió original'].loc[assig,('PREVISIO_PREMATRICULA', trib)])
                        elif idiomes_assig == 'Castellà':
                            NRE_EXAMENS_DEMANDA_values.append(0)
                        elif idiomes_assig == 'Mixt':
                            NRE_EXAMENS_DEMANDA_values.append(caixes[nom_caixa]['Previsió original'].loc[assig,('PREVISIO_PREMATRICULA', trib)])

                        NRE_EXAMENS_IMPRIMIR_values.append(caixes[nom_caixa]['n_cat'][i]*caixes[nom_caixa]['x'][j])
                    
                    elif idioma_impresio == 'Castellà':
                        if dic_assig_idioma[assig] == 'Català':
                            NRE_EXAMENS_DEMANDA_values.append(0)
                        elif dic_assig_idioma[assig] == 'Castellà':
                            NRE_EXAMENS_DEMANDA_values.append(caixes[nom_caixa]['Previsió original'].loc[assig,('PREVISIO_PREMATRICULA', trib)])
                        elif dic_assig_idioma[assig] == 'Mixt':
                            NRE_EXAMENS_DEMANDA_values.append(0)

                        NRE_EXAMENS_IMPRIMIR_values.append(caixes[nom_caixa]['n_cast'][i]*caixes[nom_caixa]['x'][j])
                        
                    NRE_EXAMENS_SOBRANTS_values.append(NRE_EXAMENS_IMPRIMIR_values[-1] - NRE_EXAMENS_DEMANDA_values[-1])

                # Tribunal de reserva
                ANY_CONVOC_values.append(ANY_CONVOC)
                DIA_CALCUL_values.append(execucio_datetime.strftime("%Y-%m-%dT%H:%M:%S"))
                MATERIA_ID_values.append(dic_assig_codi[assig])
                IDIOMA_ID_values.append(idioma_impresio)
                TRIBUNAL_ID_values.append('000000')
                NRE_EXAMENS_DEMANDA_values.append(0)
                if idioma_impresio == 'Català':
                    NRE_EXAMENS_IMPRIMIR_values.append(caixes[nom_caixa]['n_cat'][i]*caixes[nom_caixa]['N Caixes reserva'])
                elif idioma_impresio == 'Castellà':
                    NRE_EXAMENS_IMPRIMIR_values.append(caixes[nom_caixa]['n_cast'][i]*caixes[nom_caixa]['N Caixes reserva'])
                NRE_EXAMENS_SOBRANTS_values.append(NRE_EXAMENS_IMPRIMIR_values[-1] - NRE_EXAMENS_DEMANDA_values[-1])

            
    df_REL_EXAMENS_PER_TRIBUNAL = pd.DataFrame({'ANY_CONVOC': ANY_CONVOC_values, 
                                            'DIA_CALCUL': DIA_CALCUL_values, 
                                            'MATERIA_ID': MATERIA_ID_values,
                                            'IDIOMA_ID': IDIOMA_ID_values,
                                            'TRIBUNAL_ID': TRIBUNAL_ID_values,
                                            'NRE_EXAMENS_DEMANDA': NRE_EXAMENS_DEMANDA_values,
                                            'NRE_EXAMENS_IMPRIMIR': NRE_EXAMENS_IMPRIMIR_values,
                                            'NRE_EXAMENS_SOBRANTS': NRE_EXAMENS_SOBRANTS_values})

    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    # --- EXPORTACIÓ A CSV ---
    new_blob_lk_any = df_LK_ANY.to_csv(index=False)       
    blob_client = blob_service_client.get_blob_client(container=container_salida, blob='LK_ANY.csv')
    blob_client.upload_blob(new_blob_lk_any, overwrite=True)
    #
    new_blob_lk_versio = df_LK_VERSIO.to_csv(index=False)        
    blob_client = blob_service_client.get_blob_client(container=container_salida, blob='LK_VERSIO.csv')
    blob_client.upload_blob(new_blob_lk_versio, overwrite=True)
    #
    new_blob_lk_caixa = df_LK_CAIXA.to_csv(index=False)        
    blob_client = blob_service_client.get_blob_client(container=container_salida, blob='LK_CAIXA.csv')
    blob_client.upload_blob(new_blob_lk_caixa, overwrite=True)
    #
    new_blob_lk_universitat = df_LK_UNIVERSITAT.to_csv(index=False)        
    blob_client = blob_service_client.get_blob_client(container=container_salida, blob='LK_UNIVERSITAT.csv')
    blob_client.upload_blob(new_blob_lk_universitat, overwrite=True)
    #
    new_blob_lk_seu = df_LK_SEU.to_csv(index=False)        
    blob_client = blob_service_client.get_blob_client(container=container_salida, blob='LK_SEU.csv')
    blob_client.upload_blob(new_blob_lk_seu, overwrite=True)
    #
    new_blob_lk_tribunal = df_LK_TRIBUNAL.to_csv(index=False)        
    blob_client = blob_service_client.get_blob_client(container=container_salida, blob='LK_TRIBUNAL.csv')
    blob_client.upload_blob(new_blob_lk_tribunal, overwrite=True)
    #
    new_blob_lk_centre = df_LK_CENTRE.to_csv(index=False)        
    blob_client = blob_service_client.get_blob_client(container=container_salida, blob='LK_CENTRE.csv')
    blob_client.upload_blob(new_blob_lk_centre, overwrite=True)
    #
    new_blob_lk_materia = df_LK_MATERIA.to_csv(index=False)        
    blob_client = blob_service_client.get_blob_client(container=container_salida, blob='LK_MATERIA.csv')
    blob_client.upload_blob(new_blob_lk_materia, overwrite=True)
    #
    new_blob_lk_idioma = df_LK_IDIOMA.to_csv(index=False)        
    blob_client = blob_service_client.get_blob_client(container=container_salida, blob='LK_IDIOMA.csv')
    blob_client.upload_blob(new_blob_lk_idioma, overwrite=True)
    #
    new_blob_rel_caixa_mat = df_REL_DIA_CAIXA_MATERIA.to_csv(index=False)        
    blob_client = blob_service_client.get_blob_client(container=container_salida, blob='REL_DIA_CAIXA_MATERIA.csv')
    blob_client.upload_blob(new_blob_rel_caixa_mat, overwrite=True)
    #
    new_blob_rel_caixes_trib = df_REL_CAIXES_PER_TRIBUNAL.to_csv(index=False)        
    blob_client = blob_service_client.get_blob_client(container=container_salida, blob='df_REL_CAIXES_PER_TRIBUNAL.csv')
    blob_client.upload_blob(new_blob_rel_caixes_trib, overwrite=True)
    #
    new_blob_rel_examens_trib = df_REL_EXAMENS_PER_TRIBUNAL.to_csv(index=False)        
    blob_client = blob_service_client.get_blob_client(container=container_salida, blob='df_REL_EXAMENS_PER_TRIBUNAL.csv')
    blob_client.upload_blob(new_blob_rel_examens_trib, overwrite=True)    
    
    #df_LK_ANY.to_csv(PATH_RESULTATS+'LK_ANY.csv',index=False)
    #df_LK_VERSIO.to_csv(PATH_RESULTATS+'LK_VERSIO.csv',index=False)
    #df_LK_CAIXA.to_csv(PATH_RESULTATS+'LK_CAIXA.csv',index=False)
    #df_LK_UNIVERSITAT.to_csv(PATH_RESULTATS+'LK_UNIVERSITAT.csv',index=False)
    #df_LK_SEU.to_csv(PATH_RESULTATS+'LK_SEU.csv',index=False)
    #df_LK_TRIBUNAL.to_csv(PATH_RESULTATS+'LK_TRIBUNAL.csv',index=False)
    #df_LK_CENTRE.to_csv(PATH_RESULTATS+'LK_CENTRE.csv',index=False)
    #df_LK_MATERIA.to_csv(PATH_RESULTATS+'LK_MATERIA.csv',index=False)
    #df_LK_IDIOMA.to_csv(PATH_RESULTATS+'LK_IDIOMA.csv',index=False)
    #df_REL_DIA_CAIXA_MATERIA.to_csv(PATH_RESULTATS+'REL_DIA_CAIXA_MATERIA.csv',index=False)
    #df_REL_CAIXES_PER_TRIBUNAL.to_csv(PATH_RESULTATS+'REL_CAIXES_PER_TRIBUNAL.csv',index=False)
    #df_REL_EXAMENS_PER_TRIBUNAL.to_csv(PATH_RESULTATS+'REL_EXAMENS_PER_TRIBUNAL.csv',index=False)


    # --- EXPORTACIÓ DE LOGs ---
    #log_name = "LOG_" + execucio_datetime.strftime("%Y_%m_%d_%Hh_%Mm_%Ss") + ".txt"
    #blob = BlobClient.from_connection_string(conn_str=conn_str, container_name="salida", blob_name=log_name)

    #data = "---------- CONVOCATÒRIA ---------- " + "\n" +
    #      "Any de la convocatòria: "+str(ANY_CONVOC) + "\n\n" +
    #       "---------- DATA D EXECUCIÓ ----------"+ "\n" +
    #       "Data d\'execució: '"+execucio_datetime.strftime("%Y/%m/%d %H:%M:%S") + "\n\n" +
    #       "---------- VALIDACIÓ  ----------"+ "\n" +
    #       resultat_validacio + "\n\n" +           
           
    #blob.upload_blob(data)

    #log_name = PATH_RESULTATS + "LOG_" + execucio_datetime.strftime("%Y_%m_%d_%Hh_%Mm_%Ss") + ".txt"

    #with open(log_name, "w", encoding='utf-8') as f: 
        # Informació sobre la convocatoria'
        #f.write('---------- CONVOCATÒRIA ----------'+ "\n")
        #f.write('Any de la convocatòria: '+str(ANY_CONVOC) + "\n\n")

        # Informació sobre la data d'execució'
        #f.write('---------- DATA D\'EXECUCIÓ ----------'+ "\n")
        #f.write('Data d\'execució: '+execucio_datetime.strftime("%Y/%m/%d %H:%M:%S") + "\n\n")

        # Informació sobre la validació de la cobertura de previsió
        #f.write('---------- VALIDACIÓ  ----------'+ "\n")
        #f.write(resultat_validacio + "\n\n")

        # Nombre de fulls per caixa
        #f.write('Fulls per caixa: \n')
        #for nom_caixa in caixes:
        #    f.write(str(caixes[nom_caixa]['Fulls per caixa'])+'\t'+ nom_caixa+'\n')

        # Informació general
        #f.write('\n---------- INFORMACIÓ GENERAL ----------'+ "\n")
        #f.write('TOTAL Fulls: ' + str(total_fulls)+ "\n")
        #f.write('TOTAL Exàmens: ' + str(total_examens)+ "\n")
        #f.write('TOTAL Exàmens en Català: ' + str(total_examens_cat)+ "\n")
        #f.write('TOTAL Exàmens en Castellà: ' + str(total_examens_cast)+ "\n")
        #f.write('TOTAL Caixes: ' + str(total_caixes)+ "\n\n")

        # Caixes de reserva
        #f.write('Caixes de reserva: \n')
        #for nom_caixa in caixes:
        #    f.write(str(caixes[nom_caixa]['N Caixes reserva'])+'\t'+ nom_caixa+'\n')

        # Informació de cada caixa
        #f.write('\n------------- INFORMACIÓ SOBRE LES CAIXES -------------\n')

        #for nom_caixa in caixes:
        #    f.write('\n--------------------- '+nom_caixa+' ---------------------'+ "\n")
            
            # Informació del nombre d'exàmens per caixa
        #    f.write('----- NOMBRE D\'EXÀMENS PER CAIXA -----'+ "\n")
        #    f.write('----- Examens en Català -----'+ "\n")
        #    for i, assig in enumerate(caixes[nom_caixa]['Assignatures']):
        #        f.write(str(caixes[nom_caixa]['n_cat'][i])+ '\t'+ assig+ "\n")
            
        #    f.write('\n----- Examens en Castellà -----'+ "\n")
        #    for i, assig in enumerate(caixes[nom_caixa]['Assignatures']):
        #        f.write(str(caixes[nom_caixa]['n_cast'][i])+ '\t'+ assig+ "\n")
            
            # Informació addicional de la caixa
        #    f.write('\n----- INFORMACIÓ ADDICIONAL -----'+ "\n\n")
        #    f.write('Fulls per caixa: ' + str(caixes[nom_caixa]['Fulls per caixa'])+ "\n")
        #    f.write('Exàmens per caixa: ' + str(caixes[nom_caixa]['Examens per caixa'])+ "\n\n")

        #    f.write('Exàmens en Català per caixa: ' + str(caixes[nom_caixa]['Examens Català per caixa'])+ "\n")
        #    f.write('Exàmens en Castellà per caixa: ' + str(caixes[nom_caixa]['Examens Castellà per caixa'])+ "\n\n")

        #    f.write('Caixes a encarregar: ' + str(caixes[nom_caixa]['Caixes a encarregar']))
            
        #    f.write('\n------------------------------------------------------'+ "\n")

        # Informació sobre el repartiment
        #f.write("\n"+'------------- INFORMACIÓ SOBRE EL REPARTIMENT -------------'+"\n")

        # Nombre de caixes (x)
        #for nom_caixa in caixes:
        #    f.write('\n--------------------- '+nom_caixa+' ---------------------'+ "\n")
        #    for i, trib in enumerate(caixes[nom_caixa]['Tribunals']):
        #        f.write(str(caixes[nom_caixa]['x'][i])+ '\t'+ trib+'\n')
        #    f.write('------------------------------------------------------'+ "\n")"""
            
    print('CSV files created successfully! hey')

    return 'CSV file created successfully!'