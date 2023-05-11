# --- LLIBRERIES ---
from azure.storage.blob import BlobServiceClient
from flask import Flask
from io import StringIO
import os
import csv
import pandas as pd
from datetime import datetime
from solver import *
import shutil

app = Flask(__name__)

# Este controla la pagina inicial de nuestra Web App
@app.route('/')
def index():
   return "¡La app está activa!"

# Mover los ficheros generados
@app.route('/moure_fitxers', methods=['GET'])

def moure_fitxers():    
    execucio_datetime = datetime.now()
    container_hist = 'innenu-historics'  
    #conn_str = 'DefaultEndpointsProtocol=https;AccountName=stfwehqdcdes;AccountKey=gPydOfNRuyS5spnv80AXBue7fJwbC5DqyPTRzx5Djc8ZxvkVgo11CAvtfv8IE3P1BJQ0TXPiXXac+AStT8N+UQ==;EndpointSuffix=core.windows.net'
    conn_str = os.environ['CUSTOMCONNSTR_blobstorage']
    container_name = "innenu-sortida"

    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    container_client = blob_service_client.get_container_client(container=container_name)
    blob_list = container_client.list_blobs()   


    for blob in blob_list:
        print(f"Name: {blob.name}")
        blob_name = blob.name
        new_blob_name = blob_name.split('.')[0]+'_'+execucio_datetime.strftime("%Y-%m-%dT%H:%M:%S")+".csv"        
        source_blob = blob_service_client.get_blob_client(container_name, blob_name)        
        dest_blob = blob_service_client.get_blob_client(container_hist,new_blob_name)

        # Copy the blob to the new name
        dest_blob.start_copy_from_url(source_blob.url)
        # Delete the original blob
        #blob_client.delete_blob()
        print("The blob was Renamed successfully:",{new_blob_name})   

    return 'Proceso moure_fitxers Terminado con Exito'

# Concatenacion de los ficheros
@app.route('/concatenate', methods=['GET'])
def concatenate():    
    #conn_str = 'DefaultEndpointsProtocol=https;AccountName=stfwehqdcdes;AccountKey=gPydOfNRuyS5spnv80AXBue7fJwbC5DqyPTRzx5Djc8ZxvkVgo11CAvtfv8IE3P1BJQ0TXPiXXac+AStT8N+UQ==;EndpointSuffix=core.windows.net'
    conn_str = os.environ['CUSTOMCONNSTR_blobstorage']
    container_salida = 'innenu-sortida'

    # Lectura del fichero de Versio
    blob_lk_versio = 'innenu-sortida/LK_VERSIO.csv'
    df_LK_VERSIO = pd.read_csv(f"abfs://{blob_lk_versio}",
                                    storage_options={"connection_string": conn_str}, 
                                    )

    versio = df_LK_VERSIO.iloc[-1][0]
    print(versio)

    #Llamada a la funcion que genera el fichero LK_CAIXA concatenado
    generate_file('LK_CAIXA',versio)    
    generate_file('REL_CAIXES_PER_TRIBUNAL',versio)
    generate_file('REL_DIA_CAIXA_MATERIA',versio)
    generate_file('REL_EXAMENS_PER_TRIBUNAL',versio)

    return 'Proceso Concatenate Terminado con Exito'

def generate_file(nom_file,version):
    
    #conn_str = 'DefaultEndpointsProtocol=https;AccountName=stfwehqdcdes;AccountKey=gPydOfNRuyS5spnv80AXBue7fJwbC5DqyPTRzx5Djc8ZxvkVgo11CAvtfv8IE3P1BJQ0TXPiXXac+AStT8N+UQ==;EndpointSuffix=core.windows.net'
    conn_str = os.environ['CUSTOMCONNSTR_blobstorage']
    container_salida = 'innenu-sortida'
    #Borro el fichero por si existiera
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    blob_client = blob_service_client.get_blob_client(container=container_salida, blob=nom_file+'.csv')   
    
    existe_blob = blob_client.exists()
    if (existe_blob):            
        blob_client.delete_blob()
        print('Se ha borrado el fichero')

    # SE GENERA NUEVAMENTE
    container_client = blob_service_client.get_container_client(container=container_salida)
    blob_list_files = container_client.list_blobs(name_starts_with=nom_file)     
    
    df_data = [pd.read_csv(f"abfs://{container_salida+'/'+blob.name}",
                                storage_options={"connection_string": conn_str}
                                ,dtype=str) for blob in blob_list_files]
    concat = pd.concat(df_data)
    if 'DIA_CALCUL' in concat.columns:
        concat.loc[:, 'DIA_CALCUL'] = version
    
    concat.dropna(axis=0, how='all',inplace=True)
    # Crear un nuevo archivo CSV a partir del dataframe procesado
    new_blob_lk_any = concat.to_csv(index=False)    
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    blob_client = blob_service_client.get_blob_client(container=container_salida, blob=nom_file+'.csv')
    blob_client.upload_blob(new_blob_lk_any, overwrite=True)

    #Leer el fichero generado
    '''df_data = pd.read_csv(f"abfs://{container_salida+'/'+blob.name}",
                                storage_options={"connection_string": conn_str}
                                )

    Lines = df_data.readlines()'''

    print('Se ha generado el fichero concatenado '+ nom_file)
    return 'Se ha generado el fichero concatenado '



# Creación de los ficheros
@app.route('/caixa_1_A', methods=['GET'])
def caixa_1_A():        
    caixa("Caixa 1 A")
    return 'CSV files created successfully!'

@app.route('/caixa_1_B', methods=['GET'])
def caixa_1_B():        
    caixa("Caixa 1 B")
    return 'CSV files created successfully!'

@app.route('/caixa_2_A', methods=['GET'])
def caixa_2_A():        
    caixa("Caixa 2 A")
    return 'CSV files created successfully!'

@app.route('/caixa_2_B', methods=['GET'])
def caixa_2_B():        
    caixa("Caixa 2 B")
    return 'CSV files created successfully!'

@app.route('/caixa_3_A', methods=['GET'])
def caixa_3_A():        
    caixa("Caixa 3 A")
    return 'CSV files created successfully!'

@app.route('/caixa_3_B', methods=['GET'])
def caixa_3_B():        
    caixa("Caixa 3 B")
    return 'CSV files created successfully!'

## Funcion que genera los ficheros por cada caja
def caixa(nombre_caixa):
    conn_str = os.environ['CUSTOMCONNSTR_blobstorage']
    #conn_str = 'DefaultEndpointsProtocol=https;AccountName=stfwehqdcdes;AccountKey=gPydOfNRuyS5spnv80AXBue7fJwbC5DqyPTRzx5Djc8ZxvkVgo11CAvtfv8IE3P1BJQ0TXPiXXac+AStT8N+UQ==;EndpointSuffix=core.windows.net'
    container_salida = 'innenu-sortida'    

    blob_caixes = 'innenu-entrada/caixes.json'
    blob_excel = 'innenu-entrada/CalculEnunciatsCaixesExamens_NumeroAlumnes_Prematriculats.xlsx'
    SHEET_EXTRACCIO = 'Extraccio'
    SHEET_SEUS_TRIBUNALS = 'SeusTribunals'
    blob_asignatures = 'innenu-entrada/Dades_assignatures.xlsx'

    data_raw_previsio = pd.read_excel(
        f"abfs://{blob_excel}",
        storage_options={
            "connection_string": conn_str
    }, sheet_name=SHEET_EXTRACCIO)

    data_raw_tribunals = pd.read_excel(
        f"abfs://{blob_excel}",
        storage_options={
            "connection_string": conn_str
    }, sheet_name=SHEET_SEUS_TRIBUNALS)

    data_raw_assignatures = pd.read_excel(
        f"abfs://{blob_asignatures}",
        storage_options={
            "connection_string": conn_str
    })  
    
    #LECTURA DEL JSON
    data_raw_caixes = pd.read_json(f"abfs://{blob_caixes}",
                                    storage_options={"connection_string": conn_str}
                                    )
    # PROCESAMIENTO DE LOS DATOS
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
    # Caixes a optimitzar
    caixes_a_optimitzar = [nombre_caixa]
    dic_caixes = {x:dic_caixes[x] for x in caixes_a_optimitzar}

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
        n,x = optimizar_caixa(caixes[nom_caixa])

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

    # SUFIX PER ALS ARXIUS DE CADA CAIXA
    caixes_a_optimitzar_str = ''
    for c in caixes_a_optimitzar:
        caixes_a_optimitzar_str += ' '+ c 

    # TEMPS ACTUAL
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

    #################################################################################
    
    # --- EXPORTACIÓ A CSV ---

    # Crear un nuevo archivo CSV a partir del dataframe procesado
    new_blob_lk_any = df_LK_ANY.to_csv(index=False)    
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    blob_client = blob_service_client.get_blob_client(container=container_salida, blob='LK_ANY.csv')
    blob_client.upload_blob(new_blob_lk_any, overwrite=True)
    #
    new_blob_lk_versio = df_LK_VERSIO.to_csv(index=False)    
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    blob_client = blob_service_client.get_blob_client(container=container_salida, blob='LK_VERSIO.csv')
    blob_client.upload_blob(new_blob_lk_versio, overwrite=True)
    #
    new_blob_lk_caixa = df_LK_CAIXA.to_csv(index=False)    
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    blob_client = blob_service_client.get_blob_client(container=container_salida, blob='LK_CAIXA'+caixes_a_optimitzar_str+'.csv')
    blob_client.upload_blob(new_blob_lk_caixa, overwrite=True)
    #
    new_blob_lk_universitat = df_LK_UNIVERSITAT.to_csv(index=False)   
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)     
    blob_client = blob_service_client.get_blob_client(container=container_salida, blob='LK_UNIVERSITAT.csv')
    blob_client.upload_blob(new_blob_lk_universitat, overwrite=True)
    #
    new_blob_lk_seu = df_LK_SEU.to_csv(index=False)
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)        
    blob_client = blob_service_client.get_blob_client(container=container_salida, blob='LK_SEU.csv')
    blob_client.upload_blob(new_blob_lk_seu, overwrite=True)
    #
    new_blob_lk_tribunal = df_LK_TRIBUNAL.to_csv(index=False)
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)     
    blob_client = blob_service_client.get_blob_client(container=container_salida, blob='LK_TRIBUNAL.csv')
    blob_client.upload_blob(new_blob_lk_tribunal, overwrite=True)
    #
    new_blob_lk_centre = df_LK_CENTRE.to_csv(index=False)
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)        
    blob_client = blob_service_client.get_blob_client(container=container_salida, blob='LK_CENTRE.csv')
    blob_client.upload_blob(new_blob_lk_centre, overwrite=True)
    #
    new_blob_lk_materia = df_LK_MATERIA.to_csv(index=False)   
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)     
    blob_client = blob_service_client.get_blob_client(container=container_salida, blob='LK_MATERIA.csv')
    blob_client.upload_blob(new_blob_lk_materia, overwrite=True)
    #
    new_blob_lk_idioma = df_LK_IDIOMA.to_csv(index=False)  
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)      
    blob_client = blob_service_client.get_blob_client(container=container_salida, blob='LK_IDIOMA.csv')
    blob_client.upload_blob(new_blob_lk_idioma, overwrite=True)
    #
    new_blob_rel_caixa_mat = df_REL_DIA_CAIXA_MATERIA.to_csv(index=False)    
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)    
    blob_client = blob_service_client.get_blob_client(container=container_salida, blob='REL_DIA_CAIXA_MATERIA'+caixes_a_optimitzar_str+'.csv')
    blob_client.upload_blob(new_blob_rel_caixa_mat, overwrite=True)
    #
    new_blob_rel_caixes_trib = df_REL_CAIXES_PER_TRIBUNAL.to_csv(index=False)   
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)     
    blob_client = blob_service_client.get_blob_client(container=container_salida, blob='REL_CAIXES_PER_TRIBUNAL'+caixes_a_optimitzar_str+'.csv')
    blob_client.upload_blob(new_blob_rel_caixes_trib, overwrite=True)
    #
    new_blob_rel_examens_trib = df_REL_EXAMENS_PER_TRIBUNAL.to_csv(index=False)     
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)   
    blob_client = blob_service_client.get_blob_client(container=container_salida, blob='REL_EXAMENS_PER_TRIBUNAL'+caixes_a_optimitzar_str+'.csv')
    blob_client.upload_blob(new_blob_rel_examens_trib, overwrite=True)  
        
    print('CSV files created successfully! '+caixes_a_optimitzar_str)

    return 'CSV files created successfully!' + caixes_a_optimitzar_str


# Iniciamos nuestra app
if __name__ == '__main__':
   app.debug = True
   app.run()
