"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


columnas = [(0, 9), (9, 25), (25, 41), (41, 118)]
nombre_columnas = {
    0: 'cluster',
    1: 'cantidad_de_palabras_clave',
    2: 'porcentaje_de_palabras_clave',
    3: 'principales_palabras_clave',
}

def ingest_data():
    #
    # Inserte su código aquí
    #
    
    # https://pandas.pydata.org/docs/reference/api/pandas.read_fwf.html
    # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.fillna.html
    df = (pd.read_fwf('clusters_report.txt', skiprows=4, colspecs=columnas, header=None)
          .rename(columns=nombre_columnas)
          .fillna(method='ffill')
          )
    
    # Limpieza: poner los datos en el formato adecuado
    df['porcentaje_de_palabras_clave'] = (df.porcentaje_de_palabras_clave
                                          .str.replace(' %', '')
                                          .str.replace(',', '.')
                                          .astype(float)
                                          )
    
    df['cantidad_de_palabras_clave'] = df.cantidad_de_palabras_clave.astype(int)
    df['cluster'] = df.cluster.astype(int)

    # Agrupar las palabras clave, asociándolas a las 3 primeras columnas
    df = (df.groupby(['cluster', 'cantidad_de_palabras_clave', 'porcentaje_de_palabras_clave'], as_index=False)['principales_palabras_clave']
          .apply(lambda x: ' '.join(x))) # junta todos los renglones en uno solo
    
    df['principales_palabras_clave'] = df.principales_palabras_clave.str.split(',').apply(lambda x: transformar(x))
    

    return df

# contiene toda la lógica para procesar los renglones juntos, para que queden con el formato adecuado
def transformar(lista):
    respuesta = []
    for elemento in lista:
        keyword = elemento.replace('.', '').strip()
        keyword = keyword.split(' ')
        keyword = [word for word in keyword if word != ''] # dejar todo lo que no sean strings vacios
        keyword = ' '.join(keyword) # volvemos a unir todo para que sólo quede con máximo un espacio
        
        respuesta.append(keyword)
    
    return ', '.join(respuesta) # unimos todas las palabras clave con ', '
