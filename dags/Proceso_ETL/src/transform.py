#transform.py
# Asociadas a unos daots y su estructura

import pandas as pd

class TransformEnergy:
    @staticmethod
    def transform(data: pd.DataFrame, population_data: pd.DataFrame):
        import pandas as pd

        # Cargar el archivo CSV de energía renovable
        df1 = data
        df2 = population_data

        # Cargar el archivo CSV de energía renovable


        # Transponer los datos temporales
        df1 = df1.transpose()

        df1.columns = df1.iloc[0]

        df1 = df1.iloc[4:,]

        df1 = df1.drop(df1.index[-1])

        df1.index = df1.index.astype(int)   # Transformar los datos a enteros

        df1 = df1[(df1.index >= 1990) & (df1.index <= 2014)]

        media_por_columna = df1.mean()

        media_por_columna_df = pd.DataFrame(media_por_columna).T 

        media_por_columna_df.index = ['mean_energia']

        df1 = pd.concat([df1, media_por_columna_df])

        df2["mean1990-2014"] = (df2["2015 Population"] + df2["2010 Population"] + df2["2000 Population"] + df2["1990 Population"]) / 4

        df2 = df2[["Country/Territory", "mean1990-2014"]]

        poblacion_media = dict(zip(df2['Country/Territory'], df2['mean1990-2014']))

        df1.loc['Population_Mean'] = df1.columns.map(poblacion_media)

        df1.loc['Electricidad_per_capita'] = df1.loc["mean_energia"] / df1.loc["Population_Mean"]

        df1 = df1.loc[['Electricidad_per_capita','mean_energia','Population_Mean']].T

        df1["Country"] = df1.index

        col4 = df1.columns[3]  # La cuarta columna está en el índice 3

        # Paso 2: Reordenar las columnas
        df1 = df1[[col4] + [col for col in df1.columns if col != col4]]

        return df1 # Calcular emisiones promedio 
    
class TransformEmissions:
    @staticmethod
    def transform(data: pd.DataFrame, population_data: pd.DataFrame):
        df = data
        df1 = population_data

        df = df.groupby("country_or_area")["value"].mean().reset_index()

        df_merged = pd.merge(
            df, 
            df1, 
            left_on="country_or_area", 
            right_on="Country/Territory", 
            how="inner"  # Tipo de unión: inner, left, right, outer
        )

        df_merged["Population_media"] = (df_merged["2022 Population"]+df_merged["2020 Population"]+df_merged["2015 Population"]+df_merged["2000 Population"]+df_merged["1990 Population"]+df_merged["1980 Population"]+df_merged["1970 Population"])/7

        df_merged = df_merged.rename(columns={"value": "Emisiones_gas"})

        df_merged["Emisiones_per_capita"] = df_merged["Emisiones_gas"]/df_merged["Population_media"]

        df_merged[["country_or_area","Population_media","Emisiones_gas","Emisiones_per_capita"]]

        df_merged = df_merged[["country_or_area","Population_media","Emisiones_gas","Emisiones_per_capita"]].T

        df_merged.columns = df_merged.iloc[0]

        df_merged = df_merged.drop(df_merged.index[0])
        
        df_merged = df_merged.T

        df_merged["Country"] = df_merged.index

        col4 = df_merged.columns[3]  # La cuarta columna está en el índice 3

        # Paso 2: Reordenar las columnas
        df_merged = df_merged[[col4] + [col for col in df_merged.columns if col != col4]]

        return df_merged

class TransformPib:
    @staticmethod
    def transform(data: pd.DataFrame):
        df = data
        df.head()
        df = df.rename(columns={"gdppc": "PIB_per_capita"})
        df = df.rename(columns={"pop": "Poblacion"})

        df = df.groupby("country")[["PIB_per_capita","Poblacion"]].mean().reset_index()
        
        df["PIB"] = df["PIB_per_capita"] *df["Poblacion"]
        #df_merged["country","gdppc" ]

        df = df.T

        df.columns = df.iloc[0]

        df = df.drop(df.index[0])

        df = df.T

        df["Country"] = df.index
        col4 = df.columns[3]  # La cuarta columna está en el índice 3

        # Paso 2: Reordenar las columnas
        df = df[[col4] + [col for col in df.columns if col != col4]]

        
        return df


