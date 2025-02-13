import pandas as pd

def load_data_callable(transformed_data):
    # Load the data to a DataFrame, set the columns
    loaded_data = pd.DataFrame(transformed_data)
    loaded_data.columns = [
        "date",
        "location",
        "weather_temp",
        "weather_conditions"
    ]
    print(loaded_data)
