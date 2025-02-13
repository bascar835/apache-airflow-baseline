def extract_data_callable():
    # Print message, return a response
    print("Extracting data from a weather API")
    return {
        "date": "2023-01-01",
        "location": "NYC",
        "weather": {
            "temp": 33,
            "conditions": "Light snow and wind"
        }
    }
