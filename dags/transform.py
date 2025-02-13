def transform_data_callable(raw_data):
    # Transform response to a list
    transformed_data = [
        [
            raw_data.get("date"),
            raw_data.get("location"),
            raw_data.get("weather").get("temp"),
            raw_data.get("weather").get("conditions")
        ]
    ]
    return transformed_data
