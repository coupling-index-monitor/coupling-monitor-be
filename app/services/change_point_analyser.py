import numpy as np
import pandas as pd

# Function to Detect Gradual Shifts (CUSUM)
def detect_cusum(df, column, threshold=0.5):
    """Detects gradual changes using CUSUM."""
    df["cusum"] = np.cumsum(df[column] - df[column].mean())
    change_points = df[df["cusum"].abs() > threshold * df["cusum"].std()]
    return change_points


# Visualization Function
def get_change_points(service_data, column):
    # Convert Data to DataFrame
    df = pd.DataFrame(service_data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    cusum_changes = detect_cusum(df, column)

    print("\nCUSUM Changes:")
    print(cusum_changes)
    print(cusum_changes[column].values)
