import math
from typing import List

def calculate_percentiles(data: List[float], percentiles_to_calc: List[int]) -> List[float]:
    '''Calculates specified percentiles for a list of data.

    Args:
        data: A list of numbers.
        percentiles_to_calc: A list of percentiles to calculate (e.g., [50, 95, 99]).

    Returns:
        A list of calculated percentile values corresponding to percentiles_to_calc.
    '''
    if not data:
        return [0.0] * len(percentiles_to_calc) # Return 0 or NaN if data is empty, matching desired output type

    data.sort()
    results = []
    for p in percentiles_to_calc:
        if not (0 <= p <= 100):
            raise ValueError("Percentile must be between 0 and 100")
        
        k = (len(data) - 1) * (p / 100.0)
        f = math.floor(k)
        c = math.ceil(k)

        if f == c: # Exact index
            results.append(data[int(k)])
        else: # Interpolate
            d0 = data[int(f)] * (c - k)
            d1 = data[int(c)] * (k - f)
            results.append(d0 + d1)
    return results 