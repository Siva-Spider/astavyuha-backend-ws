import json
import inspect
import os

def save_variable_to_json(variable, user_id, symbol, variable_name=None, folder="upstox_data_store"):
    # Auto-detect variable name if not given
    if variable_name is None:
        frame = inspect.currentframe().f_back
        for name, val in frame.f_locals.items():
            if val is variable:
                variable_name = name
                break

    if variable_name is None:
        raise ValueError("Variable name could not be determined. Pass variable_name explicitly.")

    os.makedirs(folder, exist_ok=True)

    # File name now includes user_id + symbol + variable_name
    filename = f"{folder}/{user_id}_{symbol}_{variable_name}.json"

    with open(filename, "w") as f:
        json.dump({variable_name: variable}, f, indent=4)

    return filename

def load_variable_from_json(user_id, symbol, variable_name, folder="upstox_data_store"):
    filename = f"{folder}/{user_id}_{symbol}_{variable_name}.json"

    # If file missing, return default 0
    if not os.path.exists(filename):
        return 0

    try:
        with open(filename, "r") as f:
            data = json.load(f)
        return data.get(variable_name, 0)   # fallback to 0 if key missing
    except Exception:
        return 0   # Safe fallback for corrupted JSON

def reset_json_variables(user_id, symbol, variable_names, folder="upstox_data_store"):
    for var in variable_names:
        filename = f"{folder}/{user_id}_{symbol}_{var}.json"
        try:
            with open(filename, "w") as f:
                json.dump({var: 0}, f, indent=4)
        except Exception:
            pass  # Safe fail
