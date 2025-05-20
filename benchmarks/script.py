import subprocess
import time
import random
import string
import logging
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Configuration
DUCKDB_PATH = "../build/release/duckdb"
DATABASE_FILE = "benchmark.duckdb"
REPEATS = 5  # Number of query executions for averaging

# Logging setup
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

def generate_values(data_type, cardinality, num_rows):
    if data_type == "INTEGER":
        values = [random.randint(0, cardinality - 1) for _ in range(num_rows)]
    elif data_type == "VARCHAR":
        alphabet = string.ascii_lowercase
        distinct = [''.join(random.choices(alphabet, k=5)) for _ in range(cardinality)]
        values = [random.choice(distinct) for _ in range(num_rows)]
    else:
        raise ValueError("Unsupported data type")
    return values

def run_duckdb_script(script):
    result = subprocess.run(
        [DUCKDB_PATH, DATABASE_FILE],
        input=script,
        text=True,
        capture_output=True
    )
    if result.stderr:
        logging.error(result.stderr.strip())
    return result.stdout.strip()

def create_table_and_insert(table_name, column_name, values, data_type):
    logging.info(f"Creating and populating table '{table_name}'")
    create = f"DROP TABLE IF EXISTS {table_name}; CREATE TABLE {table_name}({column_name} {data_type});"
    inserts = "\n".join(
        f"INSERT INTO {table_name} VALUES ('{v}');" if data_type == "VARCHAR" else f"INSERT INTO {table_name} VALUES ({v});"
        for v in values
    )
    return f"{create}\n{inserts}"

def create_index(table, column, index_type):
    if index_type == "cubit":
        return f"CREATE INDEX idx_cubit_{table} ON {table} USING CUBIT({column});"
    elif index_type == "default":
        return f"CREATE INDEX idx_default_{table} ON {table}({column});"
    elif index_type == "none":
        return "-- no index"
    else:
        raise ValueError("Unknown index type")

def measure_query_time(table, column, value, data_type):
    val = f"'{value}'" if data_type == "VARCHAR" else f"{value}"
    query = f"SELECT * FROM {table} WHERE {column} = {val};"

    start = time.perf_counter()
    run_duckdb_script(query)
    end = time.perf_counter()

    return end - start

def benchmark_case(data_type, num_rows, cardinality, index_type):
    column_name = "col"
    table_name = f"{data_type.lower()}_{index_type}"
    values = generate_values(data_type, cardinality, num_rows)
    target = values[random.randint(0, len(values) - 1)]

    logging.info(f"Benchmarking: Type={data_type}, Rows={num_rows}, Card={cardinality}, Index={index_type}")

    script = create_table_and_insert(table_name, column_name, values, data_type)
    script += "\n" + create_index(table_name, column_name, index_type)
    run_duckdb_script(script)

    timings = []
    for _ in range(REPEATS):
        duration = measure_query_time(table_name, column_name, target, data_type)
        timings.append(duration)

    avg_time = sum(timings) / REPEATS
    logging.info(f"Avg Time: {avg_time:.6f} seconds")
    return avg_time

def run_benchmarks(configs):
    results = []
    for config in configs:
        for index_type in ["none", "default", "cubit"]:
            avg_time = benchmark_case(
                config["data_type"],
                config["num_rows"],
                config["cardinality"],
                index_type
            )
            results.append({
                "DataType": config["data_type"],
                "Rows": config["num_rows"],
                "Cardinality": config["cardinality"],
                "Index": index_type,
                "AvgTime": avg_time
            })
    return pd.DataFrame(results)

def visualize_results(df, output_path="benchmark_results.png"):
    import warnings
    warnings.filterwarnings("ignore", category=UserWarning)

    df["Cardinality"] = df["Cardinality"].astype(int)
    sns.set(style="whitegrid")
    plt.figure(figsize=(12, 6))
    sns.barplot(data=df, x="Cardinality", y="AvgTime", hue="Index", errorbar=None)
    plt.title("Query Execution Time by Index Type")
    plt.ylabel("Average Time (s)")
    plt.xlabel("Cardinality")
    plt.tight_layout()
    plt.savefig(output_path)
    logging.info(f"Saved plot to {output_path}")
    # plt.show()  # Commented out since not useful in non-GUI shell

def main():
    test_configs = [
        {"data_type": "INTEGER", "num_rows": 10000, "cardinality": c} for c in [10]
    ] 
    
    # + [
    #     {"data_type": "VARCHAR", "num_rows": 1000, "cardinality": c} for c in [10]
    # ]

    results_df = run_benchmarks(test_configs)
    results_df.to_csv("benchmark_results.csv", index=False)
    visualize_results(results_df)

if __name__ == "__main__":
    main()
