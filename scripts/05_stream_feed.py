# Simulates a live earthquake data stream using TCP socket
# Reads CSV file line-by-line and sends to localhost:9999
# This server allows other processes (like Spark Streaming) to consume real-time data

import socket
import time
from pathlib import Path

import pandas as pd


HOST = "localhost"
PORT = 9999
DELAY_SECONDS = 0.5


def format_row_for_stream(row: pd.Series) -> str:
    # Convert a DataFrame row to a comma-separated string for streaming
    # Cleans up the data by removing newlines and commas
    values = []
    for key in ["time", "latitude", "longitude", "depth", "mag", "place", "type", "region"]:
        value = row.get(key, "")
        if pd.isna(value):
            value = ""
        # Remove problematic characters that break CSV format
        text = str(value).replace("\n", " ").replace("\r", " ").replace(",", " ").strip()
        values.append(text)
    return ",".join(values)


def main() -> None:
    try:
        # Load earthquake data from CSV
        project_root = Path(__file__).resolve().parents[1]
        data_file = project_root / "data" / "earthquakes.csv"

        if not data_file.exists():
            print(f"✗ CSV file not found: {data_file}")
            return

        df = pd.read_csv(data_file)

        # Create TCP socket server
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((HOST, PORT))
            server_socket.listen(1)

            print(f"✓ Stream server started on {HOST}:{PORT}")
            print("  Waiting for client to connect...\n")

            # Wait for a client to connect
            client_socket, client_address = server_socket.accept()
            with client_socket:
                print(f"✓ Client connected: {client_address}\n")
                print(f"Streaming {len(df)} earthquake records...\n")

                # Send each row as a stream event
                for idx, (_, row) in enumerate(df.iterrows(), 1):
                    line = format_row_for_stream(row)
                    payload = f"{line}\n".encode("utf-8")
                    client_socket.sendall(payload)
                    
                    if idx % 10 == 0:
                        print(f"  Sent {idx}/{len(df)} records...")
                    time.sleep(DELAY_SECONDS)

                print(f"\n✓ Finished streaming all {len(df)} records")

    except KeyboardInterrupt:
        print("\n\n✓ Stream server stopped by user")
    except ConnectionError as error:
        print(f"✗ Connection error: {error}")
    except OSError as error:
        print(f"✗ Socket error: {error}")
    except Exception as error:
        print(f"✗ Error: {error}")


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
