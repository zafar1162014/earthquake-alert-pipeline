import socket
import time
from pathlib import Path

import pandas as pd


HOST = "localhost"
PORT = 9999
DELAY_SECONDS = 0.5


def to_stream_line(row: pd.Series) -> str:
    values = []
    for key in ["time", "latitude", "longitude", "depth", "mag", "place", "type", "region"]:
        value = row.get(key, "")
        if pd.isna(value):
            value = ""
        text = str(value).replace("\n", " ").replace("\r", " ").replace(",", " ").strip()
        values.append(text)
    return ",".join(values)


def main() -> None:
    try:
        project_root = Path(__file__).resolve().parents[1]
        data_file = project_root / "data" / "earthquakes.csv"

        if not data_file.exists():
            print(f"Error: CSV file not found: {data_file}")
            return

        dataframe = pd.read_csv(data_file)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((HOST, PORT))
            server_socket.listen(1)

            print("Stream server started on port 9999")
            print("Waiting for client connection...")

            client_socket, client_address = server_socket.accept()
            with client_socket:
                print(f"Client connected: {client_address}")

                for _, row in dataframe.iterrows():
                    line = to_stream_line(row)
                    payload = f"{line}\n".encode("utf-8")
                    client_socket.sendall(payload)
                    print(f"Sending: {line}")
                    time.sleep(DELAY_SECONDS)

                print("Streaming completed. No more rows to send.")

    except KeyboardInterrupt:
        print("\nStream server stopped by user.")
    except ConnectionError as error:
        print(f"Connection error: {error}")
    except OSError as error:
        print(f"Socket error: {error}")
    except Exception as error:  # noqa: BLE001
        print(f"Unexpected error: {error}")


if __name__ == "__main__":
    main()
