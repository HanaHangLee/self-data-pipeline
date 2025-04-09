import pandas as pd

def load_and_resample_timeseries(
    filepath: str,
    timestamp_col: str = "Timestamp",
    drop_columns: list[str] = ["datetime"],
    resample_interval: str = "1min",
    keep_last_n: int = 402000
) -> pd.DataFrame:
    """
    Load a time series CSV file, convert timestamp to datetime, 
    optionally limit recent rows, set datetime index (but keep original column),
    drop unnecessary columns, and resample to fixed interval with forward-fill.

    Parameters:
        filepath (str): Path to the CSV file.
        timestamp_col (str): Name of the timestamp column.
        drop_columns (list[str]): Columns to drop.
        resample_interval (str): Pandas resampling string (e.g. '1min').
        keep_last_n (int): Number of last rows to keep (set None to keep all).

    Returns:
        pd.DataFrame: Resampled DataFrame indexed by datetime, with original timestamp column retained.
    """
    df = pd.read_csv(filepath)

    if keep_last_n is not None:
        df = df[-keep_last_n:].copy()

    df[timestamp_col] = pd.to_datetime(df[timestamp_col], unit='s')

    # Create a copy of timestamp column for indexing
    df = df.copy()
    df = df.set_index(timestamp_col, drop=False)

    # Drop specified columns if they exist
    df.drop(columns=[col for col in drop_columns if col in df.columns], inplace=True)

    df = df.sort_index()

    df_resampled = df.resample(resample_interval).ffill()

    # Chuyển cột datetime sang string
    df_resampled['datetime'] = df_resampled.index.strftime('%Y-%m-%d %H:%M:%S')
    df_resampled.drop(columns=[timestamp_col], inplace=True)
    df_resampled[timestamp_col] = df_resampled.index.astype('int64') // 10**9

    return df_resampled
