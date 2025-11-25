import os
from datetime import datetime

OUT_DIR = "data_output"
os.makedirs(OUT_DIR, exist_ok=True)


def run_load(df):
    """Very small load implementation for testing pipeline.
    Saves cleaned DataFrame to CSV in `data_output/` and returns number of rows written.
    """
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(OUT_DIR, f"clean_laptop_{timestamp}.csv")
        # Accept both pandas DataFrame or list
        try:
            import pandas as pd
            if hasattr(df, 'to_csv'):
                df.to_csv(out_path, index=False, encoding='utf-8-sig')
                rows = len(df)
            else:
                # assume list of dicts
                pd.DataFrame(df).to_csv(out_path, index=False, encoding='utf-8-sig')
                rows = len(df)
        except Exception:
            # Fallback: write simple text
            with open(out_path, 'w', encoding='utf-8') as f:
                f.write(str(df))
            rows = 0
        print(f"run_load: saved {rows} rows -> {out_path}")
        # record load run into DB (if log store available)
        try:
            from .log_store import insert_load
            insert_load(status='success', rows_inserted=rows, csv_path=out_path)
        except Exception:
            pass
        return rows
    except Exception as e:
        print(f"run_load error: {e}")
        try:
            from .log_store import insert_load
            insert_load(status='failure', message=str(e), rows_inserted=0)
        except Exception:
            pass
        return 0
