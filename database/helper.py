from config import settings
from database import MSSQLDatabase


def init_db_instance():
    return MSSQLDatabase()


def load_tickers():
    query = settings.DB_TICKERS_QUERY
    conn = init_db_instance()
    df = conn.select_table(query)
    data = []
    for ticker, bbg, curr in [list(row)[1:] for row in df.to_records("dict")]:
        data.append((ticker.replace(" ", ""), bbg, curr))

    return data
