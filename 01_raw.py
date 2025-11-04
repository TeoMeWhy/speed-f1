# %%

import argparse

from tqdm import tqdm

import pandas as pd
import fastf1

fastf1.set_log_level(level=100)

import time

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)

# %%

class Loader:

    def __init__(self, start, stop, identifiers):
        self.start = start
        self.stop = stop
        self.identifiers = identifiers


    def get_data(self, year, gp, identifier):

        try:
            session = fastf1.get_session(year, gp, identifier)
            session.load(
                laps=False,
                telemetry=False,
                weather=False,
                messages=False,
            )
            
            df = session.results
            df['identifier'] = identifier
            df['date'] = session.date
            df['date'] = df['date'].astype(str)
            df['year'] = session.date.year
            df['RoundNumber'] = int(session.event['RoundNumber'])
            df['Country'] = session.event['Country']
            df['Location'] = session.event['Location']
            df['OfficialEventName'] = session.event['OfficialEventName']
            df = df.reset_index(drop=True)
            return df

        except ValueError as err:
            # print(err)
            # print("Erro capturado!")
            return pd.DataFrame()
        
    def save_data(self, year:int, gp:int, identifier:str, df:pd.DataFrame):
        file_name = f"data/raw/{year}_{gp:02d}_{identifier}.parquet"
        df.to_parquet(file_name)
        print(file_name, "salvo...")


    def process_one(self, year:int, gp:int, identifier:str):
        df = self.get_data(year, gp, identifier)
        if df.shape[0] == 0:
            return False
        else:
            self.save_data(year, gp, identifier, df)
            return True

    def process_year(self, year):
        df_rounds = fastf1.get_event_schedule(year)
        year_rounds = df_rounds['RoundNumber'].max()
        for gp in range(1, year_rounds+1):
            for identifier in self.identifiers:
                result = self.process_one(year, gp, identifier)
                if not result and identifier == 'race':
                    return
            
    def process_start_stop(self):
        
        iter = [self.start + i for i in range(0, self.stop - self.start + 1)]
        
        for i in tqdm(iter):
            self.process_year(i)
            time.sleep(10)

# %%

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int, default=2025, help='ano de início')
    parser.add_argument("--stop", type=int, default=2025, help='ano de término')
    parser.add_argument("--identifiers", nargs='*', default=['race', 'sprint'])
    args = parser.parse_args()
    
    loader = Loader(args.start, args.stop, args.identifiers)
    loader.process_start_stop()

if __name__ == "__main__":
    main()