import os
import pandas as pd

dbutils.widgets.text("username", "regina.gurung@aol.com")
dbutils.widgets.text("password", "xxxxxxx")
dbutils.widgets.text("owner", "regina")
dbutils.widgets.text("status", "StandBy")

def get_etl_tracker_file(download_path: str, file_url: str) -> None:
  username, password = getArgument('username'), getArgument('password')
  
  from office365.runtime.auth.user_credential import UserCredential
  from office365.sharepoint.client_context import ClientContext
  
  url = "https://ktglbuc.sharepoint.com/sites/<folder-name>/"
  ctx = ClientContext(url).with_credentials(UserCredential(username, password))
  
  with open(download_path, "wb") as local_file:
    file = ctx.web.get_file_by_server_relative_path(file_url).download(local_file).execute_query()
    
  print(f"\nFile downloaded : {download_path}")
  
  def get_columns(all_columns: list) -> list:
  tmp_cols = dict()
  for column in all_columns:
    first_part = column[0]
    second_part = '' if str(column[1]).startswith('Unnamed') or str(column[1]).lower() == 'nan' else '-'+column[1]
    tmp_cols[first_part+second_part] = second_part
  return list(tmp_cols.keys())

def read_excel_file(file_path: str) -> pd.DataFrame:
  """
  TODO: Write docstrings.
  """
  df = pd.read_excel(file_path, sheet_name="Sprint-Dataload-Plans", engine="openpyxl", header=[0,1], na_filter=False)
  columns = get_columns(df.columns)
  
  df.columns = df.columns.droplevel(1) 
  new_df = df.iloc[:, :len(columns)]
  new_df.columns = columns
  
  return new_df

def get_result(df: pd.DataFrame, name: str, status: str) -> dict:
  """
  TODO: Write docstring
  """
  required_columns = ['Owner', 'ID_Desc_Ref', 'ENV', 'Country', 'type', 'BS-TS', 'Dataload-request', 
                      'Dataload-Plan-weekday', 'Dataload-Plan-date', 'Dataload-Plan-triggered time','Status',
                      'Services', 'Flipped-required-Required', 'Flipped-required-status', 'Dependencies-Step', 'Dependencies-status']
  df = df[required_columns]

  clean_df = df[(df['Owner'].str.contains(f"(?i){name}")) & (df['Status'].str.lower() == f"{status.lower()}")]
  clean_df = clean_df.drop(['Owner', 'Status'], axis=1)
  clean_df['Dataload-Plan-date'] = clean_df['Dataload-Plan-date'].astype(str)
  clean_df = clean_df.fillna('N/A')
  
  keys = list(clean_df.columns)
  temp_dict = clean_df.T.to_dict('list')
  
  result_dict = {key: dict(zip(keys, value)) for key, value in temp_dict.items()}
  return result_dict

def print_result(result_dict: dict) -> None:
  print("+"*100)
  for key, value in result_dict.items():
    for inner_key, inner_value in value.items():
      if inner_key == "Services":
        print('{:30s} : {:50}'.format(inner_key, inner_value.replace("\n", ", ")))
      else:
        print('{:30s} : {:50}'.format(inner_key, inner_value))
    print("+"*100)
    
if __name__ == '__main__':
 
  file_url = "/sites/Prism-ETL/Shared Documents/<some-folder>/<some-file>.xlsx"
  etl_tracker_path = os.path.join("/dbfs/mnt/<some-blob-path>", os.path.basename(file_url))
  
  print(f"Sharepoint file path : {file_url}")
  get_etl_tracker_file(etl_tracker_path, file_url)
  
  df_etl_file = read_excel_file(etl_tracker_path)
  result = get_result(df_etl_file, getArgument('owner'), getArgument('status'))
  
  print_result(result)
  
  
