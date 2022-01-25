import pytz
from datetime import datetime, timedelta

from sendgrid.helpers.mail import Mail
from sendgrid import SendGridAPIClient


def get_total_time(activities: dict) -> str:
  """
  Sum up all the running times of a given run.
  """
  all_times = [activities.get(key).get('{duration}') for key in activities if activities.get(key).get('{duration}')]
  total_time = timedelta()
  for times in all_times:
    (h, m, s) = times.split(':')
    d = timedelta(hours=int(h), minutes=int(m), seconds=int(s))
    total_time += d
  return str(total_time)

def create_html_rows(activity_dicts: dict) -> str:
  """
  Replace the given html row template by the values derived from activity_dicts.
  """
  row_template = '<tr style="font-size:9.0pt;text-align:center"><td width="275" style="text-align:left">{activity_name}</td><td width="375">{activity_run_start}</td><td width="89">{activity_run_end}</td><td width="67">{duration}</td><td width="133" style={background_color}>{activity_status}</td><td width="116" style="text-align:left">{error}</td><td width="189" style="text-align:left"></td></tr>'
  
  status_color = {"Succeeded": "background:#015e3c", "InProgress": "background:#FFFF00", "Failed": "background:#FF0000", '': "background:#FFFFFF",
                 "Cancelled": "background:#FFA500", "Queued": "background:#808080", "Skipped": "background:#FFA500"}

  all_rows = ''
  for key, value in activity_dicts.items():
      temp_template = row_template
      temp_row = temp_template.replace('{activity_name}', key)
      for inner_key, inner_value in value.items():
          temp_row = temp_row.replace('{background_color}', status_color.get(value.get('{activity_status}')))
          temp_row = temp_row.replace(inner_key, inner_value)
      all_rows += temp_row
  return all_rows

def load_table_template(html_template: str, run_id: str, pipeline: str, run_time: str) -> str:
  """
  Load the table template file and make replacements. The values will come from the ADF pipeline.
  Returns the template as a string.
  """
  replace_dict = {'{country}': getArgument("country"), '{env}': getArgument("env"), '{scenario}': getArgument("scenario"), 
                  '{filename}': getArgument("filename"), '{services}': getArgument("services"),'\n': '', '{time_taken}': run_time}
  
  with open(html_template, 'r') as htmlfile:
    template = htmlfile.read()
  
  for key, value in replace_dict.items():
    template = template.replace(key, value)
  return template

def send_email(client_api: str, subject: str, output: str) -> None:
  """
  The recipient is split in case there are more than one recipient. This will return a list.
  """
  recipient = [dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')]
  message = Mail(from_email='noreply@something.com', to_emails=recipient, subject=subject, html_content=output)
  try:
    print("Emailing report.")
    sendgrid_client = SendGridAPIClient(client_api)
    response = sendgrid_client.send(message)
  except Exception as error:
    raise error

def add_extra_keys(main_dict):
  """
  If adding extra rows in the report. Put `yes` in the widget `extra_keys` (of the main notebook)
  Add the values to be added in the keys.
  """
  keys = ['Model Refresh', 'Post Validation']
  empty_values = {'{error}': '', '{activity_status}': '', '{activity_run_start}': '', '{activity_run_end}': '', '{duration}': ''}
  extra_info = {key: empty_values for key in keys}
  return dict(**main_dict, **extra_info)

def get_run_dates():
  """
  last_run is the datetime close to `Run start`.
  today is the datetime upper limit. This date will be greater than last_run.
  """
  last_run = (datetime.now(tz=pytz.timezone('Europe/London'))).replace(hour=14, minute=0, second=0, microsecond=0) - timedelta(1)
  today = datetime.now(tz=pytz.timezone('Europe/London'))
  return last_run, today
