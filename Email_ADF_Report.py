from typing import Union, Tuple, List, Dict
from datetime import datetime, timedelta
from itertools import groupby, islice

from azure.identity import ClientSecretCredential 
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *


%run ./Activities_Utils

def set_widgets():
  dbutils.widgets.removeAll()
  dbutils.widgets.text("pipeline_name", "")
  dbutils.widgets.text("subject", "")
  dbutils.widgets.text("services", "")
  dbutils.widgets.text("filename", "")
  dbutils.widgets.text("env", "")
  dbutils.widgets.text("scenario", "")
  dbutils.widgets.text("country", "")
  dbutils.widgets.text("extra_keys", "")
  
class RunIDNotFoundError(Exception):
  pass

class FunctionNotDefinedError(Exception):
  pass

def get_activities_dict():
  scenario = getArgument("scenario")
  print(scenario)
  if scenario not in globals():
    raise FunctionNotDefinedError(f"Please create function {scenario}() in `Activities_Utils` notebook first.")
    
  func = globals()[scenario]
  return func()


class PipelineActivities:
  """
  Class to extract information from a given pipeline run.
  TODO: write docstrings 
  """
  def __init__(self, resource_group, factory_name, activities_names, pipeline):
    self.annotations = [getArgument('country')]
    self.filename = getArgument('filename')
    self.resource_group = resource_group
    self.factory_name = factory_name
    self.activities_names = activities_names
    self.pipeline = pipeline
    self.tenant_id = dbutils.secrets.get(scope=current_scope, key='tenantid')
    self.client_id = dbutils.secrets.get(scope=current_scope, key='client-id-u')
    self.client_secret = dbutils.secrets.get(scope=current_scope, key='client-secret-u') 
    self.subscription_id = dbutils.secrets.get(scope=current_scope, key='subscription-id')
    self.last_run, self.today = get_run_dates()
    print(f"Last run: {self.last_run}")
    print(f"Today : {self.today}")
    print(f'annotations: {self.annotations}')
    
    credentials = ClientSecretCredential(client_id=self.client_id, client_secret=self.client_secret, tenant_id=self.tenant_id)
    self.adf_client = DataFactoryManagementClient(credentials, self.subscription_id)
    
  def activities_filter_params(self):
    return RunFilterParameters(last_updated_after=self.last_run, last_updated_before=self.today )
    
  def pipeline_filter_params(self):
    return RunFilterParameters(last_updated_after=self.last_run, last_updated_before=self.today,
                                filters=[RunQueryFilter(operand =RunQueryFilterOperand('PipelineName'), 
                                operator=RunQueryFilterOperator('In'), values=[self.pipeline])])
    
  @staticmethod
  def convert_datetime(dt: object) -> str:
    """
    dt = '2021-03-04 09:44:13.621417+00:00'
    Returns '04-03-2021 09:44:13'
    """
    dt_obj = datetime.strptime(str(dt), '%Y-%m-%d %H:%M:%S.%f+00:00') + timedelta(hours=1)
    return dt_obj.strftime('%d-%m-%Y %H:%M:%S')
        
  @staticmethod
  def convert_ms(ms: int) -> str:
    """
    Convert given ms into 'HH:MM:SS' format.
        '00:11:39'
    """
    return datetime.fromtimestamp(ms/1000).strftime('%H:%M:%S')      
    
  def query_activities_for_execute_pipeline(self, run_id, filter_params, activities_names):

    def extract_standard_activities(activity) -> Dict[str, str]:
      return {"{error}": activity.error.get('message'),
              "{activity_name}": activity.activity_name, 
              "{activity_status}": activity.status,
              "{activity_run_start}": self.convert_datetime(activity.activity_run_start),
              "{activity_run_end}": '' if not activity.activity_run_end else self.convert_datetime(activity.activity_run_end),
              "{duration}": '' if not activity.duration_in_ms else self.convert_ms(activity.duration_in_ms)
              }
    activities = self.adf_client.activity_runs.query_by_pipeline_run(self.resource_group, self.factory_name, run_id, filter_parameters=filter_params).value
    queried_activities = [extract_standard_activities(activity) for activity in activities if activity.activity_name in activities_names]
    
    pipeline_run_ids = [activity.output.get("pipelineRunId") for activity in activities if activity.activity_type == "ExecutePipeline"]
    return queried_activities, pipeline_run_ids
    
  def get_activities_from_pipeline_runs(self, run_id: str) -> List[Dict[str, str]]:
    """"
    pipeline_work_list will hold all the pipeline run_ids. It will initially have the run_id of the main pipeline.
    It is later extended to add the pipeline run_ids where the activities are 'ExecutePipeline'.    
    """
    activities_for_pipeline_run = list()
    pipeline_work_list = [run_id]
    while pipeline_work_list:
        current_pipeline_run_id = pipeline_work_list.pop()
        activities, pipeline_run_ids = self.query_activities_for_execute_pipeline(current_pipeline_run_id, 
                                                                                  self.activities_filter_params(), 
                                                                                  self.activities_names)
        activities_for_pipeline_run.extend(activities)
        pipeline_work_list.extend(pipeline_run_ids)

    return activities_for_pipeline_run
    
  def check_if_re_run(self, pipeline_run_id: str, run_dict_id: dict) -> Union[list, str]:
    """
    Here pipeline_run_id is the latest pipeline run id.

    Return a sorted list of tuple [(run_id, run_start)] if part of a re-run.
     [('f24f5a5f-fc6e-4b8f-9b3d-37fcb235c5f4', '2021-03-25T17:05:13.433091+00:00'),
     ('73b021e1-91a8-49be-a9e1-d9a0d1768de1', '2021-03-26T05:54:50.042154+00:00'),
     ('60e66d94-49f9-4a38-8671-eb8d02230ef5', '2021-03-26T07:15:08.223061+00:00'),]
     
    If there are no re-runs, it returns the run_id as str
    """
    for key, group in groupby(run_dict_id, key=lambda e: run_dict_id[e]['run_group_id']):
        group_list = list(group)
        if key != pipeline_run_id and pipeline_run_id in group_list:    
            print(f"pipeline_run_id {pipeline_run_id} is part of a re-run. Returning list of all run ids. The group run id is: {key}")
            run_ids_run_start = [(k, run_dict_id.get(k)['run_start']) for k in group_list]
            return sorted(run_ids_run_start, key=lambda x: x[1])
    return ''.join(group_list)
  
  def process_re_run_pipelines(self, pipeline_id_list: list):
    pipeline_activities = dict()
    for pipe_run_id, _ in pipeline_id_list:
      print(f"Current run_id is: {pipe_run_id}")
      activities_for_run = self.get_activities_from_pipeline_runs(pipe_run_id)
      pipeline_activities[pipe_run_id] = activities_for_run
    return pipeline_activities
  
  def get_pipeline_run_time(self, response: object) -> str:
    """
    This response object contains all pipeline run information. If there are re-runs, for e.g 6 times, then it will loop 
    through each run_id and grab the duration_in_ms.
    """
    runtime_in_ms = sum([run.duration_in_ms for run in response.value if run.additional_properties.get('annotations') == self.annotations 
                                                      and run.parameters.get('fileName') == self.filename and run.duration_in_ms])
    return self.convert_ms(runtime_in_ms)
  
  def pipeline_run(self) -> Tuple[int, str, List[Dict[str, str]]]:
    response_query = self.adf_client.pipeline_runs.query_by_factory(self.resource_group, self.factory_name,
                                                                    filter_parameters=self.pipeline_filter_params())
    pipeline_run_time = self.get_pipeline_run_time(response_query)
    
    run_dict_id = {run.run_id: {'run_group_id':run.run_group_id,'run_start': run.run_start.isoformat()} 
                   for run in response_query.value if run.additional_properties.get('annotations') == self.annotations 
                                                      and run.parameters.get('fileName') == self.filename}
    
    if not bool(run_dict_id):
      raise RunIDNotFoundError("No run id found for this date range. Please check.")
     
    latest_run_id = sorted(run_dict_id, key=lambda e: run_dict_id[e]['run_start'], reverse=True)[0]
    print(f"Latest run id is: {latest_run_id}")
    pipeline_run_ids = self.check_if_re_run(latest_run_id, run_dict_id)
    
    if isinstance(pipeline_run_ids, list):
      activities_for_rerun = self.process_re_run_pipelines(pipeline_run_ids)
      return 1, latest_run_id, activities_for_rerun, pipeline_run_time
    
    activities_for_run =  self.get_activities_from_pipeline_runs(pipeline_run_ids)  # For pipeline run id without re-runs.
    activities_runs = {act_dict.pop('{activity_name}'): act_dict for act_dict in activities_for_run}
    return 0, latest_run_id, activities_runs, pipeline_run_time
 
# If the pipeline is part of a re-run.
def remaining_dict_iter(activities_runs: dict) -> 'iterator object':
  """
  Returns a list iterator containing all activities as dict, APART from the first run.
  To call it use next()
  """
  rem_dict_list = []
  for i in range(1, len(activities_runs)):
    rem_dict_vals = dict(islice(activities_runs.items(), i, i+1)).values()
    rem_dict = {v.pop('{activity_name}'):v for val in rem_dict_vals for v in val}
    rem_dict_list.append(rem_dict)
  return iter(rem_dict_list)

def update_rerun_dict(first_run_dict: dict, next_dict: dict, not_succeeded_keys: list, activities_list: list) -> dict:
  """
  This function is only called for a run id which is part of a rerun.
  param: first_run_dict: All activites which were run the first.
  param: next_dict: The next dictionary generated from the list iterator.
  param: not_succeeded_keys: Activities which have status != 'Succeeded'
  
  For activities where the status is not 'Succeeded', it will recursively look in the next dictionary.
  Once list iterator has been exhausted, it will return the first_run_dict, updated till that point.
  
  The activities_list is here so that we can add the activities which have not started. This is used in the func: add_activities_not_started()
  """
  next_dict = add_activities_not_started(activities_list, next_dict)
  
  no_success_activities = []
  for current_key in not_succeeded_keys:
    first_run_dict.update({current_key: next_dict.get(current_key)})
    
    if next_dict.get(current_key).get('{activity_status}') != 'Succeeded':
      no_success_activities.append(current_key)

  if no_success_activities:
    try:
      next_dict = next(dict_iter)
      update_rerun_dict(first_run_dict, next_dict, no_success_activities, activities_list)
    except StopIteration:
      print("All dict have been parsed.")
      return first_run_dict
  return first_run_dict

def add_activities_not_started(activities_list: list, activities_runs_dict: dict) -> Dict[str, str]:
  """
  If any activity has not started then it is not returned in the activities_run dict. 
  So create an empty entry into the final dict so that it is shown in the report.
  
  If none of the activities that we want to monitor have started, then activities_runs_dict will be empty.
  In this case, set all the values to empty_values for the given activity name.
  
  E:g:
    {'Switch_SourceDrop': {'{error}': '',
                           '{activity_status}': '',
                           '{activity_run_start}': '',
                           '{activity_run_end}': '',
                           '{duration}': ''}
  """
  empty_values = {'{error}': '', '{activity_status}': '', '{activity_run_start}': '', '{activity_run_end}': '', '{duration}': ''}
  if not activities_runs_dict:
    return {name:empty_values for name in activities_list}
    
  not_run_activities = [name for name in activities_list if name not in list(activities_runs_dict.keys())]
  not_run_activities_dict = {activity:empty_values for activity in not_run_activities}
  return dict(**activities_runs_dict, **not_run_activities_dict)

def rename_keys(data_dict: dict, keys_dict: dict) -> dict:
  """
  Rename the output activities dict with the names from the activities_dict.
  Also sort it according to how it is in activities_dict.
  """
  index_map = {val: indx for indx, val in enumerate(list(keys_dict.keys()))}
  sorted_dict = {key:value for key, value in sorted(data_dict.items(), key=lambda indx: index_map[indx[0]])}
  renamed = {keys_dict.get(key):val for key, val in sorted_dict.items()}
  return renamed

if __name__ == '__main__':
  set_widgets()
  
  current_scope = getArgument("secret_scope")
  pipeline_name, activities_dict = get_activities_dict()
  activities_list = list(activities_dict.keys())
  resource_group, factory_name = getArgument('resource_group'), getArgument('factory_name')
  
  pipeline_run_activities = PipelineActivities(resource_group=resource_group, factory_name=factory_name, 
                                               activities_names=activities_list, pipeline=pipeline_name)
 
  is_rerun, run_id, activities_runs, runtime = pipeline_run_activities.pipeline_run() 
  if is_rerun:
    first_run_vals = dict(islice(activities_runs.items(), 1)).values()
    first_run_dict = {v.pop('{activity_name}'): v for values in first_run_vals for v in values}  # This is the very first run.
    activities_runs_dict_first = add_activities_not_started(activities_list, first_run_dict)
    not_succeeded_keys = [key for key, value in activities_runs_dict_first.items() if value.get('{activity_status}') != 'Succeeded']
    
    dict_iter = remaining_dict_iter(activities_runs)
    remaining_dict = next(dict_iter)
    activities_runs_dict = update_rerun_dict(first_run_dict, remaining_dict, not_succeeded_keys, activities_list)
  else:
    activities_runs_dict = add_activities_not_started(activities_list, activities_runs)
  
  activities_runs_final = rename_keys(activities_runs_dict, activities_dict)
  
  table_template = load_table_template(".../adf_table_template.html", run_id, pipeline_name, runtime)
  final_output = table_template.replace('{html_rows}', create_html_rows(activities_runs_final))
  
  dbutils.notebook.exit({'result': final_output})
  
