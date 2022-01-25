def check_modules_exist():
  import importlib.util

  for module in ['azure.identity', 'azure.mgmt.datafactory', 'azure.mgmt.resource']:
    if not importlib.util.find_spec(module):
      print(f"Installing module : {module}")
      dbutils.library.installPyPI(module)
      
  dbutils.library.restartPython()
  
check_modules_exist()
