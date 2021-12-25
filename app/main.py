import os
from typing import Dict, List, Optional
from fastapi import FastAPI, HTTPException, APIRouter, Request
from pydantic import BaseModel, validator
import requests
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

load_dotenv()

# database credentials
DATABASE_NAME = "Default" # os.getenv("DATABASE_NAME")
DATABASE_HOST_READ = "scheduler-mysql-read" # os.getenv("DATABASE_HOST_READ")
DATABASE_HOST_WRITE =  "scheduler-mysql-0.scheduler-headless" # os.getenv("DATABASE_HOST_WRITE")
DATABASE_USER = "root" # os.getenv("DATABASE_USER")
DATABASE_PASSWORD = '' # os.getenv("DATABASE_PASSWORD")

# service ips
MONITOR_SERVICE_IP = "monitor-app" # os.getenv("MONITOR_SERVICE_IP")
QUOTA_SERVICE_IP = "quotas-app" # os.getenv("QUOTA_SERVICE_IP")
MZN_SERVICE_IP = "mzn-dispatcher-service" # os.getenv("MZN_SERVICE_IP")
MZN_DATA_SERVICE_IP = "minizinc-app" # os.getenv("MZN_DATA_SERVICE_IP")
SOLVERS_SERVICE_IP = "solvers-app-service" # os.getenv("SOLVERS_SERVICE_IP")

headers = { 'UserId': 'system', 'Role': 'admin', 'Content-Type': 'application/json'}

# All data that needs to be added to the database
class ScheduleComputationRequest(BaseModel):
    solver_ids: List[int]
    mzn_file_id: str #The URL to that point to where the minizin model file is stored. 
    dzn_file_id: Optional[str] #The URL that points to where the minizin data fil is stored.
    vcpus: int #The amount of Vcpu resources that this computation should have
    memory: int #The amout of memory resources that this computation should have
    user_id: str # don't know the format that the guid is stored in.
    timeout_seconds: int # how long the computation should run at most before terminating
    solver_options: Optional[str]

    @validator('vcpus')
    def check_vcpu_more_than_one(cls, v):
        if (v < 1):
            raise ValueError ("vcpus can't be less than 1")
        return v

    @validator('memory')
    def check_memory_more_than_one(cls, v):
        if (v < 1):
            raise ValueError ("memory can't be less than 1")
        return v

# Same as ScheduleComputationRequest but with the autoincremented id
class ScheduledComputationResponse(BaseModel):
    id: int
    solver_ids: List[int]
    mzn_file_id: str #The file id used to create dzn file url 
    dzn_file_id: Optional[str] #The file id used to create mzn file url 
    vcpus: int #The amount of Vcpu resources that this computation should have
    memory: int #The amout of memory resources that this computation should have
    user_id: str # don't know the format that the guid is stored in.
    timeout_seconds: int
    solver_options: Optional[str]

# model for launching a computation. mzn attributes are now urls, not ids
class LaunchComputationResponse(BaseModel):
    solver_ids: List[int]
    mzn_file_url: str #The URL to that point to where the minizin model file is stored. 
    dzn_file_url: Optional[str] #The URL that points to where the minizin data fil is stored.
    vcpus: int #The amount of Vcpu resources that this computation should have
    memory: int #The amout of memory resources that this computation should have
    user_id: str # don't know the format that the guid is stored in.
    timeout_seconds: int
    solver_options: Optional[str]

# Data needed from solverexecution when it notifies about finishing a computaiton
class FinishComputationMessage(BaseModel):
    user_id: str
    computation_id: str

class Solver(BaseModel):
    image: str
    cpu_request: int
    mem_request: int

app = FastAPI()
router = APIRouter()

@router.post("/api/scheduler/computation", tags=["Scheduler"]) 
@router.post("/api/scheduler/computation/", include_in_schema=False)
def create_computation(request_body: ScheduleComputationRequest, http_req: Request):
    """Call the dispatcher service to start a computation, provided that the user has enough available resources. 
    Otherwise, schedule it if the user has enough resources in its quota.
    """
    #Both admin and user has access to this endpoint. But it needs to be to a specific user. 
    userId = http_req.headers.get("UserId")
    role = http_req.headers.get("Role")

    if(userId != request_body.user_id and role != "admin"):
        raise HTTPException(status_code=401)

    # check if the computation request is ever runnable with the user's quota
    user_quota = get_user_quota(request_body.user_id)
    limit_vcpu = user_quota.get("vCpu")
    limit_memory = user_quota.get("memory")

    if (len(request_body.solver_ids) > limit_vcpu):
        raise HTTPException(status_code=403, detail="""The requested computation can never be launched, 
                                                    because the requested amount of parallel solvers (%s) 
                                                    exceeds the user's vCPU quota (%s)""" 
                                                    % (len(request_body.solver_ids), limit_vcpu))
    if (request_body.vcpus > limit_vcpu):
        raise HTTPException(status_code=403, detail="""The requested computation can never be launched, 
                                                    because the requested amount of vCPUs (%s) 
                                                    exceeds the user's vCPU quota (%s)""" 
                                                    % (request_body.vcpus, limit_vcpu))
    if (request_body.memory > limit_memory):
        raise HTTPException(status_code=403, detail="""The requested computation can never be launched, 
                                                    because the requested amount of memory (%s) 
                                                    exceeds the user's memory quota (%s)""" 
                                                    % (request_body.memory, limit_memory))

    # create a computation object with both mzn/dzn urls and the request data
    computation = ScheduleComputationRequest(solver_ids = request_body.solver_ids, 
                            mzn_file_id = request_body.mzn_file_id, 
                            dzn_file_id = request_body.dzn_file_id, 
                            user_id = request_body.user_id, 
                            memory = request_body.memory, 
                            vcpus = request_body.vcpus,
                            timeout_seconds = request_body.timeout_seconds,
                            solver_options = request_body.solver_options)

    scheduled_computations: List = get_all_user_scheduled_computations(computation.user_id)

    if (len(scheduled_computations) == 0 and user_resources_are_available(computation.user_id, computation.vcpus, computation.memory)):
        computation_id = launch_computation(computation)
        return "Computation has been launched immediately with computation id: %s" % computation_id
    else:
        schedule_computation(computation)
        return "Computation has been scheduled for launch"

@router.delete("/api/scheduler/computation/{scheduled_computation_id}", tags=["Scheduler"])
@router.delete("/api/scheduler/computation/{scheduled_computation_id}/", include_in_schema=False) 
def delete_computation(scheduled_computation_id, http_req: Request):
    """Remove a computaiton from the queue
    """
    scheduled_computation = load_scheduled_computation(scheduled_computation_id)
    userId = http_req.headers.get("UserId")
    role = http_req.headers.get("Role")

    if (scheduled_computation == None):
        return "There is no scheduled computation with id %s" % (scheduled_computation_id)

    if(userId != scheduled_computation.user_id and role != "admin"):
        raise HTTPException(status_code=401)

    delete_scheduled_computation(scheduled_computation_id)

    return "Scheduled computation '%s' has been unscheduled" % scheduled_computation_id

@router.get("/api/scheduler/computations/{user_id}", response_model=List[ScheduledComputationResponse], tags=["Scheduler"])
@router.get("/api/scheduler/computations/{user_id}/", response_model=List[ScheduledComputationResponse], include_in_schema=False) 
def list_user_computations(user_id: str, http_req: Request):
    """List all computations that a user has scheduled (in the queue)
    """
    userId = http_req.headers.get("UserId")
    role = http_req.headers.get("Role")

    if(userId != user_id and role != "admin"):
        raise HTTPException(status_code=401)

    scheduled_computations = get_all_user_scheduled_computations(user_id)
    return scheduled_computations

@router.delete("/api/scheduler/computations/{user_id}", tags=["Scheduler"])
@router.delete("/api/scheduler/computations/{user_id}/", include_in_schema=False) 
def delete_scheduled_computation(user_id: str, http_req: Request):
    """Remove all computations scheduled by a specific user.
    """
    userId = http_req.headers.get("UserId")
    role = http_req.headers.get("Role")

    if(userId != user_id and role != "admin"):
        raise HTTPException(status_code=401)

    scheduled_computations = get_all_user_scheduled_computations(user_id)
    len(scheduled_computations)

    for scheduled_computation in scheduled_computations:
        scheduled_computation_id = scheduled_computation.id
        delete_scheduled_computation(scheduled_computation_id)

    return "Deleted all (%s) scheduled computations associated with user %s" % (len(scheduled_computations), user_id)

@router.post("/api/scheduler/finish_computation", tags=["Scheduler"])
@router.post("/api/scheduler/finish_computation/", include_in_schema=False)
def finish_computation(request_body: FinishComputationMessage, http_req: Request):
    """Clean up after a computation is done executing. Reassigns resources and launches a queued computation. 
    (Called by the dispatcher)
    """
    role = http_req.headers.get("Role")

    if(role != "admin"):
        raise HTTPException(status_code=401)

    monitor_request_url = "http://%s/api/monitor/process/%s" % (MONITOR_SERVICE_IP, request_body.computation_id)

    monitor_response = requests.delete(monitor_request_url, headers=headers)     
    
    if(monitor_response.status_code > 210):
        response_body = monitor_response.json()
        print(response_body)
        error_dict = {
        "error": "Error on DELETE request to monitor service", 
        "monitor_error_message": response_body.get("detail"), 
        "monitor_request_url": monitor_request_url
        }
        raise HTTPException(status_code=monitor_response.status_code, detail=error_dict)

    return launch_scheduled_computation(request_body.user_id)

@router.delete("/api/scheduler/computation/running/{computation_id}", tags=["Scheduler"])
@router.delete("/api/scheduler/computation/running/{computation_id}/", include_in_schema=False) 
def delete_running_computation(computation_id: str, http_req: Request):
    """Terminate a currently running computation
    """
    # get user_id for computation
    monitor_get_request_url = "http://%s/api/monitor/process/%s" % (MONITOR_SERVICE_IP, computation_id)
    monitor_get_response = requests.get(monitor_get_request_url, headers=headers)

    if (monitor_get_response.status_code == 404):
        return "Computation already terminated" 
    
    elif (monitor_get_response.status_code > 210):
        monitor_get_body = monitor_get_response.json()
        print(monitor_get_body)
        error_dict = {
        "error": "Error on GET request to monitor service", 
        "monitor_error_message": monitor_get_body.get("detail"), 
        "monitor_request_url": monitor_get_request_url
        }
        raise HTTPException(status_code=monitor_get_response.status_code, detail=error_dict)
    monitor_get_body = monitor_get_response.json()

    userId = http_req.headers.get("UserId")
    role = http_req.headers.get("Role")

    # auth
    if(userId != monitor_get_body.get("user_id") and role != "admin"):
        raise HTTPException(status_code=401)

    # stop computation
    mzn_request_url = "http://%s:8080/delete/%s" % (MZN_SERVICE_IP, computation_id)
    solver_execution_response = requests.post(mzn_request_url, headers=headers)     

    if(solver_execution_response.status_code > 210):
        response_body = solver_execution_response.json()
        print(response_body)
        error_dict = {
        "error": "Error on DELETE request to mzn_dispatcher service", 
        "mzn_dispatcher_error_message": response_body.get("detail"), 
        "mzn_dispatcher_request_url": mzn_request_url
        }
        raise HTTPException(status_code=solver_execution_response.status_code, detail=error_dict)

    return "The running computation with id %s has been stopped"  % (computation_id)

app.include_router(router)

# Checks to see if the resources that a user asks for is available
def user_resources_are_available(user_id, vcpu_requested, memory_requested) -> bool:
    # initialize values to allow increments later
    current_vcpu_usage = 0
    current_memory_usage = 0
    
    #Gets the limit resources for a user, by call the GetQuotasEndPoint
    getQuotaResult = get_user_quota(user_id)
    limit_vcpu = getQuotaResult.get("vCpu")
    limit_memory = getQuotaResult.get("memory")

    # A list of monitored processes
    getMonitorForUserResult = get_user_monitor_processes(user_id)

    #Calculates the current_vcpu_usage and currrent_memory_usage
    if len(getMonitorForUserResult) > 0:
        for x in getMonitorForUserResult:
            current_vcpu_usage += x.get('vcpu_usage')
            current_memory_usage += x.get('memory_usage')
    
    available_vcpu = limit_vcpu - current_vcpu_usage
    available_memory = limit_memory - current_memory_usage

    if (available_vcpu >= vcpu_requested) and (available_memory >= memory_requested):
        return True
    else:
        return False

def launch_scheduled_computation(user_id):
    """Find oldest scheduled computation and launch it after deleting it from the "queue".

    Args:
        user_id (str): the id of the user
    """
    scheduled_computations = get_all_user_scheduled_computations(user_id)

    if (len(scheduled_computations) == 0):
        return "User %s has not scheduled computations" % user_id

    oldest_scheduled_computation = min(scheduled_computations, key=lambda x: x.id)

    delete_scheduled_computation(oldest_scheduled_computation.id)
    return launch_computation(oldest_scheduled_computation)

def launch_computation(computation: ScheduleComputationRequest):
    """Contact solver execution service and launch an actual execution / computation

    Args:
        computation (Computation): All the info the solver execution service needs

    Returns:
        [type]: [description]
    """

    # get urls by file_id
    mzn_request_url = get_mzn_url(computation.user_id, computation.mzn_file_id)

    dzn_url = None
    if (computation.dzn_file_id != None):
        dzn_url = get_mzn_url(computation.user_id, computation.dzn_file_id)

    # construct solver objects (calculate resource fractions and get solver image from solver image id)
    solvers = []
    for solver_id in computation.solver_ids:
        solver_image = get_solver_image(solver_id)
        vcpu_fraction = int(computation.vcpus / len(computation.solver_ids))
        memory_fraction = int(computation.memory / len(computation.solver_ids))
        solver = Solver(image = solver_image, cpu_request = vcpu_fraction, mem_request = memory_fraction)
        solver_json = solver.dict()
        solvers.append(solver_json)

    # Start minizinc solver: 
    solver_execution_request = {'user_id': computation.user_id, 'model_url': mzn_request_url, 'solvers': solvers, 'timeout_seconds': computation.timeout_seconds }
    if (dzn_url != None):  
        solver_execution_request["data_url"] = dzn_url
    if (computation.solver_options != None):
        solver_execution_request["solver_options"] = computation.solver_options

    mzn_request_url = "http://%s:8080/run" % (MZN_SERVICE_IP)

    solver_execution_response = requests.post(mzn_request_url, json = solver_execution_request, headers=headers)     
    
    if(solver_execution_response.status_code > 210):
        response_body = solver_execution_response.json()
        print(response_body)
        error_dict = {
        "error": "Error on POST request to mzn_dispatcher service", 
        "mzn_dispatcher_error_message": response_body.get("detail"), 
        "mzn_dispatcher_request_url": mzn_request_url
        }
        raise HTTPException(status_code=solver_execution_response.status_code, detail=error_dict)

    solver_execution_response_body = solver_execution_response.json()
    computation_id = solver_execution_response_body.get("computation_id")

    # Post the computation to the monitor Service: 
    monitor_request_url = "http://%s/api/monitor/process/" % (MONITOR_SERVICE_IP)
    monitor_request = {'user_id': computation.user_id, 'computation_id': computation_id, 'vcpu_usage': computation.vcpus, 'memory_usage': computation.memory}
    monitor_response = requests.post(monitor_request_url, json = monitor_request, headers=headers)

    if(monitor_response.status_code > 210):
        response_body = monitor_response.json()
        print(response_body)
        error_dict = {
        "error": "Error on POST request to monitor service", 
        "monitor_error_message": response_body.get("detail"), 
        "monitor_request_url": monitor_request_url
        }
        raise HTTPException(status_code=monitor_response.status_code, detail=error_dict)
    

    return {"computation_id": computation_id}
    

def schedule_computation(computation: ScheduleComputationRequest):
    """Adds computation to queue

    Args:
        computation (Computation): The computation to be scheduled
    """
    scheduledcomputation_prepared_sql: str = "INSERT INTO scheduledcomputation (user_id, memory_usage, vcpu_usage, mzn_file_id, dzn_file_id, timeout_seconds, solver_options) values (%s, %s, %s, %s, %s, %s, %s)"
    scheduledcomputation_values = (computation.user_id, computation.memory, computation.vcpus, computation.mzn_file_id, computation.dzn_file_id, computation.timeout_seconds, computation.solver_options)

    # write data to scheduledcomputation table and return the auto incremented id 
    inserted_row_scheduledcomputation_id = writeDB(scheduledcomputation_prepared_sql, scheduledcomputation_values)

    # write all the solver_ids to the scheduledcomputation_solver table
    scheduledcomputation_solver_prepared_sql: str = "INSERT INTO scheduledcomputation_solver (scheduledcomputation_id, solver_id) values (%s, %s)"
    for solver_id in computation.solver_ids:
        writeDB(scheduledcomputation_solver_prepared_sql, (inserted_row_scheduledcomputation_id, solver_id))

def load_scheduled_computation(scheduledcomputation_id: int) -> ScheduledComputationResponse:
    # get all solver ids and save in a list
    scheduledcomputation_solver_prepared_sql: str = "SELECT solver_id FROM scheduledcomputation_solver WHERE scheduledcomputation_id = %s" 
    solver_id_tuples = readDB(scheduledcomputation_solver_prepared_sql, (scheduledcomputation_id,))

    if (len(solver_id_tuples) == 0):
        return None

    solver_ids = [id_tuple[0] for id_tuple in solver_id_tuples]

    # get the rest of the data and save it in an object along with solver ids
    scheduledcomputation_prepared_sql: str = "SELECT id, user_id, memory_usage, vcpu_usage, mzn_file_id, dzn_file_id, timeout_seconds, solver_options FROM scheduledcomputation WHERE id = %s"
    query_result = readDB(scheduledcomputation_prepared_sql, (scheduledcomputation_id,))
    if (len(query_result) == 0):
        return None

    scheduled_computation = query_result[0]
    scheduled_computation = ScheduledComputationResponse(id = scheduled_computation[0], 
                                    user_id = scheduled_computation[1], 
                                    memory = scheduled_computation[2], 
                                    vcpus = scheduled_computation[3],
                                    mzn_file_id = scheduled_computation[4],
                                    dzn_file_id = scheduled_computation[5],
                                    timeout_seconds = scheduled_computation[6],
                                    solver_options = scheduled_computation[7],
                                    solver_ids=solver_ids)

    return scheduled_computation

def delete_scheduled_computation(scheduled_computation_id: int):
    scheduledcomputation_prepared_sql: str = "DELETE FROM scheduledcomputation WHERE id = %s"
    writeDB(scheduledcomputation_prepared_sql, (scheduled_computation_id,))

    scheduledcomputation_solver_prepared_sql: str = "DELETE FROM scheduledcomputation_solver WHERE scheduledcomputation_id = %s"
    writeDB(scheduledcomputation_solver_prepared_sql, (scheduled_computation_id,))

def get_all_user_scheduled_computations(user_id: str) -> List[ScheduledComputationResponse]:
    scheduledcomputation_prepared_sql: str = "SELECT id FROM scheduledcomputation WHERE user_id = %s"
    scheduledcomputation_values = (user_id,)

    scheduledcomputation_id_tuples: List[tuple] = readDB(scheduledcomputation_prepared_sql, scheduledcomputation_values)
    scheduledcomputation_ids = [id_tuple[0] for id_tuple in scheduledcomputation_id_tuples] # map list of tuples to list of ints

    scheduled_computations = []
    for scheduledcomputation_id in scheduledcomputation_ids:
        scheduled_computations.append(load_scheduled_computation(scheduledcomputation_id))

    return scheduled_computations

def get_user_quota(user_id: str) -> Dict[int, int]:
    url = "http://%s/quota/%s" % (QUOTA_SERVICE_IP, user_id)
    response = requests.get(url=url, headers=headers)
    
    if (response.status_code > 210):
        response_body = response.json()
        print(response_body)
        error_dict = {
        "error": "Error on GET request to quota service", 
        "quota_error_message": response_body.get("detail"), 
        "quota_request": url
        }
        raise HTTPException(status_code=response.status_code, detail=error_dict)

    quota = response.json()

    return quota

def get_mzn_url(user_id, file_id):
    url = "http://%s/api/minizinc/%s/%s" % (MZN_DATA_SERVICE_IP, user_id, file_id)
    response = requests.get(url, headers=headers)

    if (response.status_code > 210):
        response_body = response.json()
        print(response_body)
        error_dict = {
        "error": "Error on GET request to mzn_data service", 
        "mzn_data_error_message": response_body.get("detail"), 
        "mzn_data_request": url
        }
        raise HTTPException(status_code=response.status_code, detail=error_dict)

    url = response.json()

    return url

def get_solver_image(solver_id):
    url = "http://%s/api/solvers/%s" % (SOLVERS_SERVICE_IP, solver_id)
    response = requests.get("http://%s/api/solvers/%s" % (SOLVERS_SERVICE_IP, solver_id), headers=headers)

    if (response.status_code > 210):
        response_body = response.json()
        print(response_body)
        error_dict = {
        "error": "Error on GET request to solvers service", 
        "solvers_error_message": response_body.get("detail"), 
        "solvers_request": url
        }
        raise HTTPException(status_code=response.status_code, detail=error_dict)

    solver = response.json()  

    solver_image = solver.get("image")
    return solver_image

def get_user_monitor_processes(user_id: str):
    url = "http://%s/api/monitor/processes/%s" % (MONITOR_SERVICE_IP, user_id)
    response = requests.get(url, headers=headers)
    if (response.status_code > 210):
        response_body = response.json()
        print(response_body)
        error_dict = {
        "error": "Error on GET request to monitor service", 
        "monitor_error_message": response_body.get("detail"), 
        "monitor_request": url
        }
        raise HTTPException(status_code=response.status_code, detail=error_dict)

    user_processes = response.json()
    
    return user_processes

def writeDB(sql_prepared_statement: str, sql_placeholder_values: tuple = ()):
    """Takes a prepared statement with values and writes to database

    Args:
        sql_prepared_statement (str): an sql statement with (optional) placeholder values
        sql_placeholder_values (tuple, optional): The values for the prepared statement. Defaults to ().
    """
    connection = mysql.connector.connect(database=DATABASE_NAME,
                                         host=DATABASE_HOST_WRITE,
                                         user=DATABASE_USER,
                                         password=DATABASE_PASSWORD
                                         )

    lastrowid = 0
    try:
        if (connection.is_connected()):
            cursor = connection.cursor(prepared=True)
            cursor.execute(sql_prepared_statement, sql_placeholder_values)
            connection.commit()
            lastrowid = cursor.lastrowid
    except Error as e:
        raise HTTPException(
            status_code=500, detail="Error while contacting database. " + str(e))
    finally:
        cursor.close()
        connection.close()

    return lastrowid

def readDB(sql_prepared_statement: str, sql_placeholder_values: tuple = ()):
    """Takes a prepared statement with values and makes a query to the database

    Args:
        sql_prepared_statement (str): an sql statement with (optional) placeholder values
        sql_placeholder_values (tuple, optional): The values for the prepared statement. Defaults to ().

    Returns:
        List(tuple): The fetched result
    """
    connection = mysql.connector.connect(database=DATABASE_NAME,
                                         host=DATABASE_HOST_READ,
                                         user=DATABASE_USER,
                                         password=DATABASE_PASSWORD
                                         )
    try:
        if (connection.is_connected()):
            cursor = connection.cursor(prepared=True)
            cursor.execute(sql_prepared_statement, sql_placeholder_values)
            result = cursor.fetchall()
            return result
    except Error as e:
        raise HTTPException(
            status_code=500, detail="Error while contacting database. " + str(e))
    finally:
        cursor.close()
        connection.close()
