import json
import sys

from hops import util
from hops import constants
from hops.exceptions import RestAPIError

BASE_API = "/hopsworks-api/api"
RUN_JOB = ("POST", BASE_API +
           "/project/{project_id}/jobs/{job_name}/executions?action=start")
# Get the latest execution
EXECUTION_STATE = ("GET", BASE_API +
                   "/project/{project_id}/jobs/{job_name}/executions?sort_by=appId:desc&limit=1")


def launch_job(job_name):
    """
    Function for launching a job to Hopsworks.
    :param job_name: Name of the job to be launched in Hopsworks
    :type job_name: str
    """
    headers = {
        constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method, endpoint = RUN_JOB
    endpoint = endpoint.format(project_id=util.project_id(), job_name=job_name)
    return _do_api_call(method, endpoint, headers, "Could not start job")


def get_last_execution_info(job_name):
    """
    Function to get information about the last execution
    :param job_name: Name of the job in Hopsworks
    :type job_name: str
    """
    method, endpoint = EXECUTION_STATE
    endpoint = endpoint.format(project_id=util.project_id(), job_name=job_name)
    return _do_api_call(method, endpoint, error="Could not fetch last execution")


def _do_api_call(method, endpoint, headers=None, error=""):
    response = util.send_request(
        util._get_http_connection(True), method, endpoint, headers=headers)

    resp_body = response.read().decode('utf-8')
    response_object = json.loads(resp_body)

    if sys.version_info > (3, 0) and (response.code // 100) != 2:  # for python 3
        error_code, error_msg, user_msg = util._parse_rest_error(
            response_object)
        raise RestAPIError("{} (url: {}), server response: \n "
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                               error, resource_url, response.code, response.reason, error_code, error_msg, user_msg))
    elif ((response.status // 100) != 2):  # for python 2
        error_code, error_msg, user_msg = util._parse_rest_error(
            response_object)
        raise RestAPIError("{} (url: {}), server response: \n "
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                               error, response.status, response.reason, error_code, error_msg, user_msg))
    return response_object