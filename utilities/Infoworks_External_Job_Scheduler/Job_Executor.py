import json
import argparse
import os
import logging
import sys
import traceback
import warnings
import datetime
import configparser

from infoworks.sdk.utils import IWUtils
from infoworks.sdk.client import InfoworksClientSDK
import infoworks.sdk.local_configurations

warnings.filterwarnings('ignore', '.*Unverified HTTPS request.*', )
warnings.filterwarnings("ignore")


def configure_logger(folder, filename):
    log_file = "{}/{}".format(folder, filename)
    if os.path.exists(log_file):
        os.remove(log_file)

    file_console_logger = logging.getLogger('TWSIwxTrigger')
    file_console_logger.setLevel(log_level)

    formatter = logging.Formatter(
        "[%(asctime)s - %(levelname)s - %(filename)25s:%(lineno)s - %(funcName)20s() ] %(message)s")

    log_handler = logging.FileHandler(filename=log_file)
    log_handler.setLevel(log_level)
    log_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s — [ %(levelname)s ] - %(message)s')
    console_handler.setFormatter(formatter)

    file_console_logger.addHandler(log_handler)
    file_console_logger.addHandler(console_handler)

    print("Check the log file {} for detailed logs".format(log_file))
    return file_console_logger


def create_dir(dir_name):
    try:
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
    except OSError as dir_creation_error:
        raise Exception(f"LogDirCreationException: Failed to Create Log Directory {dir_name}"
                        f" - Error: {dir_creation_error}")


class IwxException(Exception):
    def __init__(self, message, iwx_response):
        self.message = message
        self.error_desc = iwx_response['error']['error_desc']
        self.code = iwx_response['result']['response'].get("iw_code")
        self.details = iwx_response['result']['response'].get("details")
        self.help_url = iwx_response['result']['response'].get('help')

        self.final_error = "{message}\nInfoworks Code: {iwx_code},\n" \
                           "Error Description: {error_desc} , Details: {details}, Help: {help}" \
            .format(message=self.message, iwx_code=self.code, error_desc=self.error_desc, details=self.details,
                    help=self.help_url)

    def __str__(self):
        return self.final_error


def get_workflow_run_job_summary(domain_id, workflow_run_id):
    logger.info("Fetching Summary for Workflow Run ID {}\n".format(workflow_run_id))
    workflow_run_jobs_response = iwx_client.get_list_of_workflow_runs_jobs(run_id=workflow_run_id,
                                                                           params={'filter': {"status": "failed"}})
    try:

        if workflow_run_jobs_response['result']['status'] == 'success':

            logger.debug("get_list_of_workflow_runs_jobs() - Response: {}"
                         .format(json.dumps(workflow_run_jobs_response)))
            workflow_run_jobs_result = workflow_run_jobs_response['result']['response'].get('result')

            if workflow_run_jobs_result:
                # Result can contain multiple failed job ids
                for job in workflow_run_jobs_result:
                    job_id = job.get('id')
                    job_type = job.get('type')
                    job_summary = job.get('summary', [])
                    job_cluster_id = job.get('cluster_id')

                    if job_type == "pipeline_group_build":
                        pipeline_id = job.get('entity_id')
                        if pipeline_id:
                            get_pipeline_group_pipelines(domain_id, pipeline_id, job_id)
                    if job_summary:
                        job_final_summary = ''

                        for row in job_summary:
                            job_final_summary = job_final_summary + '{} - Job ID: {} - {} \n' \
                                .format(row['timestamp'], row['job_id'], row['message'])

                        logger.info("**** Job Summary for Job ID {} **** \n{}".format(job_id, job_final_summary))
                    else:
                        logger.info("No Job Summary for Job ID {} Found\n".format(job_id))

                    if job_id is not None:
                        get_logfile_path(domain_id, job_id)
            else:
                logger.info("No Summary for Workflow Run ID {} Found".format(workflow_run_id))

        else:

            raise IwxException("JobSummaryFetchException", workflow_run_jobs_response)

    except Exception as job_summary_error:
        logger.exception("Failed to fetch job summary: {}".format(job_summary_error))


def get_logfile_path(domain_id, job_id):
    logger.info("Fetching Log File Path Info for Job Id {}\n".format(job_id))
    job_details_response = iwx_client.get_cluster_job_details(job_id=job_id,
                                                              params={'filter': {'application_state': "FAILED"}})
    logger.debug("get_cluster_job_details() - Response: {}".format(json.dumps(job_details_response)))
    try:

        if job_details_response['result']['status'] == 'success':
            job_details_result = job_details_response['result']['response'].get('result')
            logger.info("**** Failed Job Id URL to Infoworks UI *****")
            logger.info("{protocol}://{host}:{port}/job/logs?jobId={jobID}"
                        .format(protocol=protocol, host=host, port=port, jobID=job_id))

            if job_details_result:
                for job in job_details_result:
                    cluster_job_log_path = job.get('cluster_job_log_path', '').replace("////", "/")
                    cluster_id = job.get('cluster_id')
                    entity_name = job.get('entity_name')
                    sub_entity_type = job.get('sub_entity_type')
                    entity_type = job.get('entity_type')
                    entity_id = job.get('entity_id')

                    logger.info("***** Log File Path Information for Job Id {} *****".format(job_id))
                    logger.info("Failed Job Log path for {sub_entity_type}: {entity_name} :"
                                " {path}/application_log/job_{jobID}.log"
                                .format(path=cluster_job_log_path, entity_name=entity_name,
                                        sub_entity_type=sub_entity_type, jobID=job_id))
                    environment_id = None
                    if entity_type == 'source':
                        environment_id = get_source_info(entity_id)
                    elif entity_type == 'pipeline':
                        environment_id = get_pipeline_info(domain_id, entity_id)

                    if environment_id is not None and cluster_id is not None:
                        workspace_url = get_databricks_workspace_url(environment_id)
                        if workspace_url:
                            logger.info("Workspace URL {}".format(workspace_url))
                            workspace_id = workspace_url.split('-')[1].split('.')[0]
                            DB_workspace_url = '{workspace_url}?o={workspace_id}#setting/clusters/' \
                                               '{cluster_id}/driverLogs'.format(workspace_id=workspace_id,
                                                                                workspace_url=workspace_url,
                                                                                cluster_id=cluster_id)
                            logger.info(
                                "Databricks Workspace URL for Job ID {} is : {}".format(job_id, DB_workspace_url))
                    else:
                        logger.info("Environment/Cluster ID is None")

            else:
                logger.info("No Cluster Job Path & Databricks Workspace URL found")

        else:
            raise IwxException("JobLogPathFetchException", job_details_response)

    except Exception as log_filepath_error:
        logger.exception("Failed to fetch log file path information: {}".format(log_filepath_error))


def trigger_workflow(domain_id, workflow_id):
    trigger_workflow_response = iwx_client.trigger_workflow(workflow_id=workflow_id, domain_id=domain_id)
    logger.debug("trigger_workflow() - Response: {}".format(json.dumps(trigger_workflow_response)))
    try:
        if trigger_workflow_response['result']['status'] == 'success':
            run_id = trigger_workflow_response['result']['response']['result']['id']
            return run_id
        else:
            raise IwxException("WorkflowTriggerException", trigger_workflow_response)

    except Exception as trigger_workflow_error:
        raise Exception(f"Failed to Trigger Workflow - Error: {trigger_workflow_error}")


def get_source_info(source_id):
    source_response = iwx_client.get_source_configurations(source_id=source_id)
    logger.debug("get_source_configurations() - Response: {}".format(json.dumps(source_response)))
    try:
        # Recheck this again
        if source_response['result']['status'] == 'success':
            environment_id = source_response['result']['response'].get('result', {}).get('environment_id')
            return environment_id
        else:
            raise IwxException("SourceInfoFetchException", source_response)
    except Exception as source_response_error:
        raise Exception(f"Failed to get source information - Error: {source_response_error}")


def get_pipeline_info(domain_id, pipeline_id):
    pipeline_response = iwx_client.get_pipeline(domain_id=domain_id, pipeline_id=pipeline_id)
    logger.debug("get_pipeline() - Response: {}".format(json.dumps(pipeline_response)))
    try:

        if pipeline_response['result']['status'] == 'success':
            environment_id = pipeline_response['result']['response'].get('result', {}).get('environment_id')
            return environment_id
        else:
            raise IwxException("PipelineInfoFetchException", pipeline_response)

    except Exception as pipeline_response_error:
        raise Exception(f"Failed to get pipeline information - Error: {pipeline_response_error}")


def get_databricks_workspace_url(environment_id):
    logger.info("Fetching Environment Information")
    environment_response = iwx_client.get_environment_details(environment_id=environment_id)
    logger.debug("get_environment_details() - Response: {}".format(json.dumps(environment_response)))
    try:
        if environment_response['result']['status'] == 'success':
            workspace_url = environment_response['result']['response']['result'][0].get('connection_configuration',
                                                                                        {}).get(
                'workspace_url')
            if workspace_url:
                return workspace_url
            else:
                logger.error("Workspace URL not found")
        else:
            raise IwxException("EnvironmentInfoFetchException", environment_response)
    except Exception as environment_response_error:
        logger.exception(f"Failed to get Environment information - Error: {environment_response_error}")


def initiate_workflow_process(domain_id, workflow_id):
    try:
        logger.info('Workflow Trigger and Monitoring Process')

        workflow_run_id = trigger_workflow(domain_id, workflow_id)
        logger.info('Workflow Run ID for the current run: {}'.format(workflow_run_id))

        IWX_wf_run_url = '{protocol}://{host}:{port}/workflow/{workflow_id}/build/{workflow_run_id}' \
            .format(host=host, port=port, protocol=protocol, workflow_run_id=workflow_run_id, workflow_id=workflow_id)

        logger.info("******  Workflow Run URL to Infoworks UI ******  \n{} ".format(IWX_wf_run_url))

        workflow_status_response = iwx_client.poll_workflow_run_till_completion(workflow_id=workflow_id,
                                                                                workflow_run_id=workflow_run_id,
                                                                                poll_interval=poll_time)

        logger.debug("poll_workflow_run_till_completion() - Response: {}".format(json.dumps(workflow_status_response)))

        workflow_status = workflow_status_response['result']['response'].get('result', {}) \
            .get('workflow_status', {}).get('state')

        logger.info("Workflow Run Execution Finished")

        if workflow_status in ('success', 'completed'):
            logger.info("***** Workflow Completed Successfully ***** ")
            workflow_status_code = SUCCESS_JOB_COMPLETED
        else:
            logger.info(f"***** Workflow {workflow_status.upper()} *****")
            logger.info("***** Workflow Run URL to Infoworks UI *****  \n{} ".format(IWX_wf_run_url))

            get_workflow_run_job_summary(domain_id, workflow_run_id)
            workflow_status_code = WORKFLOW_FAILURE
        '''
        if workflow_status == 'failed':
            logger.info("***** Workflow Run Failed *****")
            logger.info("***** Workflow Run URL to Infoworks UI *****  \n{} ".format(IWX_wf_run_url))

            # Fetches Summary of Failed Jobs in the Workflow Run
            get_workflow_run_job_summary(domain_id, workflow_run_id)
            workflow_status_code = WORKFLOW_FAILURE
        elif workflow_status == 'success':
            logger.info("***** Workflow Completed Successfully ***** ")
            workflow_status_code = SUCCESS_JOB_COMPLETED
        else:
            workflow_status_code = ERROR_ABRUPT_FAILURE
        '''
        return workflow_status_code
    except Exception:
        raise Exception("Failed in initiate_workflow_process")


def trigger_and_poll_table_ingestion(source_id, body):
    table_ingestion_response = iwx_client.submit_source_job(source_id=source_id,
                                                            body=body,
                                                            poll=True, poll_timeout=86400,
                                                            polling_frequency=poll_time, retries=3
                                                            )
    logger.debug("submit_source_job() - Response: {}".format(json.dumps(table_ingestion_response)))

    try:
        if table_ingestion_response['result']['status'] == 'success':
            job_id = table_ingestion_response['result'].get('job_id')
            job_status = table_ingestion_response['result']['response'].get('result', {}).get('status')
            db_cluster_id = table_ingestion_response['result']['response'].get('result', {}).get('cluster_id')

            return job_id, job_status, db_cluster_id
        else:
            raise IwxException("TableIngestionException", table_ingestion_response)

    except Exception as table_ingestion_error:
        raise Exception(f"Failed to Trigger and Poll Table Ingestion - Error: {table_ingestion_error}")


def get_table_ingestion_log_summary(source_id, job_id):
    logger.info("Fetching Summary for Table Ingestion Job ID {}\n".format(job_id))
    table_ingestion_summary_response = iwx_client.get_source_job_summary_or_logs(source_id=source_id, job_id=job_id)
    logger.debug("get_source_job_summary_or_logs() - Response: {}".format(json.dumps(table_ingestion_summary_response)))
    try:
        if table_ingestion_summary_response['result']['status'] == 'success':
            table_ingestion_summary_result = table_ingestion_summary_response['result']['response'].get('result')
            if table_ingestion_summary_result:
                job_final_summary = ''
                for row in table_ingestion_summary_result:
                    job_final_summary = job_final_summary + '{} - Job ID: {} - {} \n' \
                        .format(row.get('timestamp'), row.get('job_id'), row.get('message'))

                logger.info("**** Job Summary for Job ID {} **** \n{}".format(job_id, job_final_summary))

            else:
                logger.info("No Summary for Table Ingestion Job Id {} Found".format(job_id))
        else:
            raise IwxException("TableIngestionJobSummaryFetchException", table_ingestion_summary_response)

    except Exception as table_ingestion_summary_error:
        logger.exception("Failed to Fetch Table Ingestion Job Summary \nError Info: {error}"
                         .format(error=table_ingestion_summary_error))


def get_cluster_details_from_name(environment_id, compute_name):
    try:
        logger.debug("Fetching Compute Information from Name")
        # Check if Compute is Interactive
        compute_details_response = iwx_client.get_compute_template_details(environment_id,
                                                                           params={'filter': {
                                                                               'name': compute_name}},
                                                                           is_interactive=True)
        logger.debug("get_compute_template_details() - Response: {}".format(json.dumps(compute_details_response)))
        compute_details_result = compute_details_response['result']['response'].get('result')

        if compute_details_result:
            compute_id = compute_details_result[0].get('id')
            compute_type = compute_details_result[0].get('launch_strategy')
            return compute_id, compute_type
        else:
            # Check if Compute is Ephemeral if not Interactive
            compute_details_response = iwx_client.get_compute_template_details(environment_id,
                                                                               params={'filter': {
                                                                                   'name': compute_name}})
            logger.debug("get_compute_template_details() - Response: {}".format(json.dumps(compute_details_response)))
            compute_details_result = compute_details_response['result']['response'].get('result')
            if compute_details_result:
                compute_id = compute_details_result[0].get('id')
                compute_type = compute_details_result[0].get('launch_strategy')
                return compute_id, compute_type
            else:
                raise Exception(
                    f"No Compute details found with name : {compute_name} in environment : {environment_id}")

    except Exception as compute_details_error:
        raise Exception(f"Failed to Fetch Compute Details - Error: {compute_details_error}")


def initiate_table_ingestion_process(source_id, body):
    try:
        logger.info("Table Ingestion Trigger and Monitoring Process")

        iwx_source_jobs_url = "{protocol}://{host}/data-catalog/source/{source_id}/jobs/" \
            .format(protocol=protocol, host=host, source_id=source_id)
        logger.info("***** Table Ingestion Run URL to Infoworks UI *****  \n{} ".format(iwx_source_jobs_url))

        job_id, ingestion_status, db_cluster_id = trigger_and_poll_table_ingestion(source_id, body)
        logger.info("Table Ingestion Finished")

        if ingestion_status in ('success', 'completed'):
            logger.info("***** Table Ingestion Completed Successfully ***** ")
            ingestion_status_code = SUCCESS_JOB_COMPLETED
        else:
            logger.info(f"***** Table Ingestion {ingestion_status.upper()} *****")
            logger.info("***** Table Ingestion Run URL to Infoworks UI *****  \n{} ".format(iwx_source_jobs_url))
            # Fetches Summary of Table Ingestion Job
            get_table_ingestion_log_summary(source_id, job_id)
            # Fetches Log File Path Information
            get_logfile_path(None, job_id)

            if 'table_group_id' in body:
                ingestion_status_code = TABLE_GROUP_INGESTION_FAILURE
            else:
                ingestion_status_code = TABLE_INGESTION_FAILURE

        return ingestion_status_code
    except Exception:
        raise Exception("Failed in initiate_table_ingestion_process")


def initiate_pipeline_process(job_object):
    try:
        pipeline_status_code = None
        logger.info("Pipeline Trigger and Monitoring Process")
        domain_id = job_object['domain_id']
        if job_object['pipeline_job_type'] == "pipeline":
            pipeline_id = job_object['pipeline_id']
            iwx_pipeline_jobs_url = f"{protocol}://{host}:{port}/pipeline/" \
                                    f"{pipeline_id}/build/jobs/"
            logger.info("***** Pipeline Jobs URL to Infoworks UI ***** \n{}".format(iwx_pipeline_jobs_url))

            pipeline_response = iwx_client.trigger_pipeline_job(domain_id=domain_id,
                                                                pipeline_id=pipeline_id, poll=True)
            logger.debug("trigger_pipeline_job() - Response: {}".format(json.dumps(pipeline_response)))

            job_id = pipeline_response['result']['job_id']
            pipeline_job_status = pipeline_response['result'].get('response', {}).get('result', {}).get('status')
            db_cluster_id = pipeline_response['result']['response'].get('result', {}).get('cluster_id')

        elif job_object['pipeline_job_type'] == "pipeline_group":
            pipeline_group_id = job_object['pipeline_group_id']
            iwx_pipeline_group_jobs_url = f"{protocol}://{host}:{port}/pipeline-group/" \
                                          f"{pipeline_group_id}/jobs/"
            logger.info("***** Pipeline Group Jobs URL to Infoworks UI ***** \n{}".format(iwx_pipeline_group_jobs_url))

            pipeline_group_response = iwx_client.trigger_pipeline_group_build(domain_id=domain_id,
                                                                              pipeline_group_id=pipeline_group_id,
                                                                              poll=True)
            logger.debug("trigger_pipeline_group_build() - Response: {}".format(json.dumps(pipeline_group_response)))

            job_id = pipeline_group_response['result']['job_id']
            pipeline_job_status = pipeline_group_response['result'].get('response', {}).get('result', {}).get('status')
            db_cluster_id = pipeline_group_response['result']['response'].get('result', {}).get('cluster_id')
        else:
            pipeline_job_status = None
            job_id = None
            db_cluster_id = None

        if pipeline_job_status in ("success", "completed"):
            logger.info("***** Pipeline Job Run Completed Successfully ***** ")
            pipeline_status_code = SUCCESS_JOB_COMPLETED
        else:
            logger.info(f"***** Pipeline Job Run {pipeline_job_status.upper()} *****")
            if job_object['pipeline_job_type'] == "pipeline":
                pipeline_status_code = PIPELINE_FAILURE

                get_pipeline_log_summary(domain_id, job_object['pipeline_id'], job_id)

                get_logfile_path(domain_id, job_id)

            elif job_object['pipeline_job_type'] == "pipeline_group":
                pipeline_status_code = PIPELINE_GROUP_FAILURE
                get_pipeline_group_pipelines(domain_id, job_object['pipeline_group_id'], job_id)
                get_pipeline_group_log_summary(domain_id, job_object['pipeline_group_id'], job_id)
                get_logfile_path(domain_id, job_id)

        return pipeline_status_code
    except Exception:
        raise Exception("Failed in initiate_pipeline_process")


def get_domain_id_from_name(domain_name):
    try:
        domain_id_response = iwx_client.get_domain_id(domain_name)
        logger.debug(f"get_domain_id() - Response: {json.dumps(domain_id_response)}")

        if domain_id_response['result']['status'] == 'success':
            domain_id = domain_id_response['result'].get('response', {}).get('domain_id')
            if domain_id:
                return domain_id
            else:
                raise Exception(f"Returned Domain Id is None")
        else:
            raise Exception(f"API Status Returned as Failed")
    except Exception:
        raise Exception(f"Failed to Fetch Domain Id with Domain Name {domain_name}")


def get_pipeline_log_summary(domain_id, pipeline_id, job_id):
    logger.info("Fetching Summary for Pipeline Job ID {}\n".format(job_id))
    pipeline_summary_response = iwx_client.get_pipeline_job_summary(domain_id, pipeline_id, job_id)
    logger.debug("get_pipeline_job_summary() - Response: {}".format(json.dumps(pipeline_summary_response)))

    try:
        if pipeline_summary_response['result'].get('response', {}).get('result', []):
            job_final_summary = ''
            for row in pipeline_summary_response['result'].get('response', {}).get('result', []):
                job_final_summary = job_final_summary + '{} - Job ID: {} - {} \n' \
                    .format(row.get('timestamp'), row.get('job_id'), row.get('message'))

            logger.info("**** Job Summary for Job ID {} **** \n{}".format(job_id, job_final_summary))
        else:
            logger.info("No Summary for Pipeline Job Id {} Found".format(job_id))
    except Exception as pipeline_summary_response_error:
        logger.exception("Failed to Fetch Pipeline Job Summary \nError Info: {error}"
                         .format(error=pipeline_summary_response_error))


def get_pipeline_group_log_summary(domain_id, pipeline_group_id, job_id):
    logger.info("Fetching Summary for Pipeline Job ID {}\n".format(job_id))
    pipeline_group_summary_response = iwx_client.get_pipeline_group_job_summary(domain_id, pipeline_group_id, job_id)
    logger.debug("get_pipeline_group_job_summary() - Response: {}".format(json.dumps(pipeline_group_summary_response)))

    try:
        if pipeline_group_summary_response['result'].get('response', {}).get('result', []):
            job_final_summary = ''
            for row in pipeline_group_summary_response['result'].get('response', {}).get('result', []):
                job_final_summary = job_final_summary + '{} - Job ID: {} - {} \n' \
                    .format(row.get('timestamp'), row.get('job_id'), row.get('message'))

            logger.info("**** Job Summary for Job ID {} **** \n{}".format(job_id, job_final_summary))
        else:
            logger.info("No Summary for Pipeline Group Job Id {} Found".format(job_id))
    except Exception as pipeline_group_summary_response_error:
        logger.exception("Failed to Fetch Pipeline Group Job Summary \nError Info: {error}"
                         .format(error=pipeline_group_summary_response_error))


def get_pipeline_group_pipelines(domain_id, pipeline_group_id, job_id):
    logger.info("Fetching Failed Pipelines in Pipeline Group Run")
    params = {"status": {"$in": ["failed", "canceled", "aborted"]}}

    pipeline_group_pipelines_response = iwx_client.list_pipeline_job_details_in_pipeline_group_job(
        domain_id, pipeline_group_id, job_id, params=params)
    logger.debug("get_pipeline_group_pipelines() - Response: {}".format(json.dumps(pipeline_group_pipelines_response)))
    if pipeline_group_pipelines_response['result'].get('response', {}).get('result', []):
        for pipeline in pipeline_group_pipelines_response['result'].get('response', {}).get('result', []):
            logger.info(f"Pipeline Name: {pipeline.get('pipeline_name')} - Pipeline ID: {pipeline.get('id')} "
                        f"- Pipeline Version ID: {pipeline.get('pipeline_version_id')}")
    else:
        logger.info("No Failed Pipelines Found")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Infoworks Scheduler Script to Trigger Workflow, Table Ingestion '
                                                 'and Table Group Ingestion')
    # Common Parameters between all modes
    shared_configs_parser = argparse.ArgumentParser(add_help=False)
    shared_configs_parser.add_argument('--config_folder', help='Folder Path to configuration file', required=True)
    shared_configs_parser.add_argument('--poll_time', help='Polling Time(in seconds) to get the status of the job',
                                       required=False)
    shared_configs_parser.add_argument('--logs_folder', help='Folder Path to store triggered job logs', required=False)

    subparsers = parser.add_subparsers(help="Mode", dest='trigger_mode')

    parser_workflow = subparsers.add_parser("workflow", help="To Trigger Workflow", parents=[shared_configs_parser])
    parser_workflow.add_argument('--domain_name', type=str, required=True, help='Domain Name of Workflow')
    parser_workflow.add_argument('--workflow_name', type=str, required=True, help='Workflow Name to trigger')

    parser_table_ingestion = subparsers.add_parser("table_ingestion", help="To Trigger Table Ingestion",
                                                   parents=[shared_configs_parser])
    parser_table_ingestion.add_argument("--source_name", type=str, help='Source Name', required=True)
    parser_table_ingestion.add_argument('--job_type', type=str, help='Job Type', default="truncate_reload")
    parser_table_ingestion.add_argument('--environment_name', type=str, help='Environment Name', required=True)
    parser_table_ingestion.add_argument('--compute_name', type=str, help='Cluster Name', required=True)
    parser_table_ingestion.add_argument('--table_names', type=str, help='Table Names', required=True)

    parser_table_group_ingestion = subparsers.add_parser("table_group_ingestion",
                                                         help="To Trigger Table Group Ingestion",
                                                         parents=[shared_configs_parser])
    parser_table_group_ingestion.add_argument("--source_name", type=str, help='Source Name', required=True)
    parser_table_group_ingestion.add_argument('--job_type', type=str, help='Job Type', default="truncate_reload")
    parser_table_group_ingestion.add_argument('--table_group_name', type=str, help='Table Group Name', required=True)

    parser_pipeline_group = subparsers.add_parser("pipeline_group", help="To Trigger Pipeline Group",
                                                  parents=[shared_configs_parser])
    parser_pipeline_group.add_argument("--domain_name", type=str, help='Domain Name', required=True)
    parser_pipeline_group.add_argument('--pipeline_group_name', type=str, help='Pipeline Group Name', required=True)

    parser_pipeline = subparsers.add_parser("pipeline", help="To Trigger Pipeline", parents=[shared_configs_parser])
    parser_pipeline.add_argument("--domain_name", type=str, help='Domain Name', required=True)
    parser_pipeline.add_argument('--pipeline_name', type=str, help='Pipeline Name', required=True)

    args = parser.parse_args()

    if args.trigger_mode in ('workflow', 'table_ingestion', 'table_group_ingestion', 'pipeline', 'pipeline_group'):
        process_type = args.trigger_mode
    else:
        print("Provide valid trigger mode [workflow,table_ingestion,table_group_ingestion,pipeline,pipeline_group]")
        sys.exit(-1)

    # Read configurations from configuration file using configparser
    config = configparser.ConfigParser()
    config.read(f'{args.config_folder}/config.ini')

    SUCCESS_JOB_COMPLETED = config.getint("EXIT_CODES", "SUCCESS_JOB_COMPLETED")
    ERROR_ABRUPT_FAILURE = config.getint("EXIT_CODES", "ERROR_ABRUPT_FAILURE")
    TABLE_INGESTION_FAILURE = config.getint("EXIT_CODES", "TABLE_INGESTION_FAILURE")
    TABLE_GROUP_INGESTION_FAILURE = config.getint("EXIT_CODES", "TABLE_GROUP_INGESTION_FAILURE")
    WORKFLOW_FAILURE = config.getint("EXIT_CODES", "WORKFLOW_FAILURE")
    PIPELINE_FAILURE = config.getint("EXIT_CODES", "PIPELINE_FAILURE")
    PIPELINE_GROUP_FAILURE = config.getint("EXIT_CODES", "PIPELINE_GROUP_FAILURE")

    try:
        # Overwrite Value from Config.ini if argument is passed
        config_logs_folder = config.get("LOGGING", "LOG_FOLDER")
        if args.logs_folder:
            logs_folder = f'{args.logs_folder}/logs/{process_type}'
        elif config_logs_folder:
            logs_folder = f'{config_logs_folder}/logs/{process_type}'
        else:
            current_dir = os.getcwd()
            logs_folder = f'{current_dir}/logs/{process_type}'

        # Creating Logs Folder
        create_dir(logs_folder)

        # Info works Client SDK Initialization
        host = config.get("INFOWORKS_ENVIRONMENT", "HOST")
        port = config.getint("INFOWORKS_ENVIRONMENT", "PORT")
        protocol = config.get("INFOWORKS_ENVIRONMENT", "PROTOCOL")
        refresh_token = config.get("INFOWORKS_ENVIRONMENT", "REFRESH_TOKEN")
        request_time_out = config.getint("JOB_RUN_SETTINGS", "REQUEST_TIME_OUT")
        max_retries = config.getint("JOB_RUN_SETTINGS", "MAX_RETRIES")
        infoworks.sdk.local_configurations.REQUEST_TIMEOUT_IN_SEC = request_time_out
        infoworks.sdk.local_configurations.MAX_RETRIES = max_retries
        iwx_client = InfoworksClientSDK()
        iwx_client.initialize_client_with_defaults(protocol, host, port, refresh_token)

        # Overwrite Value from Config.ini if argument is passed
        poll_time = config.getint("JOB_RUN_SETTINGS", "POLL_TIME")
        if args.poll_time is not None:
            poll_time = args.poll_time

        # Setting Logging Level
        log_level = config.get("LOGGING", "LOG_LEVEL")

        # Current Time
        current_time = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

        status_code = SUCCESS_JOB_COMPLETED
        if process_type == "workflow":
            # Logger Initialization
            log_filename = f'IWX_JM_WorkflowRun_{args.domain_name}_{args.workflow_name}_{current_time}.log'
            logger = configure_logger(logs_folder, log_filename)
            try:
                # Initiate Workflow process
                iwx_domain_id = get_domain_id_from_name(args.domain_name)
                workflow_id_response = iwx_client.get_workflow_id(workflow_name=args.workflow_name,
                                                                  domain_id=iwx_domain_id)
                logger.debug(f"get_workflow_id() - Response: {json.dumps(workflow_id_response)}")

                if workflow_id_response['result']['status'] == "success":
                    iwx_workflow_id = workflow_id_response['result']['response']['id']
                else:
                    raise Exception(f"API Status Returned as Failed")

                status_code = initiate_workflow_process(iwx_domain_id, iwx_workflow_id)
            except Exception as e:
                raise Exception("Failed to finish workflow run - Error: {error}".format(error=repr(e)))

        elif process_type == "table_ingestion":
            # Logger Initialization
            log_filename = f'IWX_JM_TableIngestionRun_{args.source_name}_{current_time}.log'
            logger = configure_logger(logs_folder, log_filename)

            try:
                # Fetching IWX Source ID with Source Name
                source_id_response = iwx_client.get_sourceid_from_name(args.source_name)
                logger.debug(f"get_sourceid_from_name() - Response: {source_id_response}")

                if source_id_response['result']['status'] == "success":
                    iwx_source_id = source_id_response['result']['response']['id']
                else:
                    raise Exception(f"API Status Returned as Failed")

                # Fetching IWX Environment ID with Environment Name
                environment_id_response = iwx_client.get_environment_id_from_name(args.environment_name)
                logger.debug(f"get_environment_id_from_name() - Response: {environment_id_response}")

                if environment_id_response['result']['status'] == "success":
                    if environment_id_response['result']['response']['environment_id']:
                        iwx_environment_id = environment_id_response['result']['response']['environment_id']
                    else:
                        raise Exception(f"Environment Id Not Found with Name {args.environment_name}")
                else:
                    raise Exception(f"API Status Returned as Failed")

                # Fetching Table IDs with Table Names
                table_ids = []
                for table_name in args.table_names.split(","):
                    try:
                        table_ids.append(iwx_client.get_tableid_from_name(source_id=iwx_source_id,
                                                                          table_name=table_name.strip()))
                    except Exception as err:
                        raise Exception(f"Table ID not Found with Name {table_name}")

                iwx_compute_id, iwx_compute_type = get_cluster_details_from_name(iwx_environment_id, args.compute_name)

                if iwx_compute_type == "persistent":
                    cluster_key = "interactive_cluster_id"
                else:
                    cluster_key = "compute_template_id"

                table_ingestion_body = {"job_type": args.job_type,
                                        "job_name": "Jobs_via_Infoworks_Job_Trigger_Script",
                                        cluster_key: iwx_compute_id,
                                        "table_ids": table_ids}

                logger.debug("Table Ingestion Body : {}".format(table_ingestion_body))

                status_code = initiate_table_ingestion_process(iwx_source_id, table_ingestion_body)
            except Exception as e:
                raise Exception("Failed to finish table ingestion - Error: {error}".format(error=repr(e)))

        elif process_type == "table_group_ingestion":
            # Logger Initialization
            log_filename = f'IWX_JM_TableGroupIngestionRun_{args.source_name}_{args.table_group_name}_{current_time}.log'
            logger = configure_logger(logs_folder, log_filename)

            try:
                # Fetching IWX Source ID with Source Name
                source_id_response = iwx_client.get_sourceid_from_name(args.source_name)
                logger.debug(f"get_sourceid_from_name() - Response: {source_id_response}")

                if source_id_response['result']['status'] == "success":
                    iwx_source_id = source_id_response['result']['response']['id']
                else:
                    raise Exception(f"API Status Returned as Failed")

                # Fetching IWX Table Group ID with Table Group Name
                table_group_id_response = iwx_client.get_table_group_details(
                    source_id=iwx_source_id,
                    params={'filter': {"name": args.table_group_name}})
                logger.debug(f"get_table_group_details() - Response: {table_group_id_response}")

                if table_group_id_response['result']['status'] == "success":
                    if table_group_id_response['result']['response']['result']:
                        iwx_table_group_id = table_group_id_response['result']['response']['result'][0]['id']
                    else:
                        raise Exception(f"Table Group Not Found with Name {args.table_group_name}")
                else:
                    raise Exception(f"API Status Returned as Failed")

                table_group_ingestion_body = {"job_type": args.job_type,
                                              "table_group_id": iwx_table_group_id}

                status_code = initiate_table_ingestion_process(iwx_source_id, table_group_ingestion_body)
            except Exception as e:
                raise Exception("Failed to finish table group ingestion - Error: {error}".format(error=repr(e)))

        elif process_type == "pipeline":
            # Logger Initialization
            log_filename = f'IWX_JM_PipelineRun_{args.pipeline_name}_{current_time}.log'
            logger = configure_logger(logs_folder, log_filename)

            try:
                # Fetching IWX Domain ID with Domain Name
                iwx_domain_id = get_domain_id_from_name(args.domain_name)

                # Fetching IWX Pipeline Group ID with Pipeline Group Name
                pipeline_id_response = iwx_client.get_pipeline_id(pipeline_name=args.pipeline_name,
                                                                  domain_id=iwx_domain_id)
                logger.debug(f"get_pipeline_id() - Response: {pipeline_id_response}")

                if pipeline_id_response['result']['status'] == "success":
                    iwx_pipeline_id = pipeline_id_response['result']['pipeline_id']
                else:
                    raise Exception(f"API Status Returned as Failed")

                pipeline_job_object = {"pipeline_job_type": "pipeline",
                                       "domain_id": iwx_domain_id,
                                       "pipeline_id": iwx_pipeline_id}

                status_code = initiate_pipeline_process(pipeline_job_object)
            except Exception as e:
                raise Exception("Failed to finish Pipeline Job \nError: {error}".format(error=repr(e)))

        elif process_type == "pipeline_group":
            # Logger Initialization
            log_filename = f'IWX_JM_PipelineGroupRun_{args.pipeline_group_name}_{current_time}.log'
            logger = configure_logger(logs_folder, log_filename)

            try:
                # Fetching IWX Domain ID with Domain Name
                iwx_domain_id = get_domain_id_from_name(args.domain_name)

                # Fetching IWX Pipeline Group ID with Pipeline Group Name
                pipeline_group_id_response = iwx_client.list_pipeline_groups_under_domain(
                    domain_id=iwx_domain_id,
                    params={'filter': {"name": args.pipeline_group_name}})
                logger.debug(
                    f"list_pipeline_groups_under_domain() - Response: {json.dumps(pipeline_group_id_response)}")

                if pipeline_group_id_response['result']['status'] == "success":
                    if pipeline_group_id_response['result']['response']['result']:
                        iwx_pipeline_group_id = \
                            pipeline_group_id_response['result']['response']['result'][0]['pipeline_group_details'][
                                'id']
                    else:
                        raise Exception(f"Pipeline Group Not Found with Name {args.pipeline_group_name}")
                else:
                    raise Exception(f"API Status Returned as Failed")

                pipeline_job_object = {"pipeline_job_type": "pipeline_group",
                                       "domain_id": iwx_domain_id,
                                       "pipeline_group_id": iwx_pipeline_group_id}

                status_code = initiate_pipeline_process(pipeline_job_object)
            except Exception as e:
                raise Exception("Failed to finish Pipeline Job - Error: {error}".format(error=repr(e)))

    except Exception as error:
        print("Process Failed: {}".format(repr(error)))
        traceback.print_exc()
        status_code = ERROR_ABRUPT_FAILURE

    sys.exit(status_code)
