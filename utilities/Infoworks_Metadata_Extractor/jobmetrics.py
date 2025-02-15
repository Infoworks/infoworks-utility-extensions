import copy
import datetime
import json
import logging
import math
import traceback
from collections import OrderedDict
from infoworks.sdk import url_builder
from infoworks.sdk.utils import IWUtils
from infoworks.error import AdminError
import pandas as pd
from concurrent.futures import ThreadPoolExecutor


class JobMetricsClient:
    job_type_mappings = {
        'EXPORT_DATA': 'EXPORT',
        'SOURCE_CRAWL': 'FULL_LOAD',
        'SOURCE_CDC': 'INCREMENTAL',
        'SOURCE_CDC_MERGE': 'FULL_LOAD',
        'SOURCE_STRUCTURED_CRAWL': 'FULL_LOAD',
        'FULL_LOAD': 'FULL_LOAD',
        'CDC': 'INCREMENTAL',
        'MERGE': 'INCREMENTAL',
        'SOURCE_MERGE': 'INCREMENTAL',
        'SOURCE_STRUCTURED_CDC_MERGE': 'INCREMENTAL',
        'SOURCE_STRUCTURED_CDC': 'INCREMENTAL',
        'SOURCE_SEMISTRUCTURED_CDC_MERGE': 'INCREMENTAL',
        'SOURCE_SEMISTRUCTURED_CDC': 'INCREMENTAL'
    }

    def __init__(self, iwx_client):
        super(JobMetricsClient, self).__init__()
        self.job_metrics_final = []
        self.iwx_client = iwx_client

    def get_tablegroup_id_from_job(self, job_id, source_id):
        try:
            url_to_get_jobinfo = url_builder.submit_source_job(self.iwx_client.client_config, source_id) + f"/{job_id}"
            response = IWUtils.ejson_deserialize(
                self.iwx_client.call_api("GET", url_to_get_jobinfo,
                                         IWUtils.get_default_header_for_v3(
                                             self.iwx_client.client_config['bearer_token'])
                                         ).content)
            if response is not None and "result" in response:
                result = response.get("result")
                table_group_id = result.get("sub_entity_id", "")
                processedAt = result.get("processed_at", "")
                job_end_time = result.get("last_updated", "")
                status = result.get("status", "")
                # entity_type = result.get("entity_type", "")
                entity_type = "table"
                return table_group_id, processedAt, job_end_time, status, entity_type
        except Exception as e:
            raise AdminError("Unable to get table group details from the Job Id")

    def get_table_group_name(self, table_group_id, source_id):
        try:
            url_to_get_tginfo = url_builder.create_table_group_url(self.iwx_client.client_config,
                                                                   source_id) + f"/{table_group_id}"
            response = IWUtils.ejson_deserialize(
                self.iwx_client.call_api("GET", url_to_get_tginfo,
                                         IWUtils.get_default_header_for_v3(
                                             self.iwx_client.client_config['bearer_token'])
                                         ).content)
            if response is not None and "result" in response:
                result = response.get("result", {})
                return result.get('name'), result.get('tables')
        except Exception as e:
            raise AdminError("Unable to get table group info")

    def get_ingestion_metrics(self, job_id, source_id):
        """
        Function to get ingestion metrics for the job
        :param job_id: Entity identifier of the job
        :param source_id: Entity identifier of the source
        :return: List of job metrics
        """
        try:
            combinedJobMetric = []
            url_to_get_job_ingestion_metrics = url_builder.get_job_metrics_url(self.iwx_client.client_config, source_id,
                                                                               job_id) + "?limit=50&offset=0"
            response = IWUtils.ejson_deserialize(
                self.iwx_client.call_api("GET", url_to_get_job_ingestion_metrics,
                                         IWUtils.get_default_header_for_v3(
                                             self.iwx_client.client_config['bearer_token'])
                                         ).content)
            if response is not None and "result" in response:
                result = response.get("result", [])
                while len(result) > 0:
                    combinedJobMetric.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.iwx_client.client_config['ip'],
                                                                      port=self.iwx_client.client_config['port'],
                                                                      protocol=self.iwx_client.client_config[
                                                                          'protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(self.callurl(nextUrl).content)
                    result = response.get("result", [])
                return combinedJobMetric
        except Exception as e:
            raise AdminError("Unable to get ingestion job metrics info")

    def get_export_metrics(self, job_id, source_id):
        """
        Function to get export metrics for the source job
        :param job_id: Entity identifier of the job
        :param source_id: Entity identifier of the source
        :return: List of job metrics
        """
        try:
            combinexportMetric = []
            url_to_get_export_job_metrics = url_builder.get_export_metrics_source_url(self.iwx_client.client_config,
                                                                                      source_id,
                                                                                      job_id) + "?limit=50&offset=0"
            response = IWUtils.ejson_deserialize(
                self.iwx_client.call_api("GET", url_to_get_export_job_metrics,
                                         IWUtils.get_default_header_for_v3(
                                             self.iwx_client.client_config['bearer_token'])
                                         ).content)
            if response is not None and "result" in response:
                result = response.get("result", [])
                while len(result) > 0:
                    combinexportMetric.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.iwx_client.client_config['ip'],
                                                                      port=self.iwx_client.client_config['port'],
                                                                      protocol=self.iwx_client.client_config[
                                                                          'protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(self.callurl(nextUrl).content)
                    result = response.get("result", [])
                return combinexportMetric
        except Exception as e:
            raise AdminError("Unable to get export job metrics info")

    def get_ingestion_metrics_with_job_id(self, job_id):
        """
        Function to get ingestion metrics for the job
        :param job_id: Entity identifier of the job
        :return: List of job metrics
        """
        try:
            combinedJobMetric = []
            url_to_get_job_ingestion_metrics = url_builder.get_ingestion_metrics_admin_url(
                self.iwx_client.client_config,
                job_id) + "?limit=50&offset=0"
            response = IWUtils.ejson_deserialize(
                self.iwx_client.call_api("GET", url_to_get_job_ingestion_metrics,
                                         IWUtils.get_default_header_for_v3(
                                             self.iwx_client.client_config['bearer_token'])
                                         ).content)
            if response is not None and "result" in response:
                result = response.get("result", [])
                while len(result) > 0:
                    combinedJobMetric.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.iwx_client.client_config['ip'],
                                                                      port=self.iwx_client.client_config['port'],
                                                                      protocol=self.iwx_client.client_config[
                                                                          'protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(self.callurl(nextUrl).content)
                    result = response.get("result", [])
                return combinedJobMetric
        except Exception as e:
            raise AdminError("Unable to get ingestion job metrics info")

    def get_export_metrics_with_job_id(self, job_id):
        """
        Function to get ingestion metrics for the job
        :param job_id: Entity identifier of the job
        :return: List of job metrics
        """
        try:
            exportMetric = []
            url_to_get_job_ingestion_metrics = url_builder.get_export_metrics_url(self.iwx_client.client_config,
                                                                                  job_id) + "?limit=50&offset=0"
            response = IWUtils.ejson_deserialize(
                self.iwx_client.call_api("GET", url_to_get_job_ingestion_metrics,
                                         IWUtils.get_default_header_for_v3(
                                             self.iwx_client.client_config['bearer_token'])
                                         ).content)
            if response is not None and "result" in response:
                result = response.get("result", [])
                while len(result) > 0:
                    exportMetric.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.iwx_client.client_config['ip'],
                                                                      port=self.iwx_client.client_config['port'],
                                                                      protocol=self.iwx_client.client_config[
                                                                          'protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(self.callurl(nextUrl).content)
                    result = response.get("result", [])
                return exportMetric
        except Exception as e:
            raise AdminError("Unable to get export job metrics info")

    def get_metrics_prodops(self, config_body):
        """
        Function to Fetch the metrics for a given time period
        :param config_body: JSON dict
        config_body = {'limit': 10000, 'date_range': {'type': 'last', 'unit': 'day', 'value': 1}, 'offset': 0}
        :return: List of job metrics
        """
        try:
            if config_body is None:
                raise Exception("config_body cannot be None")
            metrics = []
            url_to_get_metrics = url_builder.get_metrics_prodops_url(
                self.iwx_client.client_config) + "?limit=50&offset=0"
            response = IWUtils.ejson_deserialize(
                self.iwx_client.call_api("GET", url_to_get_metrics,
                                         IWUtils.get_default_header_for_v3(
                                             self.iwx_client.client_config['bearer_token'])
                                         ).content)
            if response is not None and "result" in response:
                result = response.get("result", [])
                while len(result) > 0:
                    metrics.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.iwx_client.client_config['ip'],
                                                                      port=self.iwx_client.client_config['port'],
                                                                      protocol=self.iwx_client.client_config[
                                                                          'protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(self.callurl(nextUrl).content)
                    result = response.get("result", [])
                return metrics
        except Exception as e:
            raise AdminError("Unable to get job metrics info")

    def get_source_file_paths(self, source_id, table_id, job_id):
        try:
            source_files = []
            url_to_get_src_filepaths = url_builder.get_source_file_paths_url(self.iwx_client.client_config, source_id,
                                                                             table_id,
                                                                             job_id) + "?limit=50&offset=0"
            response = IWUtils.ejson_deserialize(
                self.iwx_client.call_api("GET", url_to_get_src_filepaths,
                                         IWUtils.get_default_header_for_v3(
                                             self.iwx_client.client_config['bearer_token'])
                                         ).content)
            if response is not None and "result" in response:
                result = response.get("result", [])
                while len(result) > 0:
                    source_files.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.iwx_client.client_config['ip'],
                                                                      port=self.iwx_client.client_config['port'],
                                                                      protocol=self.iwx_client.client_config[
                                                                          'protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(self.callurl(nextUrl).content)
                    result = response.get("result", [])
                return source_files
        except Exception as e:
            print(e)
            raise AdminError("Unable to get source file pat metrics info")

    def get_pipeline_build_metrics(self, job_id):
        try:
            combinedJobMetric = []
            url_to_get_pipeline_build_metrics = url_builder.get_pipeline_build_metrics_url(
                self.iwx_client.client_config,
                job_id) + "?limit=50&offset=0"
            response = IWUtils.ejson_deserialize(
                self.iwx_client.call_api("GET", url_to_get_pipeline_build_metrics,
                                         IWUtils.get_default_header_for_v3(
                                             self.iwx_client.client_config['bearer_token'])
                                         ).content)
            if response is not None and "result" in response:
                result = response.get("result", [])
                while len(result) > 0:
                    combinedJobMetric.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.iwx_client.client_config['ip'],
                                                                      port=self.iwx_client.client_config['port'],
                                                                      protocol=self.iwx_client.client_config[
                                                                          'protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(self.callurl(nextUrl).content)
                    result = response.get("result", [])
                return combinedJobMetric
        except Exception as e:
            raise AdminError("Unable to get pipeline job metrics info")

    def get_source_info(self):
        try:
            combined_sources = []
            url_to_get_get_source_info = url_builder.get_source_details_url(
                self.iwx_client.client_config) + "?limit=50&offset=0"
            response = IWUtils.ejson_deserialize(
                self.iwx_client.call_api("GET", url_to_get_get_source_info,
                                         IWUtils.get_default_header_for_v3(
                                             self.iwx_client.client_config['bearer_token'])
                                         ).content)
            if response is not None and "result" in response:
                result = response.get("result", [])
                while len(result) > 0:
                    combined_sources.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.iwx_client.client_config['ip'],
                                                                      port=self.iwx_client.client_config['port'],
                                                                      protocol=self.iwx_client.client_config[
                                                                          'protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(self.callurl(nextUrl).content)
                    result = response.get("result", [])
                return combined_sources
        except Exception as e:
            raise AdminError("Unable to get source details")

    def get_jobs_of_single_source(self, source_id, date_string):
        try:
            combinedJobs = []
            # filter_condition = "{\"$and\": [{\"createdAt\": {\"$gte\": {\"$date\": \"" + date_string + "\"}}},{\"jobType\": {\"$in\": [\"source_crawl\", \"source_cdc_merge\"]}}]}"
            filter_condition = "{\"$and\": [{\"entityId\":\"" + source_id + "\"},{\"last_upd\": {\"$gte\": {\"$date\": \"" + date_string + "\"}}},{\"jobType\": {\"$in\": [\"source_crawl\",\"source_structured_crawl\",\"source_cdc\",\"source_cdc_merge\",\"export_data\",\"full_load\",\"cdc\",\"source_merge\",\"source_structured_cdc_merge\",\"source_structured_cdc\",\"source_cdc_merge\",\"source_semistructured_cdc_merge\",\"source_semistructured_cdc\"]}}, {\"status\":{\"$in\":[\"failed\",\"completed\",\"running\",\"pending\",\"canceled\",\"aborted\"]}}]}"
            url_to_get_jobs = url_builder.get_job_status_url(
                self.iwx_client.client_config) + f"?filter={filter_condition}&limit=50&offset=0"
            response = IWUtils.ejson_deserialize(
                self.iwx_client.call_api("GET", url_to_get_jobs,
                                         IWUtils.get_default_header_for_v3(
                                             self.iwx_client.client_config['bearer_token'])
                                         ).content)

            if response is not None and "result" in response:
                result = response.get("result", [])
                while len(result) > 0:
                    combinedJobs.extend(result)
                    next_url = response.get('links')['next']
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=next_url,
                                                                      ip=self.iwx_client.client_config['ip'],
                                                                      port=self.iwx_client.client_config['port'],
                                                                      protocol=self.iwx_client.client_config[
                                                                          'protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(self.callurl(nextUrl).content)
                    result = response.get("result", [])
                return combinedJobs
        except Exception as e:
            raise AdminError("Unable to get ingestion jobs list of source")

    def get_cluster_runs_of_job(self, job_id):
        try:
            cluster_runs = []
            url_to_get_cluster_jobs = url_builder.get_cluster_jobs_status_url(self.iwx_client.client_config, job_id)
            response = IWUtils.ejson_deserialize(
                self.iwx_client.call_api("GET", url_to_get_cluster_jobs,
                                         IWUtils.get_default_header_for_v3(
                                             self.iwx_client.client_config['bearer_token'])
                                         ).content)
            if response is not None and "result" in response:
                result = response.get("result", [])
                while len(result) > 0:
                    cluster_runs.extend(result)
                    next_url = response.get('links')['next']
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=next_url,
                                                                      ip=self.iwx_client.client_config['ip'],
                                                                      port=self.iwx_client.client_config['port'],
                                                                      protocol=self.iwx_client.client_config[
                                                                          'protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(self.callurl(nextUrl).content)
                    result = response.get("result", [])
                return cluster_runs
        except Exception as e:
            raise AdminError("Unable to get cluster jobs list of source")

    def get_pipeline_jobs(self, date_string):
        try:
            combinedJobs = []
            filter_condition = "{\"$and\": [{\"last_upd\": {\"$gte\": {\"$date\": \"" + date_string + "\"}}},{\"jobType\": {\"$in\": [\"pipeline_build\"]}}, {\"status\":{\"$in\":[\"failed\",\"completed\",\"running\",\"pending\",\"aborted\",\"canceled\"]}}]}"
            url_to_get_jobs = url_builder.get_job_status_url(
                self.iwx_client.client_config) + f"?filter={filter_condition}&limit=50&offset=0"
            response = IWUtils.ejson_deserialize(
                self.iwx_client.call_api("GET", url_to_get_jobs,
                                         IWUtils.get_default_header_for_v3(
                                             self.iwx_client.client_config['bearer_token'])
                                         ).content)

            if response is not None and "result" in response:
                result = response.get("result", [])
                while len(result) > 0:
                    combinedJobs.extend(result)
                    next_url = response.get('links')['next']
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=next_url,
                                                                      ip=self.iwx_client.client_config['ip'],
                                                                      port=self.iwx_client.client_config['port'],
                                                                      protocol=self.iwx_client.client_config[
                                                                          'protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(self.callurl(nextUrl).content)
                    result = response.get("result", [])
                return combinedJobs
        except Exception as e:
            raise AdminError("Unable to get pipeline jobs list")

    def get_workflow_jobs(self, date_string):
        try:
            combinedJobs = []
            # filter = [{"name": "run_id", "op": "=", "value": "66960d446b62910064a7fa50"}]
            # filter_condition = "{\"$and\": [{\"last_upd\": {\"$gte\": {\"$date\": \"" + date_string + "\"}}},{\"jobType\": {\"$in\": [\"pipeline_build\"]}}, {\"status\":{\"$in\":[\"failed\",\"completed\",\"running\",\"pending\",\"aborted\",\"canceled\"]}}]}"
            filter_condition = "[{\"name\": \"end_date\", \"op\": \">=\", \"value\": \"" + date_string + "\"},{\"name\": \"status\", \"op\": \"LIKE\", \"value\": \"(failed|completed|success|running|pending|aborted|canceled)\"}]"
            url_to_get_jobs = url_builder.get_workflow_runs_url(
                self.iwx_client.client_config) + f"?filter={filter_condition}&limit=50&offset=0"
            response = IWUtils.ejson_deserialize(
                self.iwx_client.call_api("GET", url_to_get_jobs,
                                         IWUtils.get_default_header_for_v3(
                                             self.iwx_client.client_config['bearer_token'])
                                         ).content)
            if response is not None and "result" in response:
                result = response.get("result", {}).get('items', [])
                while len(result) > 0:
                    # combinedJobs.extend(result)
                    for workflow_run in result:
                        # print(f"Workflow Run: {workflow_run}")
                        url_to_get_workflow_run_jobs = url_builder.get_all_workflow_run_jobs_url(
                            self.iwx_client.client_config, workflow_run['run_id']) + f"?limit=50&offset=0"
                        jobs_response = IWUtils.ejson_deserialize(
                            self.iwx_client.call_api("GET", url_to_get_workflow_run_jobs,
                                                     IWUtils.get_default_header_for_v3(
                                                         self.iwx_client.client_config['bearer_token'])
                                                     ).content)
                        # print(f"Workflow Job Response: {jobs_response}")
                        jobs_result = jobs_response.get('result', [])
                        if len(jobs_result) == 0:  # No Jobs for Workflow Run exists.
                            combinedJobs.append(workflow_run)
                    next_url = response.get('links')['next']
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=next_url,
                                                                      ip=self.iwx_client.client_config['ip'],
                                                                      port=self.iwx_client.client_config['port'],
                                                                      protocol=self.iwx_client.client_config[
                                                                          'protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(self.callurl(nextUrl).content)
                    result = response.get("result", {}).get("items", [])
                return combinedJobs
            else:
                logging.info("No workflow runs found")
                return []
        except Exception as error:
            raise AdminError("Unable to get workflow jobs list")

    def get_workflow_job_metrics(self, job=None, workflow_id=None, workflow_run_id=None):
        """
        Gets the Infoworks workflow job metrics
        :param job: job object to get the workflow job metrics details
        :type job: JSON Object
        :param workflow_id: Workflow id to get the jobs
        :type workflow_id: String
        :param workflow_run_id: Workflow run id to get the jobs
        :type workflow_run_id: String
        :return: response dict
        """
        if job is None:
            self.iwx_client.logger.error("job is a mandatory parameter")
            raise Exception("job is a mandatory parameter")
        temp = []
        job_workflow_id = job.get('workflow_id', '')
        job_workflow_run_id = job.get('run_id', '')
        job_status = job.get('run_status', '')
        job_start_time = job.get('start_date')
        job_end_time = job.get('end_date')
        job_template = {
            'workflow_id': job_workflow_id,
            'workflow_run_id': job_workflow_run_id,
            'job_id': '',
            'job_type': '',
            'job_start_time': job_start_time,
            'job_end_time': job_end_time,
            'job_created_by': '',
            'cluster_id': '',
            'cluster_name': '',
            "databricks_job_id": "",
            "source_file_names": [],
            'job_status': job_status.upper(),
            'entity_type': '',
            "source_name": "", "source_schema_name": '',
            "source_database_name": '', "table_group_name": "", "iwx_table_name": "",
            'starting_watermark_value': '', 'ending_watermark_value': '',
            "pre_target_count": "", "fetch_records_count": 0,
            "target_records_count": ""}
        if workflow_id is not None and workflow_run_id is not None:
            if not (job_workflow_id == workflow_id and job_workflow_run_id == workflow_run_id):
                return
        od = OrderedDict()
        for key in ['workflow_id', 'workflow_run_id', 'job_id', 'entity_type', 'job_type', 'job_start_time',
                    'job_end_time', 'job_created_by', 'cluster_id', 'cluster_name', 'databricks_job_id',
                    'job_status',
                    'source_name', 'source_file_names', 'source_schema_name', 'source_database_name',
                    'table_group_name',
                    'iwx_table_name', 'starting_watermark_value', 'ending_watermark_value', 'target_schema_name',
                    'target_table_name', 'pre_target_count', 'fetch_records_count',
                    'target_records_count']:
            od[key] = job_template.get(key, "")
        self.job_metrics_final.append(od)
        return

    def get_all_source_jobs(self, date_string):
        try:
            combinedJobs = []
            filter_condition = "{\"$and\": [{\"last_upd\": {\"$gte\": {\"$date\": \"" + date_string + "\"}}},{\"entityType\": {\"$in\": [\"source\"]}}, {\"jobType\": {\"$in\": [\"source_crawl\",\"source_structured_crawl\",\"source_cdc\",\"source_cdc_merge\",\"export_data\",\"full_load\",\"cdc\",\"source_merge\",\"source_structured_cdc_merge\",\"source_structured_cdc\",\"source_cdc_merge\",\"source_semistructured_cdc_merge\",\"source_semistructured_cdc\"]}},{\"status\":{\"$in\":[\"failed\",\"completed\",\"running\",\"pending\",\"canceled\",\"aborted\"]}}]}"
            url_to_get_all_source_jobs = url_builder.get_job_status_url(
                self.iwx_client.client_config) + f"?filter={filter_condition}&limit=50&offset=0"
            response = IWUtils.ejson_deserialize(
                self.iwx_client.call_api("GET", url_to_get_all_source_jobs,
                                         IWUtils.get_default_header_for_v3(
                                             self.iwx_client.client_config['bearer_token'])
                                         ).content)
            if response is not None and "result" in response:
                result = response.get("result", [])
                while len(result) > 0:
                    combinedJobs.extend(result)
                    next_url = response.get('links')['next']
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=next_url,
                                                                      ip=self.iwx_client.client_config['ip'],
                                                                      port=self.iwx_client.client_config['port'],
                                                                      protocol=self.iwx_client.client_config[
                                                                          'protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(self.callurl(nextUrl).content)
                    result = response.get("result", [])
                return combinedJobs
        except Exception as e:
            raise AdminError("Unable to get source ingestion jobs list")

    def callurl(self, url):
        try:
            response = self.iwx_client.call_api("GET", url,
                                                IWUtils.get_default_header_for_v3(
                                                    self.iwx_client.client_config['bearer_token'])
                                                )
            if response is not None:
                return response
        except Exception as e:
            raise AdminError("Unable to get response for url: {url}".format(url=url))

    def get_table_info(self, source_id, table_id):
        try:
            url_to_get_table_info = url_builder.get_table_configuration(
                self.iwx_client.client_config, source_id, table_id)
            response = IWUtils.ejson_deserialize(
                self.iwx_client.call_api("GET", url_to_get_table_info,
                                         IWUtils.get_default_header_for_v3(
                                             self.iwx_client.client_config['bearer_token'])
                                         ).content)
            if response is not None:
                result = response.get("result", {})
                return result
        except Exception as e:
            raise AdminError("Unable to get table info")

    def get_table_export_info(self, source_id, table_id):
        try:
            url_to_get_table_info = url_builder.table_export_config_url(
                self.iwx_client.client_config, source_id, table_id)
            response = IWUtils.ejson_deserialize(
                self.iwx_client.call_api("GET", url_to_get_table_info,
                                         IWUtils.get_default_header_for_v3(
                                             self.iwx_client.client_config['bearer_token'])
                                         ).content)
            if response is not None and "result" in response:
                result = response.get("result", {})
                return result
        except Exception as e:
            raise AdminError("Unable to get export job metrics info")

    def get_pipeline_name(self, domain_id, pipeline_id):
        try:
            url_to_get_pipeline_info = url_builder.get_pipeline_url(self.iwx_client.client_config, domain_id,
                                                                    pipeline_id)
            response = IWUtils.ejson_deserialize(
                self.iwx_client.call_api("GET", url_to_get_pipeline_info,
                                         IWUtils.get_default_header_for_v3(
                                             self.iwx_client.client_config['bearer_token'])
                                         ).content)
            if response is not None and "result" in response:
                result = response.get("result", None)
                return result.get('name')
        except:
            raise AdminError("Unable to get pipeline NAME")

    def get_source_jobs_metrics_results_table_level(self, date_string, source, workflow_id=None, workflow_run_id=None):
        src_name = source["name"]
        source_id = source["id"]
        list_of_jobs_obj = self.get_jobs_of_single_source(source_id, date_string)
        try:
            for job in list_of_jobs_obj:
                # self.iwx_client.logger.info(f"Job Data: {json.dumps(job)}")
                job_id = job["id"]
                job_type = job["type"]
                job_status = job["status"]
                job_createdAt = job.get("created_at")
                # job_start_time = job_createdAt.split('.')[0].replace('T', ' ')
                job_start_time = job.get('build_started_at', '').split('.')[0].replace('T', ' ')
                job_end_time = job.get('build_ended_at', '').split('.')[0].replace('T', ' ')
                job_created_by = job.get('created_by')
                workflow_id = job.get('triggered_by', {}).get('parent_id', '')
                workflow_run_id = job.get('triggered_by', {}).get('run_id', '')

                # Fetches Cluster Run Info (i.e. Table Data in a Job)
                cluster_data = self.get_cluster_runs_of_job(job_id)

                # Fetches Table Group Info
                try:
                    tg_id, processedAt, job_end_time, job_status, entity_type = self.get_tablegroup_id_from_job(
                        job_id, source["id"])
                    job_end_time = job_end_time.split('.')[0].replace('T', ' ')
                    table_group_name, all_tables_list = self.get_table_group_name(tg_id, source["id"])
                except Exception as error:
                    # This means the job is non-table group job
                    table_group_name = ""
                    entity_type = "table"
                    all_tables_list = []

                # Fetches Ingestion Metrics
                ing_metrics = self.get_ingestion_metrics(str(job_id), source["id"])
                ing_metrics_df = pd.DataFrame(ing_metrics)
                if ing_metrics is not None:
                    ing_metrics_tables_list = list(set([i['table_id'] for i in ing_metrics]))
                else:
                    ing_metrics_tables_list = []

                # Fetches Export Metrics
                export_metrics = self.get_export_metrics(str(job_id), source_id)
                df_export = pd.DataFrame(export_metrics)
                if export_metrics is not None:
                    export_metrics_tables_list = list([i['table_id'] for i in export_metrics])
                else:
                    export_metrics_tables_list = []

                # Job Level Properties
                source_job_row_template = {
                    'workflow_id': workflow_id, 'workflow_run_id': workflow_run_id,
                    'job_id': job_id, 'job_type': self.job_type_mappings[job_type.upper()],
                    'job_start_time': job_start_time, 'job_end_time': job_end_time,
                    'job_created_by': job_created_by, 'cluster_id': "", "cluster_name": "", "databricks_job_id": "",
                    'job_status': job_status.upper(), 'job_table_status': '',
                    'entity_type': entity_type, "source_name": src_name, "source_schema_name": "",
                    "source_database_name": "", "source_file_names": [], "table_group_name": table_group_name,
                    "source_table_name": "", "iwx_table_name": "", 'starting_watermark_value': '', 'ending_watermark_value': '',
                    "target_schema_name": "", "target_table_name": "", "pre_target_count": "", "fetch_records_count": 0,
                    "target_records_count": "", "job_link": ""}

                if cluster_data:
                    # Table Level in a Job
                    for row in cluster_data:
                        cluster_id = row.get('cluster_id', '')
                        cluster_name = row.get('cluster_name', '')
                        databricks_job_id = ""
                        if row.get('application_type', '').lower() == "databricks":
                            databricks_job_id = row.get('application_run_id', '')
                        table_job_start_time = row.get('started_at', '').split('.')[0].replace('T', ' ')
                        table_job_end_time = row.get('ended_at', '').split('.')[0].replace('T', ' ')
                        iwx_table_name = row.get('entity_name', '')
                        table_id = row.get('sub_entity_id', '')
                        job_table_status = row.get('entity_run_details', {}).get('entity_run_status', '')

                        table_info = self.get_table_info(source_id, table_id)
                        table_name = table_info.get('name')
                        source_table_name = table_info.get('original_table_name')
                        target_table_name = table_info.get("configuration", {}).get('target_table_name', '')
                        target_schema_name = table_info.get("configuration", {}).get('target_schema_name', '')
                        table_row_count = table_info.get('row_count', 0)

                        source_job_row_template["job_start_time"] = table_job_start_time
                        source_job_row_template["job_end_time"] = table_job_end_time
                        source_job_row_template['cluster_id'] = cluster_id
                        source_job_row_template['cluster_name'] = cluster_name
                        source_job_row_template["databricks_job_id"] = databricks_job_id
                        source_job_row_template['job_table_status'] = job_table_status
                        source_job_row_template['source_table_name'] = source_table_name
                        source_job_row_template['iwx_table_name'] = table_name
                        source_job_row_template['target_schema_name'] = target_schema_name
                        source_job_row_template['target_table_name'] = target_table_name

                        if ing_metrics != [] and job_type != "export_data" and job_table_status.upper() == "SUCCESS":
                            if table_id in ing_metrics_tables_list:
                                filter1 = ing_metrics_df["table_id"] == table_id
                                table = {}
                                if len(ing_metrics_df.loc[filter1]) > 1:
                                    # Incremental Job
                                    filter2 = ing_metrics_df["job_type"] == "CDC"
                                    filter3 = ing_metrics_df["job_type"] == "MERGE"
                                    cdc_output = ing_metrics_df.loc[filter1 & filter2].to_dict('records')[0]
                                    merge_output = ing_metrics_df.loc[filter1 & filter3].to_dict('records')[0]
                                    for item in ['source_id', 'fetch_records_count', 'job_id',
                                                 'source_schema_name', 'source_database_name']:
                                        table[item] = cdc_output.get(item, "")
                                        table['job_type'] = "INCREMENTAL"
                                    for item in ['workflow_id', 'workflow_run_id', 'job_status',
                                                 'target_records_count', 'first_merged_watermark',
                                                 'last_merged_watermark']:
                                        table[item] = merge_output.get(item, "")
                                        table['job_type'] = "INCREMENTAL"
                                else:
                                    table = ing_metrics_df.loc[filter1].to_dict('records')[0]
                                table["source_schema_name"] = table.get("source_schema_name", "")
                                table["source_database_name"] = table.get("source_database_name", "")
                                table["source_file_names"] = self.get_source_file_paths(
                                    source['id'], table_id, job_id) if table["source_schema_name"] == "" else []
                                table["workflow_id"] = table.get("workflow_id", "")
                                table["workflow_run_id"] = table.get('workflow_run_id', "")
                                if workflow_id is not None and workflow_run_id is not None:
                                    if not (table["workflow_id"] == workflow_id and
                                            table["workflow_run_id"] == workflow_run_id):
                                        continue
                                table["entity_type"] = entity_type
                                table["starting_watermark_value"] = table.pop('first_merged_watermark', '')
                                table["ending_watermark_value"] = table.pop('last_merged_watermark', '')
                                if math.isnan(table.get('target_records_count')):
                                    table['pre_target_count'] = None
                                    table['target_records_count'] = None
                                else:
                                    if table["job_status"] == "FAILED":
                                        table['pre_target_count'] = table.get('target_records_count')
                                        table['target_records_count'] = int(table.get('target_records_count'))
                                    else:
                                        table['pre_target_count'] = int(
                                            table.get('target_records_count') - int(table.get('fetch_records_count')))
                                        table['target_records_count'] = int(table.get('target_records_count'))
                                if table.get('job_type') == "CDC":
                                    table['job_type'] = "INCREMENTAL"
                                # table['job_start_time'] = table['job_start_time'].split('.')[0].replace('T', ' ')
                                # table['job_end_time'] = table['job_end_time'].split('.')[0].replace('T', ' ')
                                table['fetch_records_count'] = int(table['fetch_records_count'])

                                # self.iwx_client.logger.info(f"Ingestion Table Data: {json.dumps(table)}")
                                # Metrics in Consideration from Ingestion Data
                                for key in ["job_type", "source_schema_name", "source_database_name",
                                            "source_file_names", "entity_type", "starting_watermark_value",
                                            "ending_watermark_value",
                                            "target_records_count", "pre_target_count", "fetch_records_count"]:
                                    source_job_row_template[key] = table.get(key, '')

                            # Table not in Ingestion Metrics
                            else:
                                sync_type = table_info.get('configuration', {}).get('sync_type', '')
                                source_job_row_template['job_type'] = sync_type.upper()
                                source_job_row_template["pre_target_count"] = table_row_count
                                source_job_row_template["target_records_count"] = table_row_count

                        if job_type == "export_data":
                            if export_metrics is not None:
                                if table_id in export_metrics_tables_list:
                                    filter1 = df_export["table_id"] == table_id
                                    table = df_export.loc[filter1].to_dict('records')[0]
                                    table["job_type"] = "EXPORT"
                                    table_export_config = self.get_table_export_info(table.get('source_id'), table_id)
                                    if table_export_config.get("target_type", "") == "BIGQUERY":
                                        table["target_schema_name"] = ".".join(
                                            [table_export_config.get("connection", {}).get("project_id", ""),
                                             table_export_config.get("target_configuration", {}).get("dataset_name",
                                                                                                     "")])
                                    else:
                                        table["target_schema_name"] = ".".join(
                                            [table_export_config.get("target_configuration", {}).get("schema_name", ""),
                                             table_export_config.get("target_configuration", {}).get("database_name",
                                                                                                     "")])
                                    table["target_table_name"] = table_export_config.get("target_configuration",
                                                                                         {}).get(
                                        "table_name", "")
                                    table["workflow_id"] = table.get("workflow_id", "")
                                    table["workflow_run_id"] = table.get("workflow_run_id", "")
                                    table["entity_type"] = entity_type
                                    table["starting_watermark_value"] = table.pop('first_merged_watermark', '')
                                    table["ending_watermark_value"] = table.pop('last_merged_watermark', '')
                                    if math.isnan(table.get('target_records_count')):
                                        table['pre_target_count'] = None
                                        table['target_records_count'] = None
                                    else:
                                        if table["job_status"] == "FAILED":
                                            table['pre_target_count'] = table.get('target_records_count')
                                            table['target_records_count'] = int(table.get('target_records_count'))
                                        else:
                                            table['pre_target_count'] = int(
                                                table.get('target_records_count') - int(
                                                    table.get('number_of_records_written')))
                                            table['target_records_count'] = int(table.get('target_records_count'))
                                    od = OrderedDict()
                                    # table['job_start_time'] = table['job_start_time'].split('.')[0].replace('T', ' ')
                                    # table['job_end_time'] = table['job_end_time'].split('.')[0].replace('T', ' ')
                                    table['fetch_records_count'] = int(table['number_of_records_written'])

                                    for key in ["job_type", "target_schema_name",
                                                "target_table_name", "entity_type", "starting_watermark_value",
                                                "ending_watermark_value", "target_records_count", "pre_target_count",
                                                "fetch_records_count"]:
                                        source_job_row_template[key] = table.get(key, "")
                                else:
                                    source_job_row_template['job_type'] = "EXPORT"
                                    source_job_row_template['pre_target_count'] = table_row_count
                                    source_job_row_template['target_records_count'] = table_row_count
                                    source_job_row_template['entity_type'] = entity_type

                        # self.iwx_client.logger.info(f"Source Row Template: {json.dumps(source_job_row_template)}")
                        job_od = OrderedDict()
                        for key in source_job_row_template.keys():
                            job_od[key] = source_job_row_template.get(key, "")
                        self.job_metrics_final.append(job_od)
                else:
                    job_od = OrderedDict()
                    for key in source_job_row_template.keys():
                        job_od[key] = source_job_row_template.get(key, "")
                    self.job_metrics_final.append(job_od)
        except Exception as e:
            print(str(e))
            traceback.print_exc()

    def get_pipeline_build_metrics_results(self, job=None, workflow_id=None, workflow_run_id=None):
        """
        Gets the Infoworks pipeline build metrics
        :param job: job object to get the pipeline build metrics details
        :type job: JSON Object
        :param workflow_id: Workflow id to get the jobs
        :type workflow_id: String
        :param workflow_run_id: Workflow run id to get the jobs
        :type workflow_run_id: String
        :return: response dict
        """
        if job is None:
            self.iwx_client.logger.error("job is a mandatory parameter")
            raise Exception("job is a mandatory parameter")
        temp = []
        job_id = job['id']
        job_type = job['type']
        job_status = job['status']
        job_cluster_id = job.get('cluster_id')
        job_cluster_name = job.get('cluster_name')
        databricks_job_id = ''
        job_createdAt = job['created_at']
        job_start_time = job['build_started_at'].split('.')[0].replace('T', ' ')
        job_end_time = job['build_ended_at'].split('.')[0].replace('T', ' ')
        entity_type = job['entity_type']
        job_created_by = job.get('created_by')
        running_job_template = {
            'workflow_id': job.get('triggered_by', {}).get('parent_id', ''),
            'workflow_run_id': job.get('triggered_by', {}).get('run_id', ''),
            'job_id': job_id,
            'job_type': job_type.upper(),
            'job_start_time': job_start_time,
            'job_end_time': job_end_time,
            'job_created_by': job_created_by,
            'cluster_id': job_cluster_id,
            'cluster_name': job_cluster_name,
            "databricks_job_id": "",
            "source_file_names": [],
            'job_status': job_status.upper(),
            'entity_type': entity_type,
            "source_name": "", "source_schema_name": job.get("source_schema_name", ""),
            "source_database_name": job.get("source_database_name", ""), "table_group_name": "", "iwx_table_name": "",
            'starting_watermark_value': '', 'ending_watermark_value': '',
            "pre_target_count": "", "fetch_records_count": 0,
            "target_records_count": ""}
        # Fetches Cluster Run Info (i.e. Pipeline Data in a Job)
        cluster_data = self.get_cluster_runs_of_job(job_id)
        if cluster_data:
            # Job Start Time and End Time for no Pipeline Metrics
            running_job_template['job_start_time'] = cluster_data[0]['started_at'].split('.')[0].replace('T', ' ')
            running_job_template['job_end_time'] = cluster_data[0]['ended_at'].split('.')[0].replace('T', ' ')
            running_job_template['cluster_id'] = cluster_data[0]['cluster_id']
            running_job_template['cluster_name'] = cluster_data[0]['cluster_name']
            job_cluster_id = cluster_data[0]['cluster_id']
            job_cluster_name = cluster_data[0]['cluster_name']
            databricks_job_id = ""
            if cluster_data[0].get('application_type', '').lower() == "databricks":
                databricks_job_id = cluster_data[0].get('application_run_id', '')
            running_job_template['databricks_job_id'] = databricks_job_id

        running_od = OrderedDict()
        if job_status.upper() in ["RUNNING", "PENDING", "CANCELED", "ABORTED", "FAILED"]:
            for key in ['workflow_id', 'workflow_run_id', 'job_id', 'entity_type', 'job_type', 'job_start_time',
                        'job_end_time', 'job_created_by', 'cluster_id', 'cluster_name', 'databricks_job_id',
                        'job_status',
                        'source_name', 'source_file_names', 'source_schema_name', 'source_database_name',
                        'table_group_name',
                        'iwx_table_name', 'starting_watermark_value', 'ending_watermark_value', 'target_schema_name',
                        'target_table_name', 'pre_target_count', 'fetch_records_count',
                        'target_records_count']:
                running_od[key] = running_job_template.get(key, "")
            self.job_metrics_final.append(running_od)
            return
        pipeline_metrics = self.get_pipeline_build_metrics(str(job_id))
        # For SQL Pipeline successful jobs (No Pipeline Metrics)
        if job_status.upper() == "COMPLETED" and len(pipeline_metrics) == 0:
            for key in ['workflow_id', 'workflow_run_id', 'job_id', 'entity_type', 'job_type', 'job_start_time',
                        'job_end_time', 'job_created_by', 'cluster_id', 'cluster_name', 'databricks_job_id',
                        'job_status',
                        'source_name', 'source_file_names', 'source_schema_name', 'source_database_name',
                        'table_group_name',
                        'iwx_table_name', 'starting_watermark_value', 'ending_watermark_value', 'target_schema_name',
                        'target_table_name', 'pre_target_count', 'fetch_records_count',
                        'target_records_count']:
                running_od[key] = running_job_template.get(key, "")
            self.job_metrics_final.append(running_od)
            return
        pipeline_successful_tables = []
        if pipeline_metrics is not None:
            tables_list = list(set([i['target_table_name'] for i in pipeline_metrics]))
            df_pipeline = pd.DataFrame(pipeline_metrics)
            for target_table_name in tables_list:
                schema_name, table_name = target_table_name.split(".")
                pipeline_successful_tables.append(table_name)
                filter1 = df_pipeline["target_table_name"] == target_table_name
                table = df_pipeline.loc[filter1].to_dict('records')[0]
                table["job_type"] = "PIPELINE_BUILD"
                table["source_name"] = ''
                table["source_file_names"] = []
                table["source_schema_name"] = ""
                table["source_database_name"] = ""
                table["table_group_name"] = ''
                table["iwx_table_name"] = ''
                table['target_schema_name'] = schema_name
                table['target_table_name'] = table_name
                table["workflow_id"] = table.get('workflow_id', '')
                table["workflow_run_id"] = table.get('workflow_run_id', '')
                if workflow_id is not None and workflow_run_id is not None:
                    if not (table["workflow_id"] == workflow_id and table["workflow_run_id"] == workflow_run_id):
                        continue
                table["entity_type"] = job.get("entity_type", "pipeline")
                table["starting_watermark_value"] = table.pop('first_merged_watermark', '')
                table["ending_watermark_value"] = table.pop('last_merged_watermark', '')
                table["target_records_count"] = table.get("target_records_count", None)
                table["job_status"] = table.get("job_status", "")
                table["cluster_id"] = job_cluster_id
                table["cluster_name"] = job_cluster_name
                table['databricks_job_id'] = databricks_job_id
                table['job_created_by'] = job_created_by
                if math.isnan(table.get('target_records_count')):
                    table['pre_target_count'] = None
                    table['target_records_count'] = None
                else:
                    if table["job_status"] == "FAILED":
                        table['pre_target_count'] = table.get('target_records_count')
                        table['target_records_count'] = int(table.get('target_records_count'))
                    else:
                        table['pre_target_count'] = int(
                            table.get('target_records_count') - int(table.get('number_of_records_written')))
                        table['target_records_count'] = int(table.get('target_records_count'))
                od = OrderedDict()
                table['job_start_time'] = table['job_start_time'].split('.')[0].replace('T', ' ')
                table['job_end_time'] = table['job_end_time'].split('.')[0].replace('T', ' ')
                table['fetch_records_count'] = int(table['number_of_records_written'])
                for key in ['workflow_id', 'workflow_run_id', 'job_id', 'entity_type', 'job_type', 'job_start_time',
                            'job_end_time', 'job_created_by', 'cluster_id', 'cluster_name', 'databricks_job_id',
                            'job_status',
                            'source_name', 'source_file_names', 'source_schema_name', 'source_database_name',
                            'table_group_name',
                            'iwx_table_name', 'starting_watermark_value', 'ending_watermark_value',
                            'target_schema_name', 'target_table_name', 'pre_target_count', 'fetch_records_count',
                            'target_records_count']:
                    od[key] = table.get(key, "")
                temp.append(od)
            self.job_metrics_final.extend(temp)

    def get_abc_job_metrics(self, time_range_for_jobs_in_mins=5, workflow_id=None, workflow_run_id=None):
        """
        Gets the Infoworks Job metrics
        :param time_range_for_jobs_in_mins: time range to look out for Infoworks jobs (default 5mins)
        :type time_range_for_jobs_in_mins: Integer
        :param workflow_id: Workflow id to get the jobs
        :type workflow_id: String
        :param workflow_run_id: Workflow run id to get the jobs
        :type workflow_run_id: String
        :return: response dict
        """
        try:
            sources_info = self.get_source_info()
            delay = int(time_range_for_jobs_in_mins)
            now = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=delay)
            date_string = now.strftime('%Y-%m-%dT%H:%M:%SZ')
            source_jobs = self.get_all_source_jobs(date_string)
            source_jobs_source_ids = list(set([i['entity_id'] for i in source_jobs]))
            sources_info = [i for i in sources_info if i['id'] in source_jobs_source_ids]
            with ThreadPoolExecutor(max_workers=10) as executor:
                executor.map(self.get_source_jobs_metrics_results_table_level,
                             [date_string] * len(sources_info), sources_info,
                             [workflow_id, workflow_run_id] * len(sources_info))
                executor.shutdown(wait=True)
            pipeline_jobs_list = self.get_pipeline_jobs(date_string)
            with ThreadPoolExecutor(max_workers=10) as executor:
                executor.map(self.get_pipeline_build_metrics_results, pipeline_jobs_list,
                             [workflow_id, workflow_run_id] * len(pipeline_jobs_list))
                executor.shutdown(wait=True)

            workflow_jobs_list = self.get_workflow_jobs(date_string)
            with ThreadPoolExecutor(max_workers=10) as executor:
                executor.map(self.get_workflow_job_metrics, workflow_jobs_list,
                             [workflow_id, workflow_run_id] * len(workflow_jobs_list))
                executor.shutdown(wait=True)
            # print(f"All Workflows without Jobs : {json.dumps(workflow_jobs_list)}")
            result = []
            header = ['workflow_id', 'workflow_run_id', 'job_id', 'entity_type', 'job_type', 'job_start_time',
                      'job_end_time', 'job_created_by', 'cluster_id', 'cluster_name', 'databricks_job_id', 'job_status',
                      'job_table_status', 'source_name',
                      'source_file_names', 'source_schema_name',
                      'source_database_name', 'table_group_name', 'source_table_name', 'iwx_table_name', 'starting_watermark_value',
                      'ending_watermark_value', 'target_schema_name', 'target_table_name', 'pre_target_count',
                      'fetch_records_count', 'target_records_count', 'job_link']
            ui_port = 443 if self.iwx_client.client_config["port"] == '443' else 3000
            if len(self.job_metrics_final) > 0:
                for item in self.job_metrics_final:
                    dict_temp = {}
                    for i in header:
                        dict_temp[i] = item.get(i)
                        if i == "job_link" and dict_temp.get("job_id", None) != None:
                            dict_temp[
                                i] = f"{self.iwx_client.client_config['protocol']}://{self.iwx_client.client_config['ip']}:{ui_port}/job/logs?jobId={dict_temp.get('job_id', '')}"
                    result.append(copy.deepcopy(dict_temp))
                return result
            else:
                print("Job list empty!!!")
        except Exception as e:
            print("Something went wrong")
            print(str(e))
            traceback.print_exc()
