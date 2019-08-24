import os
import json
import datetime
import copy
import pytz
import time
import logging

from lib.helper.models.format import Format
from lib.helper.models.job import Job
from lib.helper.models.spec import Spec


def time_to_iso(millis):
    iso_format = '%Y-%m-%dT%H:%M:%S%z'
    return datetime.datetime.utcfromtimestamp(millis/1000.0).replace(tzinfo=pytz.utc).strftime(iso_format)

class Data:

    def __init__(self):
        pass

    def create_datastream(self, service, test_case):
        ds_test_config = test_case['create_datastream']
        datastream_config = {}
        datastream_config['name'] =ds_test_config['datastream_name']
        if test_case['batch_or_sliding']=='batch':datastream_config['isBatch']=True
        else:datastream_config['isBatch']=False

        #  creating datastream
        active_datastream = service.create_datastream(datastream_config)
        #  ingesting source data
        for file_name in ds_test_config['source_files']:

            # create job
            job = Job()
            format = Format()
            format.set_entityIdentifier(ds_test_config['entity_field'])
            format.set_timeFormat(ds_test_config['time_format'])
            format.set_timeIdentifier(ds_test_config['time_field']) 
            if 'batch_identifier' in ds_test_config:
                format.set_batchIdentifier(ds_test_config['batch_identifier'])
            if ds_test_config['data_format'] == 'narrow':
                format.set_isWide(False)
                format.set_signalIdentifier(ds_test_config['signal_identifier'])
                format.set_valueIdentifier(ds_test_config['value_identifier'])
            else:
                format.set_isWide(True)
                format.set_entityIdentifier(ds_test_config['entity_field'])
            spec = Spec()
            spec.set_format(format.raw)
            job.set_spec(spec.raw)
            job.set_jobType("INGESTDATA")
            job.set_status("CREATED")
            job.set_datastream(active_datastream.get_id())
            job=service.create_job(job.raw)

            # ingesting data
            ingestion_status = service.add_data(active_datastream.get_id(),job.get_id(),os.getcwd() + '/../resources/data/' + test_case['case_name'] + '/' + file_name)
            if ingestion_status['status'] == 'FAILED':
                raise Exception('Data ingestion failed: ' + json.dumps(ingestion_status))
            if ingestion_status['status'] == 'PENDING':
                raise Exception('Data ingestion taking too long to finish')

                            # complete job
            job.set_jobType("INGESTDATA")
            if len(ingestion_status['errors'])!=0 or ingestion_status['status'] != 'COMPLETED':
                job.set_status("FAILED")
                job_status = service.update_job(job.get_id(),job)
            else:
                job.set_status("COMPLETED")
                job_status = service.update_job(job.get_id(),job)

        active_assessment = service.get_assessment_from_datastream(active_datastream.get_id())

        if len(active_assessment) == 0:
            raise Exception('No assessment found for datastream '+active_datastream.get_id())
        active_assessment = active_assessment[0]
        #  ingesting fact data
        for tag in ds_test_config['fact_files']:
            # create job
            job = Job()
            format = Format()
            format.set_entityIdentifier("entity")
            format.set_timeIdentifier("start")
            format.set_timeFormat(ds_test_config['time_format'])
            format.set_timeZone("Etc/GMT0")
            format.set_endIdentifier("end")
            format.set_conditionIdentifier("value")
            format.set_tagIdentifier(tag)
            spec = Spec()
            spec.set_format(format.raw)
            job.set_spec(spec.raw)
            job.set_status("CREATED")
            job.set_datastream(active_datastream.get_id())
            job.set_assessment(active_assessment["id"])
            job.set_jobType("INGESTEVENTS")
            job = service.create_job(job.raw)

            # ingesting fact
            ingestion_status = service.add_facts(active_datastream.get_id(),active_assessment['id'],job.get_id(),os.getcwd() + '/../resources/data/' + test_case['case_name'] + '/' +ds_test_config['fact_files'][tag])
            if ingestion_status['status'] == 'FAILED':
                raise Exception('Fact ingestion failed: ' + json.dumps(ingestion_status))
            if ingestion_status['status'] == 'PENDING':
                raise Exception('Fact ingestion taking too long to finish')

            # complete job
            job.set_jobType("INGESTEVENTS")
            if len(ingestion_status['errors']) != 0 or ingestion_status['status'] != 'COMPLETED':
                job.set_status("FAILED")
                job = service.update_job(job.get_id(), job)
            else:
                job.set_status("COMPLETED")
                job = service.update_job(job.get_id(), job)
            print("worked ###")
        return service.get_datastream(active_datastream.get_id())

    def create_assessment(self, service, test_case, assessment_name, active_datastream):
        active_assessment = service.create_assessment({
            'name': assessment_name,
            'datastream': active_datastream.get_id(),
            'assessmentRate': None
        })

        ds_test_config = test_case['create_datastream']
         #  ingesting source data
        logging.info('Uploading fact data for current assessment...')
        for tag in ds_test_config['fact_files']:
            # create job
            job = Job()
            format = Format()
            format.set_entityIdentifier("entity")
            format.set_timeIdentifier("start")
            format.set_timeFormat(ds_test_config['time_format'])
            format.set_timeZone("Etc/GMT0")
            format.set_endIdentifier("end")
            format.set_conditionIdentifier("value")
            format.set_tagIdentifier(tag)
            spec = Spec()
            spec.set_format(format.raw)
            job.set_spec(spec.raw)
            job.set_status("CREATED")
            job.set_datastream(active_datastream.get_id())
            job.set_assessment(active_assessment["id"])
            job.set_jobType("INGESTEVENTS")
            job = service.create_job(job.raw)

            # ingesting fact
            ingestion_status = service.add_facts(active_datastream.get_id(),active_assessment['id'],job.get_id(),os.getcwd() + '/../resources/data/' + test_case['case_name'] + '/' +ds_test_config['fact_files'][tag])
            if ingestion_status['status'] == 'FAILED':
                raise Exception('Fact ingestion failed: ' + json.dumps(ingestion_status))
            if ingestion_status['status'] == 'PENDING':
                raise Exception('Fact ingestion taking too long to finish')

            # complete job
            job.set_jobType("INGESTEVENTS")
            if len(ingestion_status['errors']) != 0 or ingestion_status['status'] != 'COMPLETED':
                job.set_status("FAILED")
                job = service.update_job(job.get_id(), job)
            else:
                job.set_status("COMPLETED")
                job = service.update_job(job.get_id(), job)

        logging.info('Completed uploading fact data for current assessment.')

        return active_assessment.raw

    def create_model(self, service, modelConfig, signals, datastream, assessment, options, index):
        input_config = []
        segment_list = []
        segment_starts = []
        segment_ends = []

        if 'build' in modelConfig:
            options = copy.deepcopy(options)
            options['build'] = modelConfig['build']

        if 'signals' in modelConfig and modelConfig['signals'] is not None and len(modelConfig['signals']) > 0:
            signal_names = [signal['name'].lower() for signal in modelConfig['signals']]
            if modelConfig['window_settings']['type'] == 'Batch':
                signal_names.append(modelConfig['window_settings']['batch_id'].lower())

            for signal in datastream['inputList']:
                sig = signal.raw
                if sig['name'].lower() in signal_names:
                    for loc_sig in signals:
                        if loc_sig['name'].lower() == sig['name'].lower() and 'is_compressed' in loc_sig and loc_sig['is_compressed']:
                            sig['annotations'].append("COMPRESSED")
                    input_config.append(sig)

        elif len(signals) > 0:
            signal_names = [signal['name'].lower() for signal in signals]
            if modelConfig['window_settings']['type'] == 'Batch':
                signal_names.append(modelConfig['window_settings']['batch_id'].lower())

            for signal in datastream['inputList']:
                sig = signal.raw
                if sig['name'].lower() in signal_names:
                    for loc_sig in signals:
                        if loc_sig['name'].lower() == sig['name'].lower() and 'is_compressed' in loc_sig and loc_sig['is_compressed']:
                            sig['annotations'].append("COMPRESSED")
                    input_config.append(sig)

        else:
            for inputObj in datastream['inputList']:
                if inputObj.raw['inputType']['type'] == 'Raw':
                    input_config.append(inputObj.raw)
        for segment in modelConfig['time_segments_to_learn_on']:
            segment_starts.append(segment['start'])
            segment_ends.append(segment['end'])
            segment_list.append({
                'startTime': time_to_iso(segment['start']),
                'endTime': time_to_iso(segment['end']),
            })
        if len(modelConfig['fact_tags_to_use']) > 0:
            batches = self.get_fact_batches(service, datastream, assessment, modelConfig)
            fact_config = {
                'batches': batches,
                'sources': [],
                'tags': modelConfig['fact_tags_to_use'],
                'conditions': [],
                'timeRange': {
                    'startTime': sorted(segment_starts)[0],
                    'endTime': sorted(segment_ends, reverse=True)[0]
                }
            }
            model_config = {
                'entityList': modelConfig['entities_to_learn_on'],
                'segmentList': [],
                'inputList': input_config,
                'clustering': {
                    'lowerBound': modelConfig['other_settings']['cluster'][0],
                    'upperBound': modelConfig['other_settings']['cluster'][1],
                },
                'unknownThreshold': modelConfig['other_settings']['pattern_generalization'],
                'factConfig': fact_config,
                'description': '{"build": "#' + options['build']+'", "description": "'+modelConfig['description']+'"}'
            }
        else:
            model_config = {
                'entityList': modelConfig['entities_to_learn_on'],
                'segmentList': [],
                'inputList': input_config,
                'clustering': {
                    'lowerBound': modelConfig['other_settings']['cluster'][0],
                    'upperBound': modelConfig['other_settings']['cluster'][1],
                },
                'unknownThreshold': modelConfig['other_settings']['pattern_generalization'],
                'description': '{"build": "#' + options['build']+'", "description": "'+modelConfig['description']+'"}'
            }
        model_config['segmentList'] = segment_list
        if modelConfig['window_settings']['type'] == 'Batch':
            model_config['interval'] = {
                'field': modelConfig['window_settings']['batch_id'],
            }
        else:
            model_config['interval'] = {
                'duration': modelConfig['window_settings']['lower_bound'] if 'lower_bound' in modelConfig['window_settings'] else 'PT1S',
                'windowUpperBound': modelConfig['window_settings']['upper_bound'] if 'upper_bound' in modelConfig['window_settings'] else 'P1DT',
                'assessmentRate': modelConfig['window_settings']['assessment_rate'] if 'assessment_rate' in modelConfig['window_settings'] else 'PT0S',
            }

        model = service.create_model(assessment['id'],datastream.get_id(), model_config, options['build'])
        logging.info('Building model #' + str(index) + ', PID: ' + model['pid'].replace('_', '-').lower() + '...')
        time.sleep(2*60)
        if model['status'] == 'FAILED':
            raise Exception('Error starting model')
        model_status = service.check_process_status(model['pid'], model['id'])

        if model_status['state'] == 'FAILED':
            raise Exception('Model creation failed: ' + json.dumps(model_status))
        elif model_status['state'] == 'STAGING':
            raise Exception('Model creation taking too long to start')
        elif model_status['state'] == 'RUNNING':
            raise Exception('Model creation taking too long to finish')

        model['pid'] = model['pid'].replace('_', '-').lower()
        logging.info('Completed building model #' + str(index) + ', PID: ' + model['pid'] + '.')
        return model

    def apply_model(self, service, assessment, applyConfig, model, index):

        model_id = model['id']

        output_summaries = service.get_outputSummary(assessment['id'])
        interesting_output_summary = None
        for o in output_summaries:
            if o['startTime'] <= applyConfig['time_segment']['start'] and o['endTime'] <= applyConfig['time_segment']['end']:
                interesting_output_summary = o
                break
        if interesting_output_summary is None:
            config = {
                'startTime': time_to_iso(applyConfig['time_segment']['start']),
                'endTime': time_to_iso(applyConfig['time_segment']['end']),
                'models': []
            }
        else:
            config = {
                'models': [],
                'outputSummary': interesting_output_summary['id']
            }
        config['models'].append({
            'id': model_id,
            'entities': applyConfig['entities']
        })

        active_apply = service.apply_model(assessment['id'], config)
        current_run = None
        for r in active_apply['runs']:
            if r['model'] == model_id:
                current_run = r
                break
        active_apply['runs'] = [current_run]
        active_apply['pid'] = active_apply['runs'][0]['pid'].replace('_', '-').lower()
        logging.info('Applying model #' + str(index) + ', PID: ' + active_apply['pid'] + '...')
        apply_model_status = service.check_apply_model_status(active_apply['id'])

        logging.info('Completed applying model #' + str(index) + ', PID: ' + active_apply['pid'] + '...')

        # save model and config used for application
        active_apply['model'] = model
        active_apply['config'] = applyConfig

        return active_apply

    def get_fact_batches(self, service, datastream, assessment, modelConfig):
        segment_times = []
        for segment in modelConfig['time_segments_to_learn_on']:
            segment_times.append(segment['start'])
            segment_times.append(segment['end'])
        segment_times.sort()

        config = {
            'name': assessment['factsMeasurement'],
            'datastreamId': datastream['id'],
            'range': {},
            'format': {
                'format': 'Series'
            },
            'snapshotId': assessment['factsMeasurement'],
            'assessmentId': assessment['id'],
            'effective': False,
            'sysTags': ['USER ADDED', 'FILE UPLOAD', 'API UPLOAD'],
            'tags': [],
            'conditions': [],
            'batches': []
        }

        if 'batchIdentifier' in datastream['field'].raw and datastream['field'].raw['batchIdentifier'] is not None:
            config['isBatch'] = True

        config['range'] = {
            'start': segment_times[0],
            'end': segment_times[len(segment_times) - 1]
        }

        batches = service.fact_batch_query(config)
        if len(batches['points']) == 0:
            raise Exception('No fact found for assessment: '+assessment['id'])

        formatted_batches = {}
        for batch in batches['points']:
            formatted_batches[batch[0]] = None

        return formatted_batches
