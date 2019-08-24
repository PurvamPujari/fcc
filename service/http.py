"""
Falkonry Client

Client to access Condition Prediction APIs

:copyright: (c) 2016-2018 by Falkonry Inc.
:license: MIT, see LICENSE for more details.

"""

import json
import requests
import urllib3
import jsonpickle

"""
HttpService:
    Service to make API requests to Falkonry Service
"""

class HttpService:
    def __init__(self, host, token, options):
        """
        constructor
        :param host: host address of Falkonry service
        :param token: Authorization token
        """
        from urllib3.exceptions import InsecureRequestWarning
        urllib3.disable_warnings(InsecureRequestWarning)
        self.host  = host if host is not None else "https://app.falkonry.ai"
        self.token = token if token is not None else ""
        if options is not None and options['header'] is not None:
            self.sourceHeader = options['header']
        else:    
            self.sourceHeader = "python-client"


    def get(self, url):
        """
        To make a GET request to Falkonry API server
        :param url: string
        """

        response = requests.get(
            self.host + url,
            headers={
                'Authorization': 'Bearer ' + self.token,
                'x-falkonry-source':self.sourceHeader
            },
            verify=False
        )
        if response.status_code == 200:
            try:
                return json.loads(response._content.decode('utf-8'))
            except Exception as error:
                return response.content
        elif response.status_code == 401:
            raise Exception(json.dumps({'message':'Unauthorized Access'}))
        else:
            raise Exception(response.content)



    def post(self, url, entity):
        """
        To make a POST request to Falkonry API server
        :param url: string
        :param entity: Instantiated class object
        """

        try:
            if entity is None or entity == "":
                jsonData = ""
            else:
                jsonData = entity.to_json()
        except Exception as e:
            jsonData = jsonpickle.pickler.encode(entity)
        response = requests.post(
            self.host + url,
            jsonData,
            headers={
                "Content-Type": "application/json",
                'Authorization': 'Bearer ' + self.token,
                'x-falkonry-source':self.sourceHeader
            },
            verify=False
        )
        if response.status_code == 201:
            try:
                return json.loads(response._content.decode('utf-8'))
            except Exception as e:
                return json.loads(response.content)
        elif response.status_code == 409:
            try:
                return json.loads(response._content.decode('utf-8'))
            except Exception as e:
                return json.loads(response.content)
        elif response.status_code == 401:
            raise Exception(json.dumps({'message':'Unauthorized Access'}))
        else:
            raise Exception(response.content)

    def postData(self, url, data):
        """
        To make a POST request to Falkonry API server
        :param url: string
        :param entity: Instantiated class object
        """

        response = requests.post(
            self.host + url,
            data,
            headers={
                "Content-Type": "text/plain",
                'Authorization': 'Bearer ' + self.token,
                'x-falkonry-source':self.sourceHeader
            },
            verify=False
        )
        if response.status_code == 202 or response.status_code == 200:
            try:
                return json.loads(response._content.decode('utf-8'))
            except Exception as e:
                return json.loads(response.content)
        elif response.status_code == 401:
            raise Exception(json.dumps({'message':'Unauthorized Access'}))
        else:
            raise Exception(response.content)           

    def put(self, url, entity):
        """
        To make a PUT request to Falkonry API server
        :param url: string
        :param entity: Instantiated class object
        """

        response = requests.put(
            self.host + url,
            entity.to_json(),
            headers={
                "Content-Type": "application/json",
                'Authorization': 'Bearer ' + self.token,
                'x-falkonry-source':self.sourceHeader
            },
            verify=False
        )
        if response.status_code == 200:
            try:
                return json.loads(response._content.decode('utf-8'))
            except Exception as e:
                json.loads(response.content)
        elif response.status_code == 401:
            raise Exception(json.dumps({'message':'Unauthorized Access'}))
        else:
            raise Exception(response.content)

    def fpost(self, url, form_data):
        """
        To make a form-data POST request to Falkonry API server
        :param url: string
        :param form_data: form-data
        """
        response = None

        if 'files' in form_data:
            response = requests.post(
                self.host + url,
                data=form_data['data'] if 'data' in form_data else {},
                files=form_data['files'] if 'files' in form_data else {},
                headers={
                    'Authorization': 'Bearer ' + self.token,
                    'x-falkonry-source':self.sourceHeader
                },
                verify=False
            )
        else:
            response = requests.post(
                self.host + url,
                data=json.dumps(form_data['data'] if 'data' in form_data else {}),
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': 'Bearer ' + self.token,
                    'x-falkonry-source':self.sourceHeader
                },
                verify=False
            )
        if response.status_code == 201 or response.status_code == 202:
            try:
                return json.loads(response._content.decode('utf-8'))
            except Exception as e:
                return json.loads(response.content)
        elif response.status_code == 401:
            raise Exception(json.dumps({'message':'Unauthorized Access'}))
        else:
            raise Exception(response.content)

    def delete(self, url):
        """
        To make a DELETE request to Falkonry API server
        :param url: string
        """
        response = requests.delete(
            self.host + url,
            headers={
              'Authorization': 'Bearer ' + self.token,
              'x-falkonry-source':self.sourceHeader
            },
            verify=False
        )
        if response.status_code == 204:
            return None
        elif response.status_code == 401:
            raise Exception(json.dumps({'message':'Unauthorized Access'}))
        else:
            raise Exception(response.content)

    def upstream(self, url, form_data):
        """
        To make a form-data POST request to Falkonry API server using stream
        :param url: string
        :param form_data: form-data
        """
        response = requests.post(
            self.host + url,
            files=form_data['files'] if 'files' in form_data else {},
            headers={
                'Authorization': 'Bearer ' + self.token,
                'x-falkonry-source':self.sourceHeader
            },
            verify=False
        )
        if response.status_code == 202 or response.status_code == 200:
            try:
                return json.loads(response._content.decode('utf-8'))
            except Exception as e:
                return json.loads(response.content)
        elif response.status_code == 401:
            raise Exception(json.dumps({'message':'Unauthorized Access'}))
        else:
            raise Exception(response.content)

    def downstream(self, url, format):
        """
        To make a GET request to Falkonry API server and return stream
        :param url: string
        """
        headers={'Authorization': 'Bearer '+self.token, 'x-falkonry-source':self.sourceHeader}
        if format is not None:
            headers['accept'] = format
        response = requests.get(self.host + url, stream=True, headers=headers, verify=False)
        if response.status_code == 200 or response.status_code == 202:
            return response
        elif response.status_code == 401:
            raise Exception(json.dumps({'message':'Unauthorized Access'}))
        else:
            raise Exception(json.dumps({'message':str(response.text)}))
