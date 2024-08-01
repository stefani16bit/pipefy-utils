import requests as req
import json
import sleep
import re

num_reconnection_attempts = 0
timeout_connection = 0

class PipefyException(Exception):
    pass

class Pipefy(object):

    def __init__(self, token):
        self.token = token if 'Bearer' in token else 'Bearer %s' % token
        self.headers = {'Content-Type': 'application/json', 'Authorization': self.token}
        self.endpoint = 'https://app.pipefy.com/graphql'
        self.numReconnectionAttempts = num_reconnection_attempts
        self.timeoutConnection = timeout_connection

    def request(self, query, headers={}):

        for i in range(self.numReconnectionAttempts):
            try:
                _headers = self.headers
                _headers.update(headers)
                response = req.post(
                    self.endpoint,
                    json={ "query": query },
                    headers=_headers,
                    verify = False
                )
                try:
                    status_code = response.status_code
                    response = json.loads(response.text)
                except ValueError:
                    print("response:", response.text)
                    raise PipefyException(response.text)
                
                if response.get('error'):
                    print("response:", response.get('error'))
                    raise PipefyException(response.get('erro_description', response.get('error')))
                
                if response.get('errors'):
                    for error in response.get('errors'):
                        print("response:", response.get('message'))
                        raise PipefyException(error.get('message'))
                    
                if status_code != req.codes.ok:
                    print("response:", response.get('error'))
                    raise PipefyException(response.get('error_description', response.get('error')))    
                
                if "DOCTYPE html" in response:
                    print("response:", response)
                    raise PipefyException("Error HTTP 429 - Too Many Requests")

                return response
            
            except Exception as e:
                print("Attempt:", i, "Error Message:", e)
                print("Waiting ", self.timeoutConnection, "seconds to reconnect")
                sleep(self.timeoutConnection)
                if i > self.numReconnectionAttempts:
                    raise e
                
    def __prepare_json_dict(self, data_dict):
        data_response = json.dumps(data_dict)
        rex = re.compile(r'"(\S+)":')
        for field in rex.findall(data_response):
            data_response = data_response.replace('"%s"' % field, field)
        return data_response                

#####-------------------PIPEFY QUERIES-------------------#####
# https://www.pipefy.com/developers/docs/api/graphql/queries/
  
    def card(self, id, response_fields=None, headers={}):
        response_fields = response_fields
        query = '{ card(id: %(id)s) { %(response_fields)s}}' % {
            'id': json.dumps(id),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('card', [])

    def phase(self, id, count=10, search={}, response_fields=None, response_card_fields=None, headers={}):
        response_fields = response_fields or 'id name cards_count'
        response_card_fields = response_card_fields or 'edges { node { id title assignees { id name email}' \
                                                        ' comments { text } comments_count current_phase { id name } createdAt done due_date ' \
                                                        'fields { field{id type} name value array_value} labels { id name } phases_history { phase { id name } firstTimeIn lastTimeOut } url } }'
        
        query = '{ phase(id: %(phase_id)s ) { %(response_fields)s cards(first: %(count)s, search: %(search)s) { %(response_card_fields)s } } }' % {
            'phase_id': json.dumps(id),
            'count': json.dumps(count),
            'search': self.__prepare_json_dict(search),
            'response_fields': response_fields,
            'response_card_fields': response_card_fields
        }

        return self.request(query, headers).get('data', {}).get('phase')

#####-------------------PIPEFY MUTATIONS-------------------#####
# https://www.pipefy.com/developers/docs/api/graphql/mutations/

    def move_card_to_phase(self, card_id, destination_phase_id, response_fields = None, headers={}):
        response_fields = response_fields or 'card { id current_phase { name}}'

        query = '''
            mutation {
                moveCardToPhase(input: {
                    card_id: %(card_id)s
                    destination_phase_id: %(destination_phase_id)s
                }) {
                    %(response_fields)s
                }
            }
        ''' % {
            'card_id': json.dumps(card_id),
            'destination_phase_id': json.dumps(destination_phase_id),
            'response_fields': response_fields
        }   
        return self.request(query, headers).get('data', {}).get('moveCardToPhase', {}).get('card')
    