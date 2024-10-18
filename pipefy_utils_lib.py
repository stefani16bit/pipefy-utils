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

    def get_all_cards(self, pipe_id, filter="", response_fields=None, headers={}):
        response_fields = response_fields or 'edges { node { id title assignees { id }' \
                ' comments { text } comments_count current_phase { name } done due_date ' \
                'fields { name value } labels { name } phases_history { phase { name } firstTimeIn lastTimeOut } url } }'
        query = '{ allCards(pipeId: %(pipe_id)s, filter: %(filter)s) { %(response_fields)s } }' % {
            'pipe_id': json.dumps(pipe_id),
            'filter': filter,
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('allCards', [])

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
    
    def pipe(self, id, response_fields=None, headers={}):
        response_fields = response_fields or 'id name start_form_fields { label id }' \
                                                    ' labels { name id } phases { name fields { label id }' \
                                                    ' cards(first: 5) { edges { node { id, title } } } }'
        query = '{ pipe (id: %(id)s) { %(response_fields)s } }' % {
            'id': json.dumps(id),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('pipe', [])

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

    def update_phase_field(self, id, label, options, required, editable, response_fields=None, headers={}):
        response_fields = response_fields or 'phase_field { id label }'
        query = '''
            mutation {
              updatePhaseField(
                input: {
                  id: %(id)s
                  label: %(label)s
                  options: %(options)s
                  required: %(required)s
                  editable: %(editable)s
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'id': json.dumps(id),
            'label': json.dumps(label),
            'options': self.__prepare_json_list(options),
            'required': json.dumps(required),
            'editable': json.dumps(editable),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('updatePhaseField', {}).get('phase_field')
    
    def update_card(self, id, title=None, due_date=None, assignee_ids=[], label_ids=[], response_fields=None, headers={}):
        response_fields = response_fields or 'card { id title }'
        query = '''
            mutation {
              updateCard(
                input: {
                  id: %(id)s
                  title: %(title)s
                  due_date: %(due_date)s
                  assignee_ids: [ %(assignee_ids)s ]
                  label_ids: [ %(label_ids)s ]
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'id': json.dumps(id),
            'title': json.dumps(title),
            'due_date': due_date.strftime('%Y-%m-%dT%H:%M:%S+00:00') if due_date else json.dumps(due_date),
            'assignee_ids': ', '.join([json.dumps(id) for id in assignee_ids]),
            'label_ids': ', '.join([json.dumps(id) for id in label_ids]),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('updateCard', {}).get('card')
    
    def update_card_field(self, card_id, field_id, new_value, response_fields=None, headers={}):
        response_fields = response_fields or 'card{ id }'
        query = '''
            mutation {
              updateCardField(
                input: {
                  card_id: %(card_id)s
                  field_id: %(field_id)s
                  new_value: %(new_value)s
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'card_id': json.dumps(card_id),
            'field_id': json.dumps(field_id),
            'new_value': json.dumps(new_value),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('updateCardField', {}).get('card')

    def delete_card(self, id, response_fields=None, headers={}):
        response_fields = response_fields or 'success'
        query = 'mutation { deleteCard(input: { id: %(id)s }) { %(response_fields)s }' % {
            'id': json.dumps(id),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('deleteCard', {})

    def create_comment(self, card_id, text, response_fields=None, headers={}):
        response_fields = response_fields or 'comment { id text }'
        query = '''
            mutation {
              createComment(
                input: {
                  card_id: %(card_id)s
                  text: %(text)s
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'card_id': json.dumps(card_id),
            'text': json.dumps(text),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('createComment', {}).get('comment')
    