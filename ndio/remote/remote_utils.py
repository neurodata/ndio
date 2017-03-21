import requests


class remote_utils:

    def __init__(self,
                 user_token):
        self._user_token = user_token

    def get_url(self, url, token=''):
        """
        Get a response object for a given url.

        Arguments:
            url (str): The url make a get to
            token (str): The authentication token

        Returns:
            obj: The response object
        """
        if (token == ''):
            token = self._user_token

        return requests.get(url,
                            headers={
                                'Authorization': 'Token {}'.format(token)},
                            verify=False,)

    def post_url(self, url, token='', json=None, data=None, headers=None):
        """
        Returns a post resquest object taking in a url, user token, and
        possible json information.

        Arguments:
            url (str): The url to make post to
            token (str): The authentication token
            json (dict): json info to send

        Returns:
            obj: Post request object
        """
        if (token == ''):
            token = self._user_token

        if headers:
            headers.update({'Authorization': 'Token {}'.format(token)})
        else:
            headers = {'Authorization': 'Token {}'.format(token)}

        if json:
            return requests.post(url,
                                 headers=headers,
                                 json=json,
                                 verify=False)
        if data:
            return requests.post(url,
                                 headers=headers,
                                 data=data,
                                 verify=False)

        return requests.post(url,
                             headers=headers,
                             verify=False)

    def delete_url(self, url, token=''):
        """
        Returns a delete resquest object taking in a url and user token.

        Arguments:
            url (str): The url to make post to
            token (str): The authentication token

        Returns:
            obj: Delete request object
        """
        if (token == ''):
            token = self._user_token

        return requests.delete(url,
                               headers={
                                   'Authorization': 'Token {}'.format(token)},
                               verify=False,)
