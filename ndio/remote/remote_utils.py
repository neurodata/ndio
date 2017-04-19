import requests


class remote_utils:
    """
    Remote Utilities class with wrappers for request methods.
    """

    def __init__(self,
                 user_token):
        """
        Initializes for remote_utils.

        Arguments:
            user_token (str): Authentication token for user.
        """
        self._user_token = user_token

    def get_url(self, url):
        """
        Get a response object for a given url.

        Arguments:
            url (str): The url make a get to
            token (str): The authentication token

        Returns:
            obj: The response object
        """
        try:
            req = requests.get(url, headers={
                'Authorization': 'Token {}'.format(self._user_token)
            }, verify=False)
            if req.status_code is 403:
                raise ValueError("Access Denied")
            else:
                return req
        except requests.exceptions.ConnectionError as e:
            if str(e) == '403 Client Error: Forbidden':
                raise ValueError('Access Denied')
            else:
                raise e

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

    def ping(self, url, endpoint=''):
        """
        Ping the server to make sure that you can access the base URL.

        Arguments:
            None
        Returns:
            `boolean` Successful access of server (or status code)
        """
        r = self.get_url(url + "/" + endpoint)
        return r.status_code
