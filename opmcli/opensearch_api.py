import re
from opensearchpy import OpenSearch
import opensearchpy
import urllib3
from opmcli.attributes import Attributes
from rich import print as rich_print
import requests
import json
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
# https://stackoverflow.com/a/41041028
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += ':HIGH:!DH:!aNULL'
import socket


class Opensearch_Python():

    # Opensearch client object
    es_client = None

    def __init__(self):
        pass

    def authenticate(self):
        """
        # https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html
        Connects to opensearch domain
        """
        if Opensearch_Python.es_client is None:
            try:
                if Attributes.env_opensearch_basic_auth:
                    Opensearch_Python.es_client = OpenSearch(hosts = [{'host': Attributes.env_opensearch_endpoint, 'port': Attributes.env_opensearch_port}], 
                                                http_auth = (Attributes.env_opensearch_username, Attributes.env_opensearch_password),
                                                use_ssl = Attributes.env_opensearch_use_ssl,
                                                verify_certs = Attributes.env_opensearch_verify_certs,
                                                ssl_assert_hostname = False,
                                                ssl_show_warn = False)
                    

                else:
                    Opensearch_Python.es_client = OpenSearch(hosts = [{'host': Attributes.env_opensearch_endpoint, 'port': Attributes.env_opensearch_port}], 
                                                use_ssl = Attributes.env_opensearch_use_ssl,
                                                verify_certs = Attributes.env_opensearch_verify_certs,
                                                ssl_assert_hostname = False,
                                                ssl_show_warn = False)
                if Attributes.debug:
                    rich_print("[bold green]INFO --[/bold green] [green]Connected to openSearch.\n")
            except (opensearchpy.exceptions.ConnectionError,
                    urllib3.exceptions.NewConnectionError,
                    opensearchpy.exceptions.AuthenticationException,
                    opensearchpy.exceptions.AuthorizationException) as e:
                raise SystemExit(f"ERROR -- Authentication failed\n> {e}")
            

    def get_info(self):
        try:
            if Opensearch_Python.es_client is None:
                self.authenticate()
            return Opensearch_Python.es_client.info()
        except (opensearchpy.exceptions.NotFoundError,
                opensearchpy.exceptions.ConnectionError,
                opensearchpy.exceptions.AuthenticationException,
                opensearchpy.exceptions.AuthorizationException,
                opensearchpy.exceptions.ConnectionTimeout) as e:
            raise SystemExit(f"ERROR -- (get_info) unable to get index info\n> {e}")


    def get_index(self, index_pattern):
        """
        Returns a (List) of indecies that match the index pattern
        """
        try:
            if Opensearch_Python.es_client is None:
                self.authenticate()
            indices_dct = Opensearch_Python.es_client.indices.get_alias(index_pattern)
            # If the dct is empty return empty list
            if not bool(indices_dct):
                return []
            # Add the dct keys to a list
            indices_lst = []
            for i in indices_dct.keys():
                indices_lst.append(i)
            return indices_lst
        except (opensearchpy.exceptions.NotFoundError,
                opensearchpy.exceptions.ConnectionError,
                opensearchpy.exceptions.AuthenticationException,
                opensearchpy.exceptions.AuthorizationException) as e:
            raise SystemExit(f"ERROR -- (get_index) Unable to get index\n> {e}")
    

    def get_index_stats(self, index, exit_on_fail=True):
        """
        Returns the index Stats (Json)
        """
        try:
            if Opensearch_Python.es_client is None:
                self.authenticate()

            return Opensearch_Python.es_client.indices.stats(index=index)
        except (opensearchpy.exceptions.NotFoundError,
                opensearchpy.exceptions.ConnectionError,
                opensearchpy.exceptions.AuthenticationException,
                opensearchpy.exceptions.AuthorizationException) as e:
            if exit_on_fail:
                raise SystemExit(f"ERROR -- (get_index_stats) Unable to get index stats\n> {e}")
            else:
                raise KeyError("index not found")

    def get_index_settings(self, index):
        """
        Returns index settings (Json)
        """
        try:
            if Opensearch_Python.es_client is None:
                self.authenticate()

            return Opensearch_Python.es_client.indices.get_settings(index=index)
        except (opensearchpy.exceptions.NotFoundError,
                opensearchpy.exceptions.ConnectionError,
                opensearchpy.exceptions.AuthenticationException,
                opensearchpy.exceptions.AuthorizationException) as e:
            rich_print(f"[bold]ERROR -- (get_index_settings) Index Not found[/bold]\n> [white]{e}")
            exit(1)

    def node_stats(self, node_id=None, metric=None, index_metric=None, params=None, headers=None):
        """
        https://elasticsearch-py.readthedocs.io/en/7.x/api.html#elasticsearch.client.NodesClient.stats
        """
        if Opensearch_Python.es_client is None:
            self.authenticate()
        try:
            stats = Opensearch_Python.es_client.nodes.stats(node_id=node_id, metric=metric, index_metric=index_metric, params=params, headers=headers)
            return stats
        except (KeyboardInterrupt, SystemExit):
            exit(1)
        except (opensearchpy.exceptions.AuthorizationException) as e:
            rich_print(f"[bold]ERROR -- Authorization Error while getting Node stats[/bold]\n> [white]{e}")
            exit(1)
        except (opensearchpy.exceptions.ConnectionError,
                opensearchpy.exceptions.ConnectionTimeout) as e:
                rich_print(f"[bold]ERROR -- Connection Error while getting Node stats[/bold]\n> [white]{e}")
                exit(1)            

    
    def node_info(self, node_id=None, metric=None, params=None, headers=None):
        """
        https://elasticsearch-py.readthedocs.io/en/7.x/api.html#elasticsearch.client.NodesClient.info
        """
        if Opensearch_Python.es_client is None:
            self.authenticate()

        info = Opensearch_Python.es_client.nodes.info(node_id=node_id, metric=metric, params=params, headers=headers)
        return info


    def node_hot_threads(self, node_id='NLKEScpcTGOeYri2dn8Ykg'):
        from rich import print
        import re
        if Opensearch_Python.es_client is None:
            self.authenticate()

        hot_threads = Opensearch_Python.es_client.nodes.hot_threads(node_id=node_id)
        blank_line_regex = r"(?:\r?\n){2,}"
        hot_threads_seperated = re.split(blank_line_regex, hot_threads)
        print(hot_threads_seperated)

    def node_search_shards(self, index):
        if Opensearch_Python.es_client is None:
            self.authenticate()
        
        search_shards = Opensearch_Python.es_client.search_shards(index)
        return search_shards

    def get_index_template(self, index_pattern, template_version):
        if Opensearch_Python.es_client is None:
            self.authenticate()

        if template_version not in [1, 2]:
            print("ERROR -- unknown Inde template version !, only support v1 and v2(composable template)")
            exit(1)

        if template_version == 2:
            index_templates_json = Opensearch_Python.es_client.indices.get_index_template(index_pattern)
        else:
            index_templates_json = Opensearch_Python.es_client.indices.get_template(index_pattern)

        return index_templates_json
        
    def get_ism_policy(self, index_pattern):
        # _opendistro/_ism/explain/index-*
        # https://opendistro.github.io/for-elasticsearch-docs/docs/im/ism/api/#explain-index

        # _plugins/_ism/explain/index-*
        # https://opensearch.org/docs/latest/im-plugin/ism/api/#explain-index

        session = requests.Session()
        session.verify = Attributes.env_opensearch_verify_certs

        session.auth = (Attributes.env_opensearch_username, Attributes.env_opensearch_password)

        # Authenticate
        try:
            url = f"https://{Attributes.env_opensearch_endpoint}:{Attributes.env_opensearch_port}"
            url_path = url + "/" + f"_opendistro/_ism/explain/{index_pattern}"
            req = session.get(url_path)
        except (urllib3.exceptions.ReadTimeoutError, 
                socket.timeout,
                requests.exceptions.SSLError,
                urllib3.exceptions.MaxRetryError,
                requests.exceptions.ConnectionError,
                urllib3.exceptions.ProtocolError,
                ConnectionResetError,
                # ssl.SSLEOFError,
                ) as e:
            print(f"ERROR -- Failed rest api: {url_path}, Timeout\n{e}")
            exit(1)

        if req.status_code == 200:
            index_templates_json = json.loads(req.text)
        elif req.status_code == 400:
            return None
        else:
            print(f"ERROR -- Failed rest api: {url_path}, status_code: {req.status_code}")
            rich_print(req.text)
            exit(1)

        if req.status_code == 200:
            first_item = list(index_templates_json)[0]
            first_item_dct = index_templates_json[first_item]
            # print(first_item_dct.keys())
            if 'policy_id' in first_item_dct.keys():
                return first_item_dct
            else:
                return {}

