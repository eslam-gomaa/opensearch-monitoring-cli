import argparse
import sys
import os

from opmcli.node import Node_Monitoring
from opmcli.index import Index_Monitoring
from opmcli.attributes import Attributes
node_monitoring_ = Node_Monitoring()
index_monitoring = Index_Monitoring()


class Cli():        

    def __init__(self):

        # CLI Input attributes
        self.index = None
        self.primaries = False
        self.node = None
        self.list  = False
        self.monitor  = False
        self.top  = False
        self.nodes = False
        self.calc_rates = False
        self.calc_rates_times = 1
        self.calc_rates_interval = 3
        self.calc_rates_seconds = 30
        self.watch = False
        self.display_shards = False

        Attributes.monitoring_interval_seconds = 30

        # Read environment variables
        self.read_env()
        # Read CLI arguments
        self.argparse()
            
        if self.calc_rates:
            node_monitoring_.print_calculate_indexing_rate(self.index, self.calc_rates_seconds, self.calc_rates_times, self.calc_rates_interval)

        # if self.nodes:
            # self.node_top('QTRXxiqmQ4-cUyBpiAB0vQ')

        if (self.list) and (self.index) and not (self.display_shards):
            node_monitoring_.print_indices_table(self.index)

        if (self.list) and (self.index) and (self.display_shards):
            index_monitoring.print_shards_nodes_allocations(self.index)

        if (self.list and self.nodes):
            node_monitoring_.node_list_table(watch=self.watch, interval=self.calc_rates_interval)
            exit(0)

        if  (self.top) and (self.node):
            node_monitoring_.node_monitor(self.node)

        if  (self.top) and (self.index):
            index_monitoring.index_monitor(index_pattern=self.index, primaries=self.primaries)

        # 

    
    def read_env(self):
        """
        Read credentials from Environment variables
            - OPENSEARCH_ENDPOINT
            - OPENSEARCH_PORT
            - OPENSEARCH_USERNAME
            - OPENSEARCH_PASSWORD
            - OPENSEARCH_BASIC_AUTH [yes, no]
            - OPENSEARCH_VERIFY_CERTS
            - OPENSEARCH_USE_SSL
        """

        # Mandatory ENV
        try:
            Attributes.env_opensearch_endpoint  = os.environ['OPENSEARCH_ENDPOINT']
        except (KeyError) as e:
            raise SystemExit(f"\nERROR -- ENV not found => {e}")

        # Optional ENV
        try:
            Attributes.env_opensearch_port  = os.environ['OPENSEARCH_PORT']
            Attributes.env_opensearch_basic_auth  = os.environ['OPENSEARCH_BASIC_AUTH']
            Attributes.env_opensearch_username  = os.environ['OPENSEARCH_USERNAME']
            Attributes.env_opensearch_password  = os.environ['OPENSEARCH_PASSWORD']
            Attributes.env_opensearch_verify_certs  = os.environ['OPENSEARCH_VERIFY_CERTS']
        except (KeyError) as e:
            pass

        if Attributes.env_opensearch_basic_auth == 'yes':
            Attributes.env_opensearch_basic_auth = True
        elif Attributes.env_opensearch_basic_auth == 'no':
            Attributes.env_opensearch_basic_auth = False
        else:
            raise SystemExit("INFO -- OPENSEARCH_BASIC_AUTH > allowed options are: 'yes' || 'no'")

        if Attributes.env_opensearch_basic_auth:
            if (Attributes.env_opensearch_username is None or Attributes.env_opensearch_password is None):
                raise SystemExit("INFO -- ENV: OPENSEARCH_USERNAME or OPENSEARCH_PASSWORD is missing")

        

    def argparse(self):
        parser = argparse.ArgumentParser(description='A Python tool to list Elasticserach indices & calculate indexing & searching rates')
        parser.add_argument('-i', '--index', type=str, required=False, metavar='', help='The Index name / pattern')
        parser.add_argument('-l', '--list', action='store_true', help='List index / indices')
        parser.add_argument('-c', '--calc-rates', action='store_true', help='Calculates indexing and searching rates')
        parser.add_argument('-T', '--times', type=int, required=False, metavar='', help='How many times to calculate rates; default: 1')
        parser.add_argument('-I', '--interval', type=int, required=False, metavar='', help='Interval (seconds) to wait between rates calculations; default: 3')
        parser.add_argument('-S', '--seconds', type=int, required=False, metavar='', help='seconds to wait while calculating rates; seconds: 30')
        # Nodes args 
        parser.add_argument('-n', '--nodes', action='store_true', help='get nodes status')
        # parser.add_argument('-m', '--monitor', action='store_true', help='Live monitoring for Node metrics')

        parser.add_argument('-t', '--top', action='store_true', help='Live monitoring for Node / Index')
        parser.add_argument('-N', '--node', type=str, required=False, metavar='', help='specify Node ID')
        parser.add_argument('-w', '--watch', action='store_true', help='show live results')

        parser.add_argument('-p', '--prim', action='store_true', help='Monitor only Primary shards (for --top --index)')

        parser.add_argument('-d', '--display-shards', action='store_true', help='display shards allocation on Nodes (for --list --index)')



        results = parser.parse_args()

        if results.index:
            self.index = results.index

        if results.prim:
            self.primaries = results.prim

        if results.node:
            self.node = results.node

        if results.index:
            self.index = results.index

        if results.calc_rates:
            self.calc_rates = results.calc_rates
        
        if results.times:
            self.calc_rates_times = results.times
        
        if results.interval:
            self.calc_rates_interval = results.interval

        if results.seconds:
            self.calc_rates_seconds = results.seconds

        if results.list:
            self.list = results.list

        if (results.list) and (results.calc_rates):
            print("\n> can NOT use '--list' AND '--calc-rates' args together")
            print("\t\t\t* * *\n")
            print(parser.print_help(sys.stderr))
            exit(1)

        if results.nodes:
            self.nodes = results.nodes

        if results.watch:
            self.watch = results.watch

        if results.top:
            self.top = results.top

        if results.display_shards:
            self.display_shards = results.display_shards


    
cli = Cli()

def run():
    cli = Cli()