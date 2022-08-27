from http import client
from tkinter.tix import INTEGER
import opensearchpy
import time
from tabulate import tabulate
from datetime import datetime
import math
import threading
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.progress import SpinnerColumn, Progress, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn, TimeElapsedColumn
from rich.layout import Layout
from rich.console import Console, Group
from rich.rule import Rule
from rich import print as rich_print
from rich.markdown import Markdown
import pyperclip
from operator import itemgetter

from opmcli.opensearch_api import Opensearch_Python
from opmcli.attributes import Attributes
from opmcli.helper import Helper
helper_ = Helper()

class Bcolors:
    def __init__(self):
        self.HEADER = '\033[95m'
        self.OKBLUE = '\033[94m'
        self.OKGREEN = '\033[92m'
        self.WARNING = '\033[93m'
        self.FAIL = '\033[91m'
        self.ENDC = '\033[0m'
        self.BOLD = '\033[1m'
        self.UNDERLINE = '\033[4m'
        self.GRAY = "\033[1;30;40m"

class Index_Monitoring(Opensearch_Python):
    def __init__(self):
        super().__init__()

        self.total_or_primaries = 'total'
        self.bcolors = Bcolors()


    def index_monitor(self, index_pattern, primaries=True):
        """
        Live (Terminal) monitoring for Index (or Indices)
        INPUT:
            - Index Pattern (str) (can match multiple indices)
            - primaries (bool) monitor only the Primary shards or (Primary + replica) shards
        """
            
        rich_print("[blink]Loading ...", end="\r")

        def make_layout() -> Layout:
            """
            The layout structure
            """
            layout = Layout(name="root")

            layout.split(
                Layout(name="header", size=3),
                # Layout(name="header2", size=7, ratio=1),
                Layout(name="main", ratio=1),
                # Layout(name="footer", size=6, ratio=1)
            )
            layout["main"].split_row(
                # Layout(name="side",),
                Layout(name="body", ratio=3, minimum_size=60),
            )
            # layout["side"].split(Layout(name="box1", )) # , Layout(name="box2")
            layout["body"].split(Layout(name="head", size=5, ratio=2), Layout(name="body1")) # , Layout(name="box2")

            return layout

        class Header(Index_Monitoring):
            """
            Display header with clock.
            """
            # def matched_indices_count(self, indices_json):
            #     indices_dct = indices_json.get('indices')
            #     return len(indices_dct.keys())

            
            
            def __rich__(self) -> Panel:
                # index_json_all = self.get_index_stats(index_pattern)
                # indices_dct = index_json_all.get('indices')
                grid = Table.grid(expand=True)
                grid.add_column(justify="center", ratio=1)
                grid.add_column(justify="right")
                grid.add_row(
                    f"[b]Index Pattern: [/b] {index_pattern} ",
                    datetime.now().ctime().replace(":", "[blink]:[/]"),
                )
                return Panel(grid, style="green")

        
        class Index_Stats_Monitoring(Index_Monitoring):
            def __init__(self):
                super().__init__()

                self.progress_start()
                
                if primaries:
                    self.total_or_primaries = 'primaries'

                self.observing_data_done = False
                self.first_observation_finished = False

                # Indexing Rate attributes
                self.indexing_rate_60s = 0
                self.indexing_rate_1s = 0
                self.maximum_indexing_rate_1s = 0
                self.minimum_indexing_rate_1s = 999999999999999
                self.indexing_rate_average = 0
                
                # Searching Rate attributes
                self.searching_rate_60s = 0
                self.searching_rate_1s = 0
                self.maximum_searching_rate_1s = 0
                self.minimum_searching_rate_1s = 999999999999999
                self.searching_rate_average = 0

                # Fetch Rate attributes
                self.fetch_rate_60s = 0
                self.fetch_rate_1s = 0
                self.maximum_fetch_rate_1s = 0
                self.minimum_fetch_rate_1s = 999999999999999
                self.fetch_rate_average = 0
                
                # Refresh Rate attributes
                self.refresh_rate_60s = 0
                self.refresh_rate_1s = 0
                self.maximum_refresh_rate_1s = 0
                self.minimum_refresh_rate_1s = 999999999999999
                self.refresh_rate_average = 0

                # Indexing Latency attributes
                self.indexing_time_60s = 0
                self.indexing_time_1s = 0
                self.maximum_indexing_time_1s = 0
                self.minimum_indexing_time_1s = 999999999999999
                self.indexing_time_average = 0

                # Searching Latency attributes
                self.searching_time_60s = 0
                self.searching_time_1s = 0
                self.maximum_searching_time_1s = 0
                self.minimum_searching_time_1s = 999999999999999
                self.searching_time_average = 0

                # Fetch Latency attributes
                self.fetch_time_60s = 0
                self.fetch_time_1s = 0
                self.maximum_fetch_time_1s = 0
                self.minimum_fetch_time_1s = 999999999999999
                self.fetch_time_average = 0

                # Refresh Latency attributes
                self.refresh_time_60s = 0
                self.refresh_time_1s = 0
                self.maximum_refresh_time_1s = 0
                self.minimum_refresh_time_1s = 999999999999999
                self.refresh_time_average = 0

        
            def progress_start(self, rate_update_interval=Attributes.rate_update_interval):

                # Test API call,  will raise exception if NOT able to authenticate.
                index_json = self.get_index_stats(index_pattern)
                n_of_indices = len(index_json.get('indices').keys())
                if n_of_indices == 0:
                    rich_print(f"[bold green]INFO -- No Indices found with the index pattern: [underline]{index_pattern}[/underline]\n")
                    exit(1)

                # Progress for updating Rates bar
                self.progress = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                    TextColumn("{task.fields[status]}"),
                )
                self.task_1 = self.progress.add_task(
                    description=f"calculating rates",
                    status=f"Not started",
                    total=rate_update_interval
                    )

                self.progress_store = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(bar_width=20),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )
                self.task_store_size  = self.progress_store.add_task(
                    description=f"Store size",
                    status="unknown",
                    )
                self.task_docs_count  = self.progress_store.add_task(
                    description=f"Docs count",
                    status="unknown",
                    )
                # self.task_docs_deleted  = self.progress_store.add_task(
                #     description=f"Docs deleted",
                #     status="unknown",
                #     )
                self.progress_index_info = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(bar_width=20),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )
                self.task_matched_indices  = self.progress_index_info.add_task(
                    description=f"Matched Indices count",
                    status="unknown",
                    )
                self.task_total_shards_count  = self.progress_index_info.add_task(
                    description=f"Total shards count",
                    status="unknown",
                    )

                self.progress_thread_index_monitoring = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(bar_width=20),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )
                self.task_thread_index_rate_observation  = self.progress_thread_index_monitoring.add_task(
                    description=f"Rate Observation",
                    status="unknown",
                    )
                self.task_thread_index_rate_update  = self.progress_thread_index_monitoring.add_task(
                    description=f"Rate update",
                    status="unknown",
                    )
                self.task_thread_index_monitoring  = self.progress_thread_index_monitoring.add_task(
                    description=f"Index Monitoring",
                    status="unknown",
                    )


                self.progress_indexing_rate = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(bar_width=20),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )
                self.task_rate_n_sec = self.progress_indexing_rate.add_task(
                    description=f"Rate ({rate_update_interval}s)",
                    status="waiting",
                    )
                self.task_rate_1sec = self.progress_indexing_rate.add_task(
                    description=f"Rate (1s)",
                    status="waiting",
                    )
                self.task_rate_min = self.progress_indexing_rate.add_task(
                    description=f"Rate MIN",
                    status="waiting",
                    )
                self.task_rate_max = self.progress_indexing_rate.add_task(
                    description=f"Rate MAX",
                    status="waiting",
                    )
                self.task_rate_avg = self.progress_indexing_rate.add_task(
                    description=f"Rate AVG",
                    status="waiting",
                    )

                self.progress_searching_rate = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(bar_width=20),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )
                self.task_rate_n_sec = self.progress_searching_rate.add_task(
                    description=f"Rate ({rate_update_interval}s)",
                    status="waiting",
                    )
                self.task_rate_1sec = self.progress_searching_rate.add_task(
                    description=f"Rate (1s)",
                    status="waiting",
                    )
                self.task_rate_min = self.progress_searching_rate.add_task(
                    description=f"Rate MIN",
                    status="waiting",
                    )
                self.task_rate_max = self.progress_searching_rate.add_task(
                    description=f"Rate MAX",
                    status="waiting",
                    )
                self.task_rate_avg = self.progress_searching_rate.add_task(
                    description=f"Rate AVG",
                    status="waiting",
                    )

                self.progress_fetch_rate = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(bar_width=20),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )
                self.task_rate_n_sec = self.progress_fetch_rate.add_task(
                    description=f"Rate ({rate_update_interval}s)",
                    status="waiting",
                    )
                self.task_rate_1sec = self.progress_fetch_rate.add_task(
                    description=f"Rate (1s)",
                    status="waiting",
                    )
                self.task_rate_min = self.progress_fetch_rate.add_task(
                    description=f"Rate MIN",
                    status="waiting",
                    )
                self.task_rate_max = self.progress_fetch_rate.add_task(
                    description=f"Rate MAX",
                    status="waiting",
                    )
                self.task_rate_avg = self.progress_fetch_rate.add_task(
                    description=f"Rate AVG",
                    status="waiting",
                    )

                self.progress_refresh_rate = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(bar_width=20),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )
                self.task_rate_n_sec = self.progress_refresh_rate.add_task(
                    description=f"Rate ({rate_update_interval}s)",
                    status="waiting",
                    )
                self.task_rate_1sec = self.progress_refresh_rate.add_task(
                    description=f"Rate (1s)",
                    status="waiting",
                    )
                self.task_rate_min = self.progress_refresh_rate.add_task(
                    description=f"Rate MIN",
                    status="waiting",
                    )
                self.task_rate_max = self.progress_refresh_rate.add_task(
                    description=f"Rate MAX",
                    status="waiting",
                    )
                self.task_rate_avg = self.progress_refresh_rate.add_task(
                    description=f"Rate AVG",
                    status="waiting",
                    )
                
                self.progress_indexing_latency = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(bar_width=20),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )
                self.task_time_n_sec = self.progress_indexing_latency.add_task(
                    description=f"Indexing Time ({rate_update_interval}s)",
                    status="waiting",
                    )
                self.task_time_1sec = self.progress_indexing_latency.add_task(
                    description=f"Indexing Time (1s)",
                    status="waiting",
                    )
                self.task_time_min = self.progress_indexing_latency.add_task(
                    description=f"Indexing Time MIN",
                    status="waiting",
                    )
                self.task_time_max = self.progress_indexing_latency.add_task(
                    description=f"Indexing Time MAX",
                    status="waiting",
                    )
                self.task_time_avg = self.progress_indexing_latency.add_task(
                    description=f"Indexing Time AVG",
                    status="waiting",
                    )
                
                self.progress_searching_latency = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(bar_width=20),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )
                self.task_time_n_sec = self.progress_searching_latency.add_task(
                    description=f"Query Time ({rate_update_interval}s)",
                    status="waiting",
                    )
                self.task_time_1sec = self.progress_searching_latency.add_task(
                    description=f"Query Time (1s)",
                    status="waiting",
                    )
                self.task_time_min = self.progress_searching_latency.add_task(
                    description=f"Query Time MIN",
                    status="waiting",
                    )
                self.task_time_max = self.progress_searching_latency.add_task(
                    description=f"Query Time MAX",
                    status="waiting",
                    )
                self.task_time_avg = self.progress_searching_latency.add_task(
                    description=f"Query Time AVG",
                    status="waiting",
                    )

                self.progress_fetch_latency = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(bar_width=20),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )
                self.task_time_n_sec = self.progress_fetch_latency.add_task(
                    description=f"Fetch Time ({rate_update_interval}s)",
                    status="waiting",
                    )
                self.task_time_1sec = self.progress_fetch_latency.add_task(
                    description=f"Fetch Time (1s)",
                    status="waiting",
                    )
                self.task_time_min = self.progress_fetch_latency.add_task(
                    description=f"Fetch Time MIN",
                    status="waiting",
                    )
                self.task_time_max = self.progress_fetch_latency.add_task(
                    description=f"Fetch Time MAX",
                    status="waiting",
                    )
                self.task_time_avg = self.progress_fetch_latency.add_task(
                    description=f"Fetch Time AVG",
                    status="waiting",
                    )

                self.progress_refresh_latency = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(bar_width=20),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )
                self.task_time_n_sec = self.progress_refresh_latency.add_task(
                    description=f"Refresh Time ({rate_update_interval}s)",
                    status="waiting",
                    )
                self.task_time_1sec = self.progress_refresh_latency.add_task(
                    description=f"Refresh Time (1s)",
                    status="waiting",
                    )
                self.task_time_min = self.progress_refresh_latency.add_task(
                    description=f"Refresh Time MIN",
                    status="waiting",
                    )
                self.task_time_max = self.progress_refresh_latency.add_task(
                    description=f"Refresh Time MAX",
                    status="waiting",
                    )
                self.task_time_avg = self.progress_refresh_latency.add_task(
                    description=f"Refresh Time AVG",
                    status="waiting",
                    )

                # Searching / Indexing / Fetches (Number of operations In progress)
                self.progress_searching_current = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(bar_width=20),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )
                self.task_seraching_current = self.progress_searching_current.add_task(
                    description=f"[yellow]IN PROGRESS",
                    status="...",
                    )

                self.progress_indexing_current = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(bar_width=20),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )
                self.task_indexing_current = self.progress_indexing_current.add_task(
                    description=f"[yellow]IN PROGRESS",
                    status="...",
                    )

                self.progress_fetch_current = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(bar_width=20),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )   
                self.task_fetch_current = self.progress_fetch_current.add_task(
                    description=f"[yellow]IN PROGRESS",
                    status="...",
                    )


                self.group_index_info = Group (
                    self.progress_index_info,
                    Rule(style='#AAAAAA'),
                )

                self.group_index_store = Group (
                    self.progress_store,
                    Rule(style='#AAAAAA'),
                )

            def observe_data(self, task_id, sleep_time, seconds=Attributes.rate_update_interval, interval=20):
                # Wait for N seconds
                cnt = 0
                while True:
                    # Get readings
                    # current_time = datetime.now((timezone.utc)).strftime("%Y-%m-%d %H:%M:%S")
                    index_json = self.get_index_stats(index_pattern).get('_all').get(self.total_or_primaries)
                    start_time = time.time()

                    start_indexing_total = index_json.get('indexing').get('index_total')
                    start_searching_total = index_json.get('search').get('query_total')
                    start_fetch_total = index_json.get('search').get('fetch_total')
                    start_refresh_total = index_json.get('refresh').get('total')
                    
                    start_indexing_time_ms = index_json.get('indexing').get('index_time_in_millis')
                    start_searching_time_ms = index_json.get('search').get('query_time_in_millis')
                    start_fetch_time_ms = index_json.get('search').get('fetch_time_in_millis')
                    start_refresh_time_ms = index_json.get('refresh').get('total_time_in_millis')

                    # Wait for N seconds (60s)
                    while (not self.observing_data_done):
                        time.sleep(sleep_time) # 1 sec
                        cnt +=1

                        # After the counter is done, get second readings & calculate
                        if cnt > seconds:
                            self.progress.update(task_id=task_id, status=f"done", completed=cnt, total=seconds,)
                            # Declare that first observation round is done.
                            if not self.first_observation_finished:
                                self.first_observation_finished = True

                            # Get readings
                            index_json = self.get_index_stats(index_pattern).get('_all').get(self.total_or_primaries)

                            end_time = time.time()
                            end_indexing_total = index_json.get('indexing').get('index_total')
                            end_searching_total = index_json.get('search').get('query_total')
                            end_fetch_total = index_json.get('search').get('fetch_total')
                            end_refresh_total = index_json.get('refresh').get('total')

                            end_indexing_time_ms = index_json.get('indexing').get('index_time_in_millis')
                            end_searching_time_ms = index_json.get('search').get('query_time_in_millis')
                            end_fetch_time_ms = index_json.get('search').get('fetch_time_in_millis')
                            end_refresh_time_ms = index_json.get('refresh').get('total_time_in_millis')


                            time_difference = (int(end_time - start_time))
                            Attributes.total_observation_time = time_difference


                            #### Update the attributes with the calculation results ###

                            # Indexing / Searching Rate
                            self.indexing_rate_60s = str((end_indexing_total - start_indexing_total)) + " doc"
                            self.indexing_rate_1s = (end_indexing_total - start_indexing_total) / time_difference
                            
                            self.searching_rate_60s = str((end_searching_total - start_searching_total)) + " doc"
                            self.searching_rate_1s = (end_searching_total - start_searching_total) / time_difference

                            if self.maximum_searching_rate_1s < self.searching_rate_1s:
                                self.maximum_searching_rate_1s = self.searching_rate_1s

                            if self.searching_rate_1s < self.minimum_searching_rate_1s:
                                self.minimum_searching_rate_1s = self.searching_rate_1s

                            if self.maximum_indexing_rate_1s < self.indexing_rate_1s:
                                self.maximum_indexing_rate_1s = self.indexing_rate_1s

                            if self.indexing_rate_1s < self.minimum_indexing_rate_1s:
                                self.minimum_indexing_rate_1s = self.indexing_rate_1s

                            if self.maximum_indexing_rate_1s > self.minimum_indexing_rate_1s:
                                self.indexing_rate_average = (self.maximum_indexing_rate_1s + self.minimum_indexing_rate_1s) / 2

                            if self.maximum_searching_rate_1s > self.minimum_searching_rate_1s:
                                self.searching_rate_average = (self.maximum_searching_rate_1s + self.minimum_searching_rate_1s) / 2

                            # Refresh Rate
                            self.refresh_rate_60s = (end_refresh_total - start_refresh_total)
                            self.refresh_rate_1s = (end_refresh_total - start_refresh_total) / time_difference

                            if self.maximum_refresh_rate_1s < self.refresh_rate_1s:
                                self.maximum_refresh_rate_1s = self.refresh_rate_1s

                            if self.refresh_rate_1s < self.minimum_refresh_rate_1s:
                                self.minimum_refresh_rate_1s = self.refresh_rate_1s

                            if self.maximum_refresh_rate_1s > self.minimum_refresh_rate_1s:
                                self.refresh_rate_average = (self.maximum_refresh_rate_1s + self.minimum_refresh_rate_1s) / 2

                            # Fetch Rate
                            self.fetch_rate_60s = (end_fetch_total - start_fetch_total)
                            self.fetch_rate_1s = (end_fetch_total - start_fetch_total) / time_difference

                            if self.maximum_fetch_rate_1s < self.fetch_rate_1s:
                                self.maximum_fetch_rate_1s = self.fetch_rate_1s

                            if self.fetch_rate_1s < self.minimum_fetch_rate_1s:
                                self.minimum_fetch_rate_1s = self.fetch_rate_1s
                            
                            if self.maximum_fetch_rate_1s > self.minimum_fetch_rate_1s:
                                self.fetch_rate_average = (self.maximum_fetch_rate_1s + self.minimum_fetch_rate_1s) / 2

                            # Searching / Indexing Latency
                            self.indexing_time_60s = (end_indexing_time_ms - start_indexing_time_ms)
                            self.indexing_time_1s = (end_indexing_time_ms - start_indexing_time_ms) / time_difference

                            self.searching_time_60s = (end_searching_time_ms - start_searching_time_ms)
                            self.searching_time_1s = (end_searching_time_ms - start_searching_time_ms) / time_difference

                            if self.maximum_searching_time_1s < self.searching_time_1s:
                                self.maximum_searching_time_1s = self.searching_time_1s

                            if self.searching_time_1s < self.minimum_searching_time_1s:
                                self.minimum_searching_time_1s = self.searching_time_1s
                                
                            if self.maximum_indexing_time_1s < self.indexing_time_1s:
                                self.maximum_indexing_time_1s = self.indexing_time_1s

                            if self.indexing_time_1s < self.minimum_indexing_time_1s:
                                self.minimum_indexing_time_1s = self.indexing_time_1s

                            if self.maximum_indexing_time_1s > self.minimum_indexing_time_1s:
                                self.indexing_time_average = (self.maximum_indexing_time_1s + self.minimum_indexing_time_1s) / 2

                            if self.maximum_searching_time_1s > self.minimum_searching_time_1s:
                                self.searching_time_average = (self.maximum_searching_time_1s + self.minimum_searching_time_1s) / 2
                            
                            # Fetch Latency
                            self.fetch_time_60s = (end_fetch_time_ms - start_fetch_time_ms) 
                            self.fetch_time_1s = (end_fetch_time_ms - start_fetch_time_ms) / time_difference

                            if self.maximum_fetch_time_1s < self.fetch_time_1s:
                                self.maximum_fetch_time_1s = self.fetch_time_1s

                            if self.fetch_time_1s < self.minimum_fetch_time_1s:
                                self.minimum_fetch_time_1s = self.fetch_time_1s

                            if self.maximum_fetch_time_1s > self.minimum_fetch_time_1s:
                                self.fetch_time_average = (self.maximum_fetch_time_1s + self.minimum_fetch_time_1s) / 2

                            # Refresh Latency
                            self.refresh_time_60s = (end_refresh_time_ms - start_refresh_time_ms) 
                            self.refresh_time_1s = (end_refresh_time_ms - start_refresh_time_ms) / time_difference

                            if self.maximum_refresh_time_1s < self.refresh_time_1s:
                                self.maximum_refresh_time_1s = self.refresh_time_1s

                            if self.refresh_time_1s < self.minimum_refresh_time_1s:
                                self.minimum_refresh_time_1s = self.refresh_time_1s
                            
                            if self.maximum_refresh_time_1s > self.minimum_refresh_time_1s:
                                self.refresh_time_average = (self.maximum_refresh_time_1s + self.minimum_refresh_time_1s) / 2

                            # Time to wait before the next update / recommended 30 - 60 seconds
                            time.sleep(interval)
                            cnt = 0
                            break
                        else:
                            self.progress.update(task_id=task_id, status=f"updating in ({cnt}/{seconds})", completed=cnt, total=seconds)


            def update_rates(self,task_id="", sleep_time=5):
                while True:
                    time.sleep(5)

                    if self.first_observation_finished:
                        # Update Indexing rate
                        self.progress_indexing_rate.update(description=f"Rate ({Attributes.total_observation_time}s)", task_id=self.task_rate_n_sec, status=f"{self.indexing_rate_60s}")                            
                        self.progress_indexing_rate.update(task_id=self.task_rate_1sec, status=f"{str(math.ceil(self.indexing_rate_1s))} doc")
                        self.progress_indexing_rate.update(task_id=self.task_rate_min, status=f"{str(math.ceil(self.minimum_indexing_rate_1s))} doc /s")
                        self.progress_indexing_rate.update(task_id=self.task_rate_max, status=f"{str(math.ceil(self.maximum_indexing_rate_1s))} doc /s")
                        # Update Searching rate
                        self.progress_searching_rate.update(description=f"Rate ({Attributes.total_observation_time}s)", task_id=self.task_rate_n_sec, status=f"{self.searching_rate_60s}")
                        self.progress_searching_rate.update(task_id=self.task_rate_1sec, status=f"{str(math.ceil(self.searching_rate_1s))} doc")
                        self.progress_searching_rate.update(task_id=self.task_rate_min, status=f"{str(math.ceil(self.minimum_searching_rate_1s))} doc /s")
                        self.progress_searching_rate.update(task_id=self.task_rate_max, status=f"{str(math.ceil(self.maximum_searching_rate_1s))} doc /s")
                        # Update Fetch rate
                        self.progress_fetch_rate.update(description=f"Rate ({Attributes.total_observation_time}s)", task_id=self.task_rate_n_sec, status=f"{math.ceil(self.fetch_rate_60s)} doc")
                        self.progress_fetch_rate.update(task_id=self.task_rate_1sec, status=f"{str(math.ceil(self.task_rate_1sec))} doc")
                        self.progress_fetch_rate.update(task_id=self.task_rate_min, status=f"{str(math.ceil(self.task_rate_min))} doc /s")
                        self.progress_fetch_rate.update(task_id=self.task_rate_max, status=f"{str(math.ceil(self.task_rate_max))} doc /s")
                        # Update Refresh rate
                        self.progress_refresh_rate.update(description=f"Rate ({Attributes.total_observation_time}s)", task_id=self.task_rate_n_sec, status=f"{math.ceil(self.refresh_rate_60s)} doc")
                        self.progress_refresh_rate.update(task_id=self.task_rate_1sec, status=f"{math.ceil(self.refresh_rate_1s)} doc")
                        self.progress_refresh_rate.update(task_id=self.task_rate_min, status=f"{math.ceil(self.minimum_refresh_rate_1s)} doc")
                        self.progress_refresh_rate.update(task_id=self.task_rate_max, status=f"{math.ceil(self.maximum_refresh_rate_1s)} doc")
                        # Update Searching time
                        self.progress_searching_latency.update(description=f"Query time ({Attributes.total_observation_time}s)", task_id=self.task_time_n_sec, status=helper_.millisec_to_sec_m_h(self.searching_time_60s))
                        self.progress_searching_latency.update(task_id=self.task_time_1sec, status=helper_.millisec_to_sec_m_h(self.searching_time_1s))
                        self.progress_searching_latency.update(task_id=self.task_time_min, status=helper_.millisec_to_sec_m_h(self.minimum_searching_time_1s))
                        self.progress_searching_latency.update(task_id=self.task_time_max, status=helper_.millisec_to_sec_m_h(self.maximum_searching_time_1s))
                        # Update Indexing time
                        self.progress_indexing_latency.update(description=f"Indexing time ({Attributes.total_observation_time}s)", task_id=self.task_time_n_sec, status=helper_.millisec_to_sec_m_h(self.indexing_time_60s))
                        self.progress_indexing_latency.update(task_id=self.task_time_1sec, status=helper_.millisec_to_sec_m_h(self.indexing_time_1s))
                        self.progress_indexing_latency.update(task_id=self.task_time_min, status=helper_.millisec_to_sec_m_h(self.minimum_indexing_time_1s))
                        self.progress_indexing_latency.update(task_id=self.task_time_max, status=helper_.millisec_to_sec_m_h(self.maximum_indexing_time_1s))
                        # Update Fetch time
                        self.progress_fetch_latency.update(description=f"Fetch time ({Attributes.total_observation_time}s)", task_id=self.task_time_n_sec, status=helper_.millisec_to_sec_m_h(self.fetch_time_60s))
                        self.progress_fetch_latency.update(task_id=self.task_time_1sec, status=helper_.millisec_to_sec_m_h(self.fetch_time_1s))
                        self.progress_fetch_latency.update(task_id=self.task_time_min, status=helper_.millisec_to_sec_m_h(self.minimum_fetch_time_1s))
                        self.progress_fetch_latency.update(task_id=self.task_time_max, status=helper_.millisec_to_sec_m_h(self.maximum_fetch_time_1s))
                        # Update Refresh time
                        self.progress_refresh_latency.update(description=f"Refresh time ({Attributes.total_observation_time}s)", task_id=self.task_time_n_sec, status=helper_.millisec_to_sec_m_h(self.refresh_time_60s))
                        self.progress_refresh_latency.update(task_id=self.task_time_1sec, status=helper_.millisec_to_sec_m_h(self.refresh_time_1s))
                        self.progress_refresh_latency.update(task_id=self.task_time_min, status=helper_.millisec_to_sec_m_h(self.minimum_refresh_time_1s))
                        self.progress_refresh_latency.update(task_id=self.task_time_max, status=helper_.millisec_to_sec_m_h(self.maximum_refresh_time_1s))

                        if not self.indexing_rate_average == 0:
                            self.progress_indexing_rate.update(description=f"Rate [green]AVG", task_id=self.task_rate_avg, status=f"{str(math.ceil(self.indexing_rate_average))} doc /s")

                        if not self.searching_rate_average == 0:
                            self.progress_searching_rate.update(description=f"Rate [green]AVG",task_id=self.task_rate_avg, status=f"{str(math.ceil(self.searching_rate_average))} doc /s")

                        if not self.fetch_rate_average == 0:
                            self.progress_fetch_rate.update(description=f"Rate [green]AVG",task_id=self.task_rate_avg, status=f"{str(math.ceil(self.fetch_rate_average))} doc /s")

                        if not self.indexing_time_average == 0:
                            self.progress_indexing_latency.update(description=f"Indexing time [green]AVG", task_id=self.task_time_avg, status=helper_.millisec_to_sec_m_h(self.indexing_time_average))

                        if not self.searching_time_average == 0:
                            self.progress_searching_latency.update(description=f"Query time [green]AVG", task_id=self.task_time_avg, status=helper_.millisec_to_sec_m_h(self.searching_rate_average))

                        if not self.fetch_time_average == 0:
                            self.progress_fetch_latency.update(description=f"Fetch time [green]AVG", task_id=self.task_time_avg, status=helper_.millisec_to_sec_m_h(self.fetch_time_average))

                        if not self.refresh_time_average == 0:
                            self.progress_refresh_latency.update(description=f"Refresh time [green]AVG", task_id=self.task_time_avg, status=helper_.millisec_to_sec_m_h(self.refresh_time_average))


            def update(self):
                
                while True:
                    index_json_all = self.get_index_stats(index_pattern)
                    index_json = index_json_all.get('_all').get(self.total_or_primaries)
                    indices_dct = index_json_all.get('indices')

                    self.progress_store.update(task_id=self.task_store_size, status=helper_.bytes_to_kb_mb_gb(index_json.get('store').get('size_in_bytes')))
                    self.progress_store.update(task_id=self.task_docs_count, status=index_json.get('docs').get('count'))
                    # self.progress_store.update(task_id=self.task_docs_deleted, status=index_json.get('docs').get('deleted'))

                    self.progress_index_info.update(task_id=self.task_total_shards_count, status=index_json_all.get('_shards').get('total'))
                    self.progress_index_info.update(task_id=self.task_matched_indices, status=len(indices_dct))

                    self.progress_searching_current.update(task_id=self.task_seraching_current, status=index_json.get('search').get('query_current'))
                    self.progress_indexing_current.update(task_id=self.task_indexing_current, status=index_json.get('indexing').get('index_current'))
                    self.progress_fetch_current.update(task_id=self.task_fetch_current, status=index_json.get('search').get('fetch_current'))

                    time.sleep(Attributes.mointoring_interval)

            def check_thread_status_rate_observation(self, restart=True):
                while True:
                    def thread_status():
                        status = ""
                        if self.thread_rate_observation.is_alive():
                            status = f"alive [green]✔️"
                        else:
                            status = "dead [red]❌"
                            if restart:
                                self.start_threads()
                        return status
                    
                    self.progress_thread_index_monitoring.update(task_id=self.task_thread_index_rate_observation, status=thread_status())                            

                    time.sleep(5)

            def check_thread_status_rate_update(self, restart=True):
                while True:
                    def thread_status():
                        status = ""
                        if self.thread_rate_update.is_alive():
                            status = f"alive [green]✔️"
                        else:
                            status = "dead [red]❌"
                            if restart:
                                self.start_threads()
                        return status
                    
                    self.progress_thread_index_monitoring.update(task_id=self.task_thread_index_rate_update, status=thread_status())                            

                    time.sleep(5)

            def check_thread_status_monitoring(self, restart=True):
                while True:
                    def thread_status():
                        status = ""
                        if self.thread_index_store.is_alive():
                            status = f"alive [green]✔️"
                        else:
                            status = "dead [red]❌"
                            if restart:
                                # Restart thread
                                self.start_threads()
                        return status

                    self.progress_thread_index_monitoring.update(task_id=self.task_thread_index_monitoring, status=thread_status())                            
                    time.sleep(5)

            def start_threads(self):
                # Thread for updating index store size every N seconds
                self.thread_index_store = threading.Thread(target=self.update)
                self.thread_index_store.daemon = True
                self.thread_index_store.start()

                # Thread for observing data (and updating the attributes)
                self.thread_rate_observation = threading.Thread(target=self.observe_data, args=(self.task_1, 1))
                self.thread_rate_observation.daemon = True
                self.thread_rate_observation.start()

                # And thread for updating the the progress bars every N seconds (from the attributes)
                self.thread_rate_update = threading.Thread(target=self.update_rates)
                self.thread_rate_update.daemon = True
                self.thread_rate_update.start()


            def watch_threads(self):
                self.thread_check_thread_monitoring = threading.Thread(target=self.check_thread_status_monitoring)
                self.thread_check_thread_monitoring.daemon = True
                self.thread_check_thread_monitoring.start()

                self.thread_check_thread_rate_observation = threading.Thread(target=self.check_thread_status_rate_observation)
                self.thread_check_thread_rate_observation.daemon = True
                self.thread_check_thread_rate_observation.start()

                self.thread_check_thread_rate_update = threading.Thread(target=self.check_thread_status_rate_update)
                self.thread_check_thread_rate_update.daemon = True
                self.thread_check_thread_rate_update.start()


        try:
            index_monitoring = Index_Stats_Monitoring()

            group_searching_rate = Group (
                    index_monitoring.progress_searching_current,
                    # Rule(style='#AAAAAA'),
                    index_monitoring.progress_searching_rate
                )

            group_indexing_rate = Group (
                    index_monitoring.progress_indexing_current,
                    # Rule(style='#AAAAAA'),
                    index_monitoring.progress_indexing_rate
                )

            group_fetch_rate = Group (
                    index_monitoring.progress_fetch_current,
                    # Rule(style='#AAAAAA'),
                    index_monitoring.progress_fetch_rate
                )

            group_refresh_rate = Group (
                    # Rule(style='#AAAAAA'),
                    "",
                    index_monitoring.progress_refresh_rate
                )

            rates_progress_obervation = Table.grid(expand=True)
            rates_progress_obervation.add_row(
                Panel(index_monitoring.group_index_store, title="", width=35, padding=(0, 2)),
                Panel(index_monitoring.group_index_info, title="", width=35, padding=(0, 2)),
                # Panel(index_monitoring.progress_store, title="", width=35, padding=(0, 2)),
                # Panel(index_monitoring.progress_index_info, title="", width=35),
                Panel(index_monitoring.progress, title="[b]Observing Data", padding=(1, 2),  width=89),
                Panel(index_monitoring.progress_thread_index_monitoring, title="[b]Threads status", padding=(0, 2), width=40),
                )

            progress_table = Table.grid(expand=True)
            progress_table.add_row(
                Panel(group_indexing_rate, title="[b]Indexing Rate", padding=(1, 2)),
                Panel(group_refresh_rate, title="[b]Refresh Rate", padding=(1, 2)),
                Panel(group_searching_rate, title="[b]Searching Rate", padding=(1, 2)),
                Panel(group_fetch_rate, title="[b]Fetch Rate", padding=(1, 2)),
            )
            progress_table.add_row(
                Panel(index_monitoring.progress_indexing_latency, title="[b]Indexing Latency", padding=(1, 2)),
                Panel(index_monitoring.progress_refresh_latency, title="[b]Refresh Latency", padding=(1, 2)),
                Panel(index_monitoring.progress_searching_latency, title="[b]Searching Latency", padding=(1, 2)),
                Panel(index_monitoring.progress_fetch_latency, title="[b]Fetch Latency", padding=(1, 2)),
            )


            layout = make_layout()
            layout["header"].update(Header())
            layout["head"].update(rates_progress_obervation)
            layout["body1"].update(progress_table)

            index_monitoring.start_threads()
            index_monitoring.watch_threads()

            with Live(layout, auto_refresh=True, screen=True):
                while True:
                    time.sleep(30)

        except (KeyboardInterrupt):
            rich_print(layout)
            exit(0)
        except (opensearchpy.exceptions.AuthorizationException,
                opensearchpy.exceptions.ConnectionTimeout,
                opensearchpy.exceptions.ConnectionTimeout) as e:
            raise SystemExit(f"ERROR -- connection failed with openSearch\n> {e}") 


    def print_shards_nodes_allocations(self, index):
        """
        Print a table for shards allocations over the nodes
        INPUT:
            - Index pattern
        """

        try:
            print()
            self.authenticate

            shards_search = self.node_search_shards(index)

            # Create a empty dictionary for nodes
            nodes_dct = {}
            total_shards_number = 0
            for node_id, node_info in shards_search.get('nodes').items():
                nodes_dct[node_id] = {
                    "transport_address": node_info.get('transport_address'),
                    "box_type": node_info.get('attributes').get('box_type'),
                    # "zone": node_info.get('attributes').get('zone'),
                    "P_shards_number": 0,
                    "R_shards_number": 0
                }

            for shards_lst in shards_search.get('shards'):
                # print(shards_lst)
                # print()
                for shard_dct in shards_lst:
                    if shard_dct.get('primary'):
                        nodes_dct[shard_dct.get('node')]['P_shards_number'] += 1
                        total_shards_number +=1
                    elif not shard_dct.get('primary'):
                        nodes_dct[shard_dct.get('node')]['R_shards_number'] += 1
                        total_shards_number +=1

            table = [['Node ID', "Type", 'Transport address', f"Allocated shards\n[ of {total_shards_number} ]   ", "Percentage", "Primary shards", "Replica shards"]]
            for node_id, node_info in nodes_dct.items():

                row = [
                    node_id, 
                    node_info['box_type'], 
                    node_info['transport_address'], 
                    (node_info['P_shards_number'] + node_info['R_shards_number']),
                    helper_.percentage((node_info['P_shards_number'] + node_info['R_shards_number']), total_shards_number),
                    # f"[ [white]{helper_.percentage(node_info['P_shards_number'], node_info['P_shards_number'] + node_info['R_shards_number'])}[/white] ] {node_info['P_shards_number']}", 
                    node_info['P_shards_number'],
                    node_info['R_shards_number']
                    ]
                table.append(row)

            
            out = tabulate(table, headers='firstrow', tablefmt='grid', showindex=False)
            
            rich_print(Panel(f"Detected number of shards: {total_shards_number} ", expand=False, border_style="green"))
            print(out)

        except (opensearchpy.exceptions.AuthorizationException,
                opensearchpy.exceptions.ConnectionTimeout,
                opensearchpy.exceptions.ConnectionTimeout) as e:
            raise SystemExit(f"ERROR -- connection failed with openSearch\n> {e}")
        except (KeyboardInterrupt):
            exit(0)


    def print_indices_patterns_table(self, patterns_list, template_version=2, sort_by=None):
        """
        Print a strutured table with each index pattern information
        INPUT:
            - Indices_patterns (list)
        """
        try:
            print("Getting Index templates ...", end="\r")
            index_templates_list_ = self.get_index_template("*",  template_version=template_version)
            
            def find_index_template(pattern, index_templates_list=index_templates_list_):
                found = []
                if template_version == 2:
                    index_templates_list = index_templates_list.get('index_templates')
                    for template in index_templates_list:
                        if pattern in template.get("index_template").get("index_patterns"):
                            found.append(template.get("name"))

                if template_version == 1:
                    for template_name, value in index_templates_list.items():
                        if pattern in value.get("index_patterns"):
                            found.append(template_name)
                return found


            self.progress_shards_list = Progress(
                SpinnerColumn(),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(bar_width=30), 
                TaskProgressColumn(),
                TextColumn("{task.fields[status]}"),
            )
            self.task_percentage  = self.progress_shards_list.add_task(
                    description=f"[b]Discovering Patterns ",
                    status="...",
                    total=len(patterns_list),
                    )

            table = []

            with Live(self.progress_shards_list, auto_refresh=True, screen=False):
                cnt = 1
                for pattern in patterns_list:
                    # Add * at the end of the patter is doesn't exist
                    if pattern[-1] != '*':
                        pattern = pattern + "*"

                    self.progress_shards_list.update(task_id=self.task_percentage, status=f" [ [yellow]{pattern}[/yellow] ]", completed=cnt)                            
                    try:
                        pattern_stats = self.get_index_stats(pattern, exit_on_fail=False)
                        shards_number_total = pattern_stats.get("_shards").get("total")
                        if shards_number_total != 0:
                            indices_number = len(pattern_stats.get("indices"))
                            size_total = pattern_stats.get('_all').get('total').get('store').get('size_in_bytes')
                            # size_total = helper_.bytes_to_kb_mb_gb(pattern_stats.get('_all').get('total').get('store').get('size_in_bytes'))
                            size_p = pattern_stats.get('_all').get('primaries').get('store').get('size_in_bytes')
                            # size_p = helper_.bytes_to_kb_mb_gb(pattern_stats.get('_all').get('primaries').get('store').get('size_in_bytes'))
                            comment = ""
                        else:
                            indices_number = 0
                            shards_number_total = 0
                            size_total = 0
                            size_p = 0
                            comment = "❗ **INDEX NOT FOUND**"
                            
                    except KeyError:
                        pattern_stats = {}
                        indices_number = "0"
                        shards_number_total ="0"
                        size_total = "0"
                        size_p = "0"
                        comment = "❗ **INDEX NOT FOUND**"

                    matching_templates = find_index_template(pattern)
                    if len(matching_templates) > 0:
                        index_templates = f"`{''.join(matching_templates)}`"
                    else:
                        index_templates = " "

                    ism_policy = "🔍 **NOT ENABLED**"
                    if self.get_ism_policy(pattern) is None:
                        ism_policy = "❗ **NOT FOUND**"
                    elif self.get_ism_policy(pattern):
                        ism_policy = f"`{self.get_ism_policy(pattern).get('policy_id')}`"

                    row = [
                        # Index pattern
                        f"`{pattern}`",
                        # Indices number
                        int(indices_number),
                        # shards number total
                        int(shards_number_total),
                        # size total
                        int(size_total),
                        # size of Primary shards
                        int(size_p),
                        # Matching index templates
                        index_templates,
                        # ISM policy
                        ism_policy,
                        comment
                    ]

                    table.append(row)
                    cnt+=1

            # Sort the table
            print(sort_by)
            if sort_by is not None:
                if sort_by == 'size':
                    table = sorted(table, key=itemgetter(3), reverse=True)
                elif sort_by == 'indices':
                    table = sorted(table, key=itemgetter(1), reverse=True)
                elif sort_by == 'shards':
                    table = sorted(table, key=itemgetter(2), reverse=True)

            # convert sizes from bytes to gb, tb, etc.
            for i in range(len(table)):
                if isinstance(table[i][3], int):
                    table[i][3] = helper_.bytes_to_kb_mb_gb(table[i][3])
                if isinstance(table[i][4], int):
                    table[i][4] = helper_.bytes_to_kb_mb_gb(table[i][4])

            # Add the table header at the begining of the list
            table.insert(0, ['**Index pattern**', "**Indices number**", "**Shards number**", '**Size total**', "**Size P**", f"**Index Templates** (v{template_version})", "**ISM Policy**", "**Comment**"])
            out = tabulate(table, headers='firstrow', tablefmt='github')
            # rich_print(Markdown(out))
            table_file = "patterns-table.md"
            with open(table_file, 'w') as f:
                f.write(out)
            # print(f"\n{out}\n")
            rich_print(f"\n- Table is saved in '{table_file}'")
            pyperclip.copy(out)
            rich_print("[green]- Table is copied to clipboard 📋")
        except KeyboardInterrupt:
            print()
            rich_print("[green]OK!")
            exit(0)
        except (opensearchpy.exceptions.AuthorizationException,
                opensearchpy.exceptions.ConnectionTimeout) as e:
            raise SystemExit(f"ERROR -- (print_indices_patterns_table) connection failed with openSearch\n> {e}")

        



