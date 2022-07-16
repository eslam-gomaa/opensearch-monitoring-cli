import opensearchpy
import time
from tabulate import tabulate
from datetime import datetime, timezone
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



# To be used when neded
# from rich import box
# from rich.syntax import Syntax
# from rich.markdown import Markdown


from opmcli.opensearch_api import Opensearch_Python
from opmcli.attributes import Attributes
from opmcli.helper import Helper
helper_ = Helper()


class Node_Monitoring(Opensearch_Python):
    def __init__(self):
        super().__init__()
        

    def print_calculate_indexing_rate(self, index, seconds=60, times=3, interval=3):
        """
        [ DEPRECATED -> To be deleted ]
        Calculates the indexing & searching rates pe N seconds (print a table)
        - INPUT:
            1. Index: (not a pattern, name of a specific index)
            2. seconds: (number of seconds to wait while calculating)
            3. times: how many times to calculate the rate
            4. interval: the time to wait between the each rate calculation
        """
        try:
            if Opensearch_Python.es_client is None:
                self.authenticate()

            # print expected observation time
            total_seconds = (times * interval) + (times * seconds)

            print(f"Expected observation time: {helper_.sec_to_m_h(total_seconds)}")

            #  Fail if the index is a pattern not a signle index.
            index_lst = self.get_index(index)
            if len(index_lst) > 1:
                raise SystemExit("INFO -- calculating_rates takes specific Index -- NOT a pattern ... Skipped.")
            if len(index_lst) < 1:
                raise SystemExit("INFO -- Not indices found ... Skipped.")

            table = [['Index', "Time", 'Indexing rate per s', f'Indexing rate per {seconds}s', 'Searching rate per s', f'Searching rate per {seconds}s']]
    
            cnt = 0
            while cnt < times:
                # get current time
                current_time = datetime.now((timezone.utc)).strftime("%Y-%m-%d %H:%M:%S")

                start_time = time.time()
                start_index_total = self.get_index_stats(index).get('indices').get(index).get('total').get('indexing').get('index_total')
                start_search_total = self.get_index_stats(index).get('indices').get(index).get('total').get('search').get('query_total')

                print(f"{cnt + 1} - Calculating Indexing & Searching rates ...  [ Waiting for {seconds} seconds ]")

                time.sleep(seconds)
                end_time = time.time()
                end_index_total = self.get_index_stats(index).get('indices').get(index).get('total').get('indexing').get('index_total')
                end_search_total = self.get_index_stats(index).get('indices').get(index).get('total').get('search').get('query_total')

                # Calculate indexing rate
                indexing_time_difference = (int(end_time - start_time))
                indexing_per_n_seconds = str((end_index_total - start_index_total)) + " doc"
                indexing_per_second = (end_index_total - start_index_total) / indexing_time_difference
                indexing_per_second_ = str(math.ceil(indexing_per_second)) + " doc"

                # Calculate searching rate
                searching_time_difference = (int(end_time - start_time))
                searching_per_n_seconds = str((end_search_total - start_search_total)) + " doc"
                searching_per_second = (end_search_total - start_search_total) / searching_time_difference
                searching_per_second_ = str(math.ceil(searching_per_second)) + " doc"

    
                # Append the row to the table
                row = [index, current_time, indexing_per_second_, indexing_per_n_seconds, searching_per_second_, searching_per_n_seconds]
                table.append(row)   
                if not cnt == times -1:
                    print(f"\t> Waiting for interval: {interval}")
                time.sleep(interval)
                cnt +=1

            # Print a table
            out = tabulate(table, headers='firstrow', tablefmt='grid', showindex=True)

            print(out)

        except (opensearchpy.exceptions.NotFoundError,
                opensearchpy.exceptions.ConnectionError,
                opensearchpy.exceptions.AuthenticationException,
                opensearchpy.exceptions.AuthorizationException) as e:
            raise SystemExit(f"ERROR -- (calculate_indexing_rate) Unable to make an API call\n> {e}")

    def print_indices_table(self, index_pattern):
        """
        Print list of indices that match the index pattern with other custom chosen info (print a table)
        INPUT: Index or Index pattern
        """
        try:
            if Opensearch_Python.es_client is None:
                self.authenticate()


            table = [['Index', 'total P shards size', 'total shards number', 'P shards number', 'R shards number', '~ P shard size']]

            # Get the list of indeices
            indices_list = self.get_index(index_pattern)
            print(f"INFO -- The index pattern '{index_pattern}' matches '{len(indices_list)}' indices")

            # Get indices Json
            # ! Need to get the Json for each single index because the shards information only exists for total matched indices
            index_settings_json = self.get_index_settings(index_pattern)
            
            if len(indices_list) < 1:
                return "INFO -- No matching Indices found."

            self.progress_shards_list = Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(bar_width=30), 
                TaskProgressColumn(),
                TextColumn("{task.fields[status]}")
            )
            self.task_percentage  = self.progress_shards_list.add_task(
                    description=f"[b]Discovering Indices ",
                    status="...",
                    total=len(indices_list),
                    )

            with Live(self.progress_shards_list, auto_refresh=True, screen=False):
                cnt = 1
                for index in indices_list:

                    self.progress_shards_list.update(task_id=self.task_percentage, status=f" [ [yellow]{index}[/yellow] ]", completed=cnt)                            
                            
                    # Get index stats Json
                    index_stats_json = self.get_index_stats(index)

                    primary_shards_number = int(index_settings_json.get(index).get('settings').get('index').get('number_of_shards'))
                    replica_shards_number = int(index_settings_json.get(index).get('settings').get('index').get('number_of_replicas'))
                    total_shards_number = int(index_stats_json.get('_shards').get('total'))
                    total_primary_shards_size_bytes = index_stats_json.get('indices').get(index).get('primaries').get('store').get('size_in_bytes')
                    total_primary_shards_size = helper_.bytes_to_kb_mb_gb(total_primary_shards_size_bytes)
                    approximate_shard_size = 0
                    if primary_shards_number == 1:
                        approximate_shard_size = total_primary_shards_size
                    else:
                        approximate_shard_size = helper_.bytes_to_kb_mb_gb((total_primary_shards_size_bytes / primary_shards_number))
                    
                    # Create a list for each index
                    row = []
                    row.append(index)
                    row.append(total_primary_shards_size)
                    row.append(total_shards_number)
                    row.append(primary_shards_number)
                    row.append(replica_shards_number)
                    row.append(approximate_shard_size)
                    
                    # Append the index list to the table list
                    table.append(row)
                    cnt+=1

            out = tabulate(table, headers='firstrow', tablefmt='grid', showindex=True)
            print()
            print(out)
        except (opensearchpy.exceptions.NotFoundError,
                opensearchpy.exceptions.ConnectionError,
                opensearchpy.exceptions.AuthenticationException,
                opensearchpy.exceptions.AuthorizationException) as e:
            raise SystemExit(f"ERROR -- (calculate_indexing_rate) Unable to make an API call\n> {e}")

    def node_list_table(self, interval=1, watch=False):
        """
        """
        if Opensearch_Python.es_client is None:
            self.authenticate()
        
        def list_nodes_table():
            stats = self.node_stats()
            nodes = stats.get('nodes')

            table = [['Node ID', "Role", 'memory usage', 'cpu usage', 'disk usage' ,'available usage']]
            
            nodes_dct = {}

            # Loop over all the nodes to fetch needed data.
            for id, info in nodes.items():
                nodes_dct[id] = {}
                nodes_dct[id]['role'] = info.get('roles')[0]
                nodes_dct[id]['mem_usage'] = f"{info.get('os').get('mem').get('used_percent')}%"
                nodes_dct[id]['cpu_usage'] = f"{info.get('os').get('cpu').get('percent')}%"
                nodes_dct[id]['disk_usage'] = helper_.percentage(info.get('fs').get('total').get('total_in_bytes') - info.get('fs').get('total').get('available_in_bytes'), info.get('fs').get('total').get('total_in_bytes'))

            # Sort the dictionary (so that the table is displayed in the same order every time.)
            nodes_dct_sorted = dict(sorted(nodes_dct.items()))

            # Add master nodes first (to be displayed at the top)
            for id, v in nodes_dct_sorted.items():
                if v['role'] == 'master':
                    row = [id, v['role'], v['mem_usage'], v['cpu_usage'], v['disk_usage']]
                    table.append(row)
            
            # Add data nodes
            for id, v in nodes_dct_sorted.items():
                if v['role'] == 'data':
                    row = [id, v['role'], v['mem_usage'], v['cpu_usage'], v['disk_usage']]
                    table.append(row)
            
            out = tabulate(table, headers='firstrow', tablefmt='plain', showindex=False)
            return out

        if watch:
            try:
                with Live(screen=False, auto_refresh=False) as live:
                    
                    while True:
                        live.update(list_nodes_table(), refresh=True)
                        time.sleep(interval)
            except KeyboardInterrupt:
                print()
                exit(0)
        else:
            print(list_nodes_table())


    def node_monitor(self, node_id):
        """
        Live (Terminal) monitoring for Node resources
        INPUT:
            - Node ID
        """
        # Print loading because the layout may take few seconds to start (Probably due to slow connection)
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
                Layout(name="side",),
                Layout(name="body", ratio=3, minimum_size=60),
            )
            layout["side"].split(Layout(name="box1", )) # , Layout(name="box2")
            layout["body"].split(Layout(name="head", size=5, ratio=2), Layout(name="body1")) # , Layout(name="box2")

            return layout

        class Header():
            """
            Display header with clock.
            """
            def __rich__(self) -> Panel:
                grid = Table.grid(expand=True)
                grid.add_column(justify="center", ratio=1)
                grid.add_column(justify="right")
                grid.add_row(
                    f"[b]Node: [/b] {node_id} ",
                    datetime.now().ctime().replace(":", "[blink]:[/]"),
                )
                return Panel(grid, style="green")


        class Node_Resources_Progress(Node_Monitoring):
            def __init__(self):
                super().__init__()

                self.progress_start()

            def progress_start(self):
                nodes_json = self.node_stats().get('nodes')
                node_json = nodes_json.get(node_id)

                # Fail if the Node ID not found
                nodes_ids = []
                for id in nodes_json.keys():
                    nodes_ids.append(id)

                if node_id not in nodes_ids:
                    rich_print(f"[bold yellow]ERROR -- wrong Node ID: [underline]{node_id}[/underline]\n")
                    rich_print(f"[bold yellow]Available Nodes:\n")
                    self.node_list_table()
                    print()
                    exit(1)

                self.progress_swap_total = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(bar_width=20),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )

                self.progress_mem_total = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(bar_width=20),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )

                self.progress_fs_total = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(bar_width=20),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )

                self.progress_cpu_load = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(bar_width=20),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )

                self.progress_mem = Progress(TextColumn("[progress.description]{task.description}"),
                                                        BarColumn(bar_width=20), 
                                                        TaskProgressColumn(),
                                                        TextColumn("{task.fields[status]}"),
                                                        )

                self.progress_swap = Progress(TextColumn("[progress.description]{task.description}"),
                                                        BarColumn(bar_width=20), 
                                                        TaskProgressColumn(),
                                                        TextColumn("{task.fields[status]}"),
                                                        )

                self.progress_cpu = Progress(TextColumn("[progress.description]{task.description}"),
                                                        BarColumn(bar_width=30), 
                                                        TaskProgressColumn(),
                                                        # TextColumn("{task.fields[status]}"),
                                                        )

                self.progress_fs = Progress(TextColumn("[progress.description]{task.description}"),
                                                    BarColumn(bar_width=20),
                                                    TaskProgressColumn(),
                                                    TextColumn("{task.fields[status]}"),
                                                    )

                # Progress for JVM
                self.progress_jvm_percentage = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                    TextColumn("{task.fields[status]}"),
                )
                self.task_jvm_heap_used  = self.progress_jvm_percentage.add_task(
                    description=f"JVM heap used",
                    status="unknown",
                    )
            
                self.progress_jvm = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )
                self.task_jvm_heap_max  = self.progress_jvm.add_task(
                    description=f"JVM heap max",
                    status="unknown",
                    )
                self.task_jvm_uptime  = self.progress_jvm.add_task(
                    description=f"JVM uptime",
                    status="unknown",
                    )
                self.task_jvm_non_heap_used  = self.progress_jvm.add_task(
                    description=f"JVM non heap used",
                    status="unknown",
                    )
                self.task_jvm_threads  = self.progress_jvm.add_task(
                    description=f"JVM threads",
                    status="unknown",
                    )
                self.task_jvm_threads_max  = self.progress_jvm.add_task(
                    description=f"JVM threads max",
                    status="unknown",
                    )

                self.progress_process = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(bar_width=20),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )


                self.task_cpu_used = self.progress_cpu.add_task(description=f"[white]CPU used ", completed=node_json.get('os').get('cpu').get('percent'), total=100)
                self.task_cpu_load_avg_1m = self.progress_cpu_load.add_task(description=f"[white]CPU load avg 1m", status=f"{node_json.get('os').get('cpu').get('load_average').get('1m')}")
                self.task_cpu_load_avg_5m = self.progress_cpu_load.add_task(description=f"[white]CPU load avg 5m", status=f"{node_json.get('os').get('cpu').get('load_average').get('5m')}")
                self.task_cpu_load_avg_15m = self.progress_cpu_load.add_task(description=f"[white]CPU load avg 15m", status=f"{node_json.get('os').get('cpu').get('load_average').get('15m')}")

                self.task_swap_total = self.progress_swap_total.add_task(description=f"[white]Swap Total", status=f"{helper_.bytes_to_kb_mb_gb(node_json.get('os').get('swap').get('total_in_bytes'))}")
                self.task_swap_used = self.progress_swap.add_task(description=f"[white]Swap used", total=node_json.get('os').get('swap').get('total_in_bytes'), status=f"{helper_.bytes_to_kb_mb_gb(node_json.get('os').get('swap').get('used_in_bytes'))}")
                self.task_swap_free = self.progress_swap.add_task(description=f"[white]Swap free", total=node_json.get('os').get('swap').get('total_in_bytes'), status=f"{helper_.bytes_to_kb_mb_gb(node_json.get('os').get('swap').get('free_in_bytes'))}")

                self.task_mem_total = self.progress_mem_total.add_task(description=f"[white]Mem Total", status=f"{helper_.bytes_to_kb_mb_gb(node_json.get('os').get('mem').get('total_in_bytes'))}")
                self.task_mem_used = self.progress_mem.add_task(description=f"[white]Mem used", total=node_json.get('os').get('mem').get('total_in_bytes'), status=f"{helper_.bytes_to_kb_mb_gb(node_json.get('os').get('mem').get('used_in_bytes'))}")
                self.task_mem_free = self.progress_mem.add_task(description=f"[white]Mem free", total=node_json.get('os').get('mem').get('total_in_bytes'), status=f"{helper_.bytes_to_kb_mb_gb(node_json.get('os').get('mem').get('free_in_bytes'))}")

                self.task_fs_total = self.progress_fs_total.add_task(description=f"[white]FS total", status=f"{helper_.bytes_to_kb_mb_gb(node_json.get('fs').get('total').get('total_in_bytes'))}")
                self.task_fs_used = self.progress_fs.add_task(description=f"[white]FS used", total=node_json.get('fs').get('total').get('total_in_bytes'), status=f"{helper_.bytes_to_kb_mb_gb(node_json.get('fs').get('total').get('total_in_bytes') - node_json.get('fs').get('total').get('free_in_bytes'))}")
                self.task_fs_free = self.progress_fs.add_task(description=f"[white]FS free",total=node_json.get('fs').get('total').get('total_in_bytes'), status=f"{helper_.bytes_to_kb_mb_gb(node_json.get('fs').get('total').get('free_in_bytes'))}")

                self.task_process_open_file_descriptors = self.progress_process.add_task(description=f"[white]File_descriptors open", status=node_json.get('process').get('open_file_descriptors'))
                self.task_process_max_file_descriptors = self.progress_process.add_task(description=f"[white]File_descriptors max", status=node_json.get('process').get('max_file_descriptors'))

                self.progress_node_monitoring_threads_status = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )
                self.task_thread_node_monitoring  = self.progress_node_monitoring_threads_status.add_task(
                    description=f"Node Monitoring",
                    status="unknown",
                    )

                self.group  = Group(
                    self.progress_cpu,
                    self.progress_cpu_load,
                    Rule(style='#AAAAAA'),
                    self.progress_mem_total,
                    self.progress_mem,
                    Rule(style='#AAAAAA'),
                    self.progress_swap_total,
                    self.progress_swap,
                    Rule(style='#AAAAAA'),
                    self.progress_fs_total,
                    self.progress_fs,
                    Rule(style='#AAAAAA'),
                    self.progress_process
                )

                self.jvm_group = Group(
                    self.progress_jvm_percentage,
                    Rule(style='#AAAAAA'),
                    self.progress_jvm
                )

            def update(self):
                while True:
                    node_json = self.node_stats().get('nodes').get(node_id)

                    self.progress_mem_total.update(self.task_mem_total, description=f"[white]Mem Total", status=f"{helper_.bytes_to_kb_mb_gb(node_json.get('os').get('mem').get('total_in_bytes'))}")
                    self.progress_mem.update(self.task_mem_used, description=f"[white]Mem used",completed=node_json.get('os').get('mem').get('used_in_bytes'), visible=True, status=f"{helper_.bytes_to_kb_mb_gb(node_json.get('os').get('mem').get('used_in_bytes'))}")
                    self.progress_mem.update(self.task_mem_free, description=f"[white]Mem free",completed=node_json.get('os').get('mem').get('free_in_bytes'), visible=True, status=f"{helper_.bytes_to_kb_mb_gb(node_json.get('os').get('mem').get('free_in_bytes'))}")
                    
                    self.progress_swap_total.update(self.task_swap_total, description=f"[white]Swap Total", status=f"{helper_.bytes_to_kb_mb_gb(node_json.get('os').get('swap').get('total_in_bytes'))}")
                    self.progress_swap.update(self.task_swap_used, description=f"[white]Swap used",completed=node_json.get('os').get('swap').get('used_in_bytes'), visible=True, status=f"{helper_.bytes_to_kb_mb_gb(node_json.get('os').get('swap').get('used_in_bytes'))}")
                    self.progress_swap.update(self.task_swap_free, description=f"[white]Swap free",completed=node_json.get('os').get('swap').get('free_in_bytes'), visible=True, status=f"{helper_.bytes_to_kb_mb_gb(node_json.get('os').get('swap').get('free_in_bytes'))}")


                    self.progress_fs.update(self.task_fs_free, description=f"[white]FS free",completed=node_json.get('fs').get('total').get('free_in_bytes'), visible=True, status=f"{helper_.bytes_to_kb_mb_gb(node_json.get('fs').get('total').get('free_in_bytes'))}")
                    self.progress_fs.update(self.task_fs_used, description=f"[white]FS used",completed=(node_json.get('fs').get('total').get('total_in_bytes') - node_json.get('fs').get('total').get('free_in_bytes')), visible=True, status=f"{helper_.bytes_to_kb_mb_gb(node_json.get('fs').get('total').get('total_in_bytes') - node_json.get('fs').get('total').get('free_in_bytes'))}")

                    self.progress_cpu.update(self.task_cpu_used, description=f"[white]CPU used ",completed=node_json.get('os').get('cpu').get('percent'), visible=True)
                    self.progress_cpu_load.update(self.task_cpu_load_avg_1m, description=f"[white]CPU load avg 1m", visible=True, status=f"{node_json.get('os').get('cpu').get('load_average').get('1m')}")
                    self.progress_cpu_load.update(self.task_cpu_load_avg_5m, description=f"[white]CPU load avg 5m", visible=True, status=f"{node_json.get('os').get('cpu').get('load_average').get('5m')}")
                    self.progress_cpu_load.update(self.task_cpu_load_avg_15m, description=f"[white]CPU load avg 15m", visible=True, status=f"{node_json.get('os').get('cpu').get('load_average').get('15m')}")

                    self.progress_process.update(self.task_process_open_file_descriptors, description=f"[white]File_descriptors open", visible=True, status=node_json.get('process').get('open_file_descriptors'))
                    self.progress_process.update(self.task_process_max_file_descriptors, description=f"[white]File_descriptors max", visible=True, status=node_json.get('process').get('max_file_descriptors'))

                    self.progress_jvm.update(task_id=self.task_jvm_uptime, status=helper_.millisec_to_d_h_m(node_json.get('jvm').get('uptime_in_millis')))                    
                    self.progress_jvm.update(task_id=self.task_jvm_threads, status=node_json.get('jvm').get('threads').get('count'))
                    self.progress_jvm.update(task_id=self.task_jvm_threads_max, status=node_json.get('jvm').get('threads').get('peak_count'))
                    self.progress_jvm.update(task_id=self.task_jvm_heap_max, status=helper_.bytes_to_kb_mb_gb(node_json.get('jvm').get('mem').get('heap_max_in_bytes')))
                    self.progress_jvm.update(task_id=self.task_jvm_non_heap_used, status=helper_.bytes_to_kb_mb_gb(node_json.get('jvm').get('mem').get('non_heap_used_in_bytes')))                    
                    
                    self.progress_jvm_percentage.update(task_id=self.task_jvm_heap_used, status=helper_.bytes_to_kb_mb_gb(node_json.get('jvm').get('mem').get('heap_used_in_bytes')), completed=node_json.get('jvm').get('mem').get('heap_used_percent') ,total=100)                    

                    time.sleep(Attributes.mointoring_interval)

            def check_thread_node_resources(self, restart=True):
                while True:
                    def thread_status():
                        status = ""
                        if self.thread_node_resources.is_alive():
                            status = f"alive [green]✔️"
                        else:
                            status = "dead [red]❌"
                            if restart:
                                # Restart thread
                                self.start_threads()
                        return status

                    self.progress_node_monitoring_threads_status .update(task_id=self.task_thread_node_monitoring, status=thread_status())                            
                    time.sleep(5)
            
            def start_threads(self):
                self.thread_node_resources = threading.Thread(target=self.update)
                self.thread_node_resources.daemon = True
                self.thread_node_resources.start()

            def watch_threads(self):
                self.thread_check_thread_node_resources = threading.Thread(target=self.check_thread_node_resources)
                self.thread_check_thread_node_resources.daemon = True
                self.thread_check_thread_node_resources.start()


        class Rates_calculation_progress(Node_Monitoring):
            """
            Caclulate Indexing & Searching rates to be displayed in a layout.
            """              

            def __init__(self):
                super().__init__()

                self.progress_start()
                
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

                # Read operations attributes
                self.read_operations_Ns = 0
                self.read_operations_1s = 0
                self.maximum_read_operations_1s = 0
                self.minimum_read_operations_1s = 999999999999999
                self.read_operations_average = 0

                # Read KB attributes
                self.read_kb_Ns = 0
                self.read_kb_1s = 0
                self.maximum_read_kb_1s = 0
                self.minimum_read_kb_1s = 999999999999999
                self.read_kb_average = 0

                # Write operations attributes
                self.write_operations_Ns = 0
                self.write_operations_1s = 0
                self.maximum_write_operations_1s = 0
                self.minimum_write_operations_1s = 999999999999999
                self.write_operations_average = 0

                # Write KB attributes
                self.write_kb_Ns = 0
                self.write_kb_1s = 0
                self.maximum_write_kb_1s = 0
                self.minimum_write_kb_1s = 999999999999999
                self.write_kb_average = 0

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

                # Fetch Rate attributes
                self.fetch_rate_60s = 0
                self.fetch_rate_1s = 0
                self.maximum_fetch_rate_1s = 0
                self.minimum_fetch_rate_1s = 999999999999999
                self.fetch_rate_average = 0

                # Fetch Latency attributes
                self.fetch_time_60s = 0
                self.fetch_time_1s = 0
                self.maximum_fetch_time_1s = 0
                self.minimum_fetch_time_1s = 999999999999999
                self.fetch_time_average = 0


            def progress_start(self, rate_update_interval=Attributes.rate_update_interval):

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

                # Progress for Indexing Rate
                self.progress_indexing_rate = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )

                self.task_2 = self.progress_indexing_rate.add_task(
                    description=f"Rate ({rate_update_interval}s)",
                    status="waiting",
                    )
                self.task_3 = self.progress_indexing_rate.add_task(
                    description=f"Rate (1s)",
                    status="waiting",
                    )
                self.task_4 = self.progress_indexing_rate.add_task(
                    description=f"Rate MIN",
                    status="waiting",
                    )
                self.task_5 = self.progress_indexing_rate.add_task(
                    description=f"Rate MAX",
                    status="waiting",
                    )
                self.task_6 = self.progress_indexing_rate.add_task(
                    description=f"Rate AVG",
                    status="waiting",
                    )

                # Progress for Searching Rate
                self.progress_searching_rate = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )

                self.task_2 = self.progress_searching_rate.add_task(
                    description=f"Rate ({rate_update_interval}s)",
                    status="waiting",
                    )
                self.task_3 = self.progress_searching_rate.add_task(
                    description=f"Rate (1s)",
                    status="waiting",
                    )
                self.task_4 = self.progress_searching_rate.add_task(
                    description=f"Rate MIN",
                    status="waiting",
                    )
                self.task_5 = self.progress_searching_rate.add_task(
                    description=f"Rate MAX",
                    status="waiting",
                    )
                self.task_6 = self.progress_searching_rate.add_task(
                    description=f"Rate AVG",
                    status="waiting",
                    )

                # Progress for Fetch Rate
                self.progress_fetch_rate = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )

                self.task_fetch_rate_n_seconds = self.progress_fetch_rate.add_task(
                    description=f"Rate ({rate_update_interval}s)",
                    status="waiting",
                    )
                self.task_fetch_rate_1sec = self.progress_fetch_rate.add_task(
                    description=f"Rate (1s)",
                    status="waiting",
                    )
                self.task_fetch_rate_min = self.progress_fetch_rate.add_task(
                    description=f"Rate MIN",
                    status="waiting",
                    )
                self.task_fetch_rate_max = self.progress_fetch_rate.add_task(
                    description=f"Rate MAX",
                    status="waiting",
                    )
                self.task_fetch_rate_avg = self.progress_fetch_rate.add_task(
                    description=f"Rate AVG",
                    status="waiting",
                    )

                self.progress_io_state_read = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )
                self.task_read_operations  = self.progress_io_state_read.add_task(
                    description=f"Read operations",
                    status="waiting",
                    )
                self.task_read_operations_min  = self.progress_io_state_read.add_task(
                    description=f"Read operations MIN",
                    status="waiting",
                    )
                self.task_read_operations_max  = self.progress_io_state_read.add_task(
                    description=f"Read operations MAX",
                    status="waiting",
                    )
                self.task_read_operations_avg  = self.progress_io_state_read.add_task(
                    description=f"Read operations AVG",
                    status="waiting",
                    )

                self.task_read_kb_1sec  = self.progress_io_state_read.add_task(
                    description=f"Read kb",
                    status="waiting",
                    )
                self.task_read_kb_min  = self.progress_io_state_read.add_task(
                    description=f"Read kb MIN",
                    status="waiting",
                    )
                self.task_read_kb_max  = self.progress_io_state_read.add_task(
                    description=f"Read kb MAX",
                    status="waiting",
                    )
                self.task_read_kb_avg  = self.progress_io_state_read.add_task(
                    description=f"Read kb AVG",
                    status="waiting",
                    )


                self.progress_io_state_write = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )

                self.task_write_operations  = self.progress_io_state_write.add_task(
                    description=f"Write operations",
                    status="waiting",
                    )

                self.task_write_operations_min  = self.progress_io_state_write.add_task(
                    description=f"Write operations MIN",
                    status="waiting",
                    )
                self.task_write_operations_max  = self.progress_io_state_write.add_task(
                    description=f"Write operations MAX",
                    status="waiting",
                    )
                self.task_write_operations_avg  = self.progress_io_state_write.add_task(
                    description=f"Write operations AVG",
                    status="waiting",
                    )
                self.task_write_kb_1sec  = self.progress_io_state_write.add_task(
                    description=f"Write kb",
                    status="waiting",
                    )
                self.task_write_kb_min  = self.progress_io_state_write.add_task(
                    description=f"Write kb MIN",
                    status="waiting",
                    )
                self.task_write_kb_max  = self.progress_io_state_write.add_task(
                    description=f"Write kb MAX",
                    status="waiting",
                    )
                self.task_write_kb_avg  = self.progress_io_state_write.add_task(
                    description=f"Write kb AVG",
                    status="waiting",
                    )
                
                
                self.progress_threads_status = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )
                self.task_thread_rate_observation  = self.progress_threads_status.add_task(
                    description=f"Rate Observation",
                    status="unknown",
                    )
                self.task_thread_rate_update  = self.progress_threads_status.add_task(
                    description=f"Rate Update",
                    status="unknown",
                    )

                # Progress for Searching Latency
                self.progress_searching_latency = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )
                self.task_search_latency_n_seconds  = self.progress_searching_latency.add_task(
                    description=f"Query time ({Attributes.rate_update_interval}s)",
                    status="waiting",
                    )
                self.task_search_latency_1sec  = self.progress_searching_latency.add_task(
                    description=f"Query time (1s)",
                    status="waiting",
                    )
                self.task_search_latency_min  = self.progress_searching_latency.add_task(
                    description=f"Query time MIN",
                    status="waiting",
                    )
                self.task_search_latency_max  = self.progress_searching_latency.add_task(
                    description=f"Query time MAX",
                    status="waiting",
                    )
                self.task_search_latency_avg  = self.progress_searching_latency.add_task(
                    description=f"Query time AVG",
                    status="waiting",
                    )


                # Progress for Indexing Latency
                self.progress_indexing_latency = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )
                self.task_indexing_latency_n_seconds  = self.progress_indexing_latency.add_task(
                    description=f"Indexing time ({Attributes.rate_update_interval}s)",
                    status="waiting",
                    )
                self.task_indexing_latency_1sec  = self.progress_indexing_latency.add_task(
                    description=f"Indexing time (1s)",
                    status="waiting",
                    )
                self.task_indexing_latency_min  = self.progress_indexing_latency.add_task(
                    description=f"Indexing time MIN",
                    status="waiting",
                    )
                self.task_indexing_latency_max  = self.progress_indexing_latency.add_task(
                    description=f"Indexing time MAX",
                    status="waiting",
                    )
                self.task_indexing_latency_avg  = self.progress_indexing_latency.add_task(
                    description=f"Indexing time AVG",
                    status="waiting",
                    )

                # Progress for Fetch Latency
                self.progress_fetch_latency = Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    # TextColumn("[progress.percentage]{task.percentage:>3.0f}"),
                    TextColumn("{task.fields[status]}"),
                )
                self.task_fetch_latency_n_seconds  = self.progress_fetch_latency.add_task(
                    description=f"Fetch time ({Attributes.rate_update_interval}s)",
                    status="waiting",
                    )
                self.task_fetch_latency_1sec  = self.progress_fetch_latency.add_task(
                    description=f"Fetch time (1s)",
                    status="waiting",
                    )
                self.task_fetch_latency_min  = self.progress_fetch_latency.add_task(
                    description=f"Fetch time MIN",
                    status="waiting",
                    )
                self.task_fetch_latency_max  = self.progress_fetch_latency.add_task(
                    description=f"Fetch time MAX",
                    status="waiting",
                    )
                self.task_fetch_latency_avg  = self.progress_fetch_latency.add_task(
                    description=f"Fetch time AVG",
                    status="waiting",
                    )

                self.io_group  = Group(
                    self.progress_io_state_read,
                    Rule(style='#AAAAAA'),
                    self.progress_io_state_write,
                )

            def observe_data(self, task_id, sleep_time, seconds=Attributes.rate_update_interval, interval=20):
                # Wait for N seconds
                cnt = 0
                while True:
                    # Get readings
                    # current_time = datetime.now((timezone.utc)).strftime("%Y-%m-%d %H:%M:%S")
                    node_json = self.node_stats(node_id)
                    start_time = time.time()
                    start_indexing_total = node_json.get('nodes').get(node_id).get('indices').get('indexing').get('index_total')
                    start_searching_total = node_json.get('nodes').get(node_id).get('indices').get('search').get('query_total')
                    start_fetch_total = node_json.get('nodes').get(node_id).get('indices').get('search').get('fetch_total')

                    start_read_operations = node_json.get('nodes').get(node_id).get('fs').get('io_stats').get('total').get('read_operations')
                    start_write_operations = node_json.get('nodes').get(node_id).get('fs').get('io_stats').get('total').get('write_operations')

                    start_read_kb = node_json.get('nodes').get(node_id).get('fs').get('io_stats').get('total').get('read_kilobytes')
                    start_write_kb = node_json.get('nodes').get(node_id).get('fs').get('io_stats').get('total').get('write_kilobytes')
 
                    start_indexing_time_ms = node_json.get('nodes').get(node_id).get('indices').get('indexing').get('index_time_in_millis')
                    start_searching_time_ms = node_json.get('nodes').get(node_id).get('indices').get('search').get('query_time_in_millis')
                    start_fetch_time_ms = node_json.get('nodes').get(node_id).get('indices').get('search').get('fetch_time_in_millis')


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
                            node_json = self.node_stats(node_id)
                            end_time = time.time()
                            end_indexing_total = node_json.get('nodes').get(node_id).get('indices').get('indexing').get('index_total')
                            end_searching_total = node_json.get('nodes').get(node_id).get('indices').get('search').get('query_total')
                            end_fetch_total = node_json.get('nodes').get(node_id).get('indices').get('search').get('fetch_total')
        
                            end_read_operations = node_json.get('nodes').get(node_id).get('fs').get('io_stats').get('total').get('read_operations')
                            end_write_operations = node_json.get('nodes').get(node_id).get('fs').get('io_stats').get('total').get('write_operations')

                            end_read_kb = node_json.get('nodes').get(node_id).get('fs').get('io_stats').get('total').get('read_kilobytes')
                            end_write_kb = node_json.get('nodes').get(node_id).get('fs').get('io_stats').get('total').get('write_kilobytes')

                            end_indexing_time_ms = node_json.get('nodes').get(node_id).get('indices').get('indexing').get('index_time_in_millis')
                            end_searching_time_ms = node_json.get('nodes').get(node_id).get('indices').get('search').get('query_time_in_millis')
                            end_fetch_time_ms = node_json.get('nodes').get(node_id).get('indices').get('search').get('fetch_time_in_millis')

                            time_difference = (int(end_time - start_time))
                            Attributes.total_observation_time = time_difference

                            #### Update the attributes with the calculation results ###

                            # Indexing / Searching Rate
                            self.indexing_rate_60s = str((end_indexing_total - start_indexing_total)) + " doc"
                            self.indexing_rate_1s = (end_indexing_total - start_indexing_total) / time_difference
                            
                            self.searching_rate_60s = str((end_searching_total - start_searching_total)) + " doc"
                            self.searching_rate_1s = (end_searching_total - start_searching_total) / time_difference

                            self.fetch_rate_60s = (end_fetch_total - start_fetch_total)
                            self.fetch_rate_1s = (end_fetch_total - start_fetch_total) / time_difference

                            self.indexing_time_60s = (end_indexing_time_ms - start_indexing_time_ms)
                            self.indexing_time_1s = (end_indexing_time_ms - start_indexing_time_ms) / time_difference

                            self.searching_time_60s = (end_searching_time_ms - start_searching_time_ms)
                            self.searching_time_1s = (end_searching_time_ms - start_searching_time_ms) / time_difference

                            self.fetch_time_60s = (end_fetch_time_ms - start_fetch_time_ms) 
                            self.fetch_time_1s = (end_fetch_time_ms - start_fetch_time_ms) / time_difference

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

                            # Searching / Indexing Latency
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
                            
                            # Fetch Rate
                            if self.maximum_fetch_rate_1s < self.fetch_rate_1s:
                                self.maximum_fetch_rate_1s = self.fetch_rate_1s

                            if self.fetch_rate_1s < self.minimum_fetch_rate_1s:
                                self.minimum_fetch_rate_1s = self.fetch_rate_1s
                            
                            if self.maximum_fetch_rate_1s > self.minimum_fetch_rate_1s:
                                self.fetch_rate_average = (self.maximum_fetch_rate_1s + self.minimum_fetch_rate_1s) / 2

                            # Fetch Latency
                            if self.maximum_fetch_time_1s < self.fetch_time_1s:
                                self.maximum_fetch_time_1s = self.fetch_time_1s

                            if self.fetch_time_1s < self.minimum_fetch_time_1s:
                                self.minimum_fetch_time_1s = self.fetch_time_1s

                            if self.maximum_fetch_time_1s > self.minimum_fetch_time_1s:
                                self.fetch_time_average = (self.maximum_fetch_time_1s + self.minimum_fetch_time_1s) / 2

                            # Read / Write Operations
                            self.read_operations_1s = (end_read_operations - start_read_operations) / time_difference
                            self.write_operations_1s = (end_write_operations - start_write_operations) / time_difference

                            if self.maximum_read_operations_1s < self.read_operations_1s:
                                self.maximum_read_operations_1s = self.read_operations_1s

                            if self.read_operations_1s < self.minimum_read_operations_1s:
                                self.minimum_read_operations_1s = self.read_operations_1s

                            if self.maximum_write_operations_1s < self.write_operations_1s:
                                self.maximum_write_operations_1s = self.write_operations_1s

                            if self.write_operations_1s < self.minimum_write_operations_1s:
                                self.minimum_write_operations_1s = self.write_operations_1s

                            if self.maximum_read_operations_1s > self.minimum_read_operations_1s:
                                self.read_operations_average = (self.maximum_read_operations_1s + self.minimum_read_operations_1s) / 2

                            if self.maximum_write_operations_1s > self.minimum_write_operations_1s:
                                self.write_operations_average = (self.maximum_write_operations_1s + self.minimum_write_operations_1s) / 2


                            # Read / Write KBs
                            self.read_kb_1s = (end_read_kb - start_read_kb) / time_difference
                            self.write_kb_1s = (end_write_kb - start_write_kb) / time_difference
                            # Read
                            if self.maximum_read_kb_1s < self.read_kb_1s:
                                self.maximum_read_kb_1s = self.read_kb_1s

                            if self.read_kb_1s < self.minimum_read_kb_1s:
                                self.minimum_read_kb_1s = self.read_kb_1s
                            # Write
                            if self.maximum_write_kb_1s < self.write_kb_1s:
                                self.maximum_write_kb_1s = self.write_kb_1s

                            if self.write_kb_1s < self.minimum_write_kb_1s:
                                self.minimum_write_kb_1s = self.write_kb_1s
                            # Average
                            if self.maximum_read_kb_1s > self.minimum_read_kb_1s:
                                self.read_kb_average = (self.maximum_read_kb_1s + self.minimum_read_kb_1s) / 2

                            if self.maximum_write_kb_1s > self.minimum_write_kb_1s:
                                self.write_kb_average = (self.maximum_write_kb_1s + self.minimum_write_kb_1s) / 2
                            
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
                            self.progress_indexing_rate.update(description=f"Rate ({Attributes.total_observation_time}s)", task_id=self.task_2, status=f"{self.indexing_rate_60s}")                            
                            self.progress_indexing_rate.update(task_id=self.task_3, status=f"{str(math.ceil(self.indexing_rate_1s))} doc")
                            self.progress_indexing_rate.update(task_id=self.task_4, status=f"{str(math.ceil(self.minimum_indexing_rate_1s))} doc /s")
                            self.progress_indexing_rate.update(task_id=self.task_5, status=f"{str(math.ceil(self.maximum_indexing_rate_1s))} doc /s")
                            # Update Searching rate
                            self.progress_searching_rate.update(description=f"Rate ({Attributes.total_observation_time}s)", task_id=self.task_2, status=f"{self.searching_rate_60s}")
                            self.progress_searching_rate.update(task_id=self.task_3, status=f"{str(math.ceil(self.searching_rate_1s))} doc")
                            self.progress_searching_rate.update(task_id=self.task_4, status=f"{str(math.ceil(self.minimum_searching_rate_1s))} doc /s")
                            self.progress_searching_rate.update(task_id=self.task_5, status=f"{str(math.ceil(self.maximum_searching_rate_1s))} doc /s")
                            # Update Fetch rate
                            self.progress_fetch_rate.update(description=f"Rate ({Attributes.total_observation_time}s)", task_id=self.task_fetch_rate_n_seconds, status=f"{math.ceil(self.fetch_rate_60s)} doc")
                            self.progress_fetch_rate.update(task_id=self.task_fetch_rate_1sec, status=f"{str(math.ceil(self.fetch_rate_1s))} doc")
                            self.progress_fetch_rate.update(task_id=self.task_fetch_rate_min, status=f"{str(math.ceil(self.minimum_fetch_rate_1s))} doc /s")
                            self.progress_fetch_rate.update(task_id=self.task_fetch_rate_max, status=f"{str(math.ceil(self.maximum_fetch_rate_1s))} doc /s")
                            # Update Read operations
                            self.progress_io_state_read.update(task_id=self.task_read_operations, status=f"{math.ceil(self.read_operations_1s)} /s")
                            self.progress_io_state_read.update(task_id=self.task_read_operations_min, status=f"{math.ceil(self.minimum_read_operations_1s)} /s")
                            self.progress_io_state_read.update(task_id=self.task_read_operations_max, status=f"{math.ceil(self.maximum_read_operations_1s)} /s")
                            # Update Read KBs
                            self.progress_io_state_read.update(task_id=self.task_read_kb_1sec, status=f"{helper_.kb_to_mb(self.read_kb_1s)} /s")
                            self.progress_io_state_read.update(task_id=self.task_read_kb_min, status=f"{helper_.kb_to_mb(self.minimum_read_kb_1s)} /s")
                            self.progress_io_state_read.update(task_id=self.task_read_kb_max, status=f"{helper_.kb_to_mb(self.maximum_read_kb_1s)} /s")
                            # Update Write operations
                            self.progress_io_state_write.update(task_id=self.task_write_operations, status=f"{math.ceil(self.write_operations_1s)} /s")
                            self.progress_io_state_write.update(task_id=self.task_write_operations_min, status=f"{math.ceil(self.minimum_write_operations_1s)} /s")
                            self.progress_io_state_write.update(task_id=self.task_write_operations_max, status=f"{math.ceil(self.maximum_write_operations_1s)} /s")
                            # Update Write KBs
                            self.progress_io_state_write.update(task_id=self.task_write_kb_1sec, status=f"{helper_.kb_to_mb(self.write_kb_1s)} /s")
                            self.progress_io_state_write.update(task_id=self.task_write_kb_min, status=f"{helper_.kb_to_mb(self.minimum_write_kb_1s)} /s")
                            self.progress_io_state_write.update(task_id=self.task_write_kb_max, status=f"{helper_.kb_to_mb(self.maximum_write_kb_1s)} /s")
                            # Update Searching time
                            self.progress_searching_latency.update(description=f"Query time ({Attributes.total_observation_time}s)", task_id=self.task_search_latency_n_seconds, status=helper_.millisec_to_sec_m_h(self.searching_time_60s))
                            self.progress_searching_latency.update(task_id=self.task_search_latency_1sec, status=helper_.millisec_to_sec_m_h(self.searching_time_1s))
                            self.progress_searching_latency.update(task_id=self.task_search_latency_min, status=helper_.millisec_to_sec_m_h(self.minimum_searching_time_1s))
                            self.progress_searching_latency.update(task_id=self.task_search_latency_max, status=helper_.millisec_to_sec_m_h(self.maximum_searching_time_1s))
                            # Update Indexing time
                            self.progress_indexing_latency.update(description=f"Indexing time ({Attributes.total_observation_time}s)", task_id=self.task_indexing_latency_n_seconds, status=helper_.millisec_to_sec_m_h(self.indexing_time_60s))
                            self.progress_indexing_latency.update(task_id=self.task_indexing_latency_1sec, status=helper_.millisec_to_sec_m_h(self.indexing_time_1s))
                            self.progress_indexing_latency.update(task_id=self.task_indexing_latency_min, status=helper_.millisec_to_sec_m_h(self.minimum_indexing_time_1s))
                            self.progress_indexing_latency.update(task_id=self.task_indexing_latency_max, status=helper_.millisec_to_sec_m_h(self.maximum_indexing_time_1s))
                            # Update Fetch time
                            self.progress_fetch_latency.update(description=f"Fetch time ({Attributes.total_observation_time}s)", task_id=self.task_fetch_latency_n_seconds, status=helper_.millisec_to_sec_m_h(self.fetch_time_60s))
                            self.progress_fetch_latency.update(task_id=self.task_fetch_latency_1sec, status=helper_.millisec_to_sec_m_h(self.fetch_time_1s))
                            self.progress_fetch_latency.update(task_id=self.task_fetch_latency_min, status=helper_.millisec_to_sec_m_h(self.minimum_fetch_time_1s))
                            self.progress_fetch_latency.update(task_id=self.task_fetch_latency_max, status=helper_.millisec_to_sec_m_h(self.maximum_fetch_time_1s))


                        if not self.indexing_rate_average == 0:
                            self.progress_indexing_rate.update(description=f"Rate [green]AVG", task_id=self.task_6, status=f"{str(math.ceil(self.indexing_rate_average))} doc /s")

                        if not self.searching_rate_average == 0:
                            self.progress_searching_rate.update(description=f"Rate [green]AVG",task_id=self.task_6, status=f"{str(math.ceil(self.searching_rate_average))} doc /s")

                        if not self.fetch_rate_average == 0:
                            self.progress_fetch_rate.update(description=f"Rate [green]AVG",task_id=self.task_fetch_rate_avg, status=f"{str(math.ceil(self.fetch_rate_average))} doc /s")

                        if not self.read_operations_average == 0:
                            self.progress_io_state_read.update(description=f"Read operations [green]AVG", task_id=self.task_read_operations_avg, status=f"{math.ceil(self.read_operations_average)} /s")

                        if not self.read_kb_average == 0:
                            self.progress_io_state_read.update(description=f"Read kb [green]AVG", task_id=self.task_read_kb_avg, status=f"{helper_.kb_to_mb(self.read_kb_average)} /s")
   
                        if not self.write_operations_average == 0:
                            self.progress_io_state_write.update(description=f"Write operations [green]AVG", task_id=self.task_write_operations_avg, status=f"{math.ceil(self.write_operations_average)} /s")

                        if not self.write_kb_average == 0:
                            self.progress_io_state_write.update(description=f"Write kb [green]AVG", task_id=self.task_write_kb_avg, status=f"{helper_.kb_to_mb(self.write_kb_average)} /s")

                        if not self.indexing_time_average == 0:
                            self.progress_indexing_latency.update(description=f"Indexing time [green]AVG", task_id=self.task_indexing_latency_avg, status=helper_.millisec_to_sec_m_h(self.indexing_time_average))

                        if not self.searching_time_average == 0:
                            self.progress_searching_latency.update(description=f"Query time [green]AVG", task_id=self.task_search_latency_avg, status=helper_.millisec_to_sec_m_h(self.searching_rate_average))

                        if not self.fetch_time_average == 0:
                            self.progress_fetch_latency.update(description=f"Fetch time [green]AVG", task_id=self.task_fetch_latency_avg, status=helper_.millisec_to_sec_m_h(self.fetch_time_average))

            def check_threads_status(self, restart=True):
                while True:
                    def thread_status(thread):
                        status = ""
                        if thread.is_alive():
                            status = f"alive [green]✔️"
                        else:
                            status = "dead [red]❌"
                            if restart:
                                self.start_threads()
                        return status
                    
                    self.progress_threads_status.update(task_id=self.task_thread_rate_update, status=thread_status(self.thread_rate_update))                            
                    self.progress_threads_status.update(task_id=self.task_thread_rate_observation, status=thread_status(self.thread_rate_observation))
                    time.sleep(5)


            def start_threads(self):
                # Thread for observing data (and updating the attributes)
                self.thread_rate_observation = threading.Thread(target=self.observe_data, args=(self.task_1, 1))
                self.thread_rate_observation.daemon = True
                self.thread_rate_observation.start()
                # And thread for updating the the progress bars every N seconds (from the attributes)
                self.thread_rate_update = threading.Thread(target=self.update_rates)
                self.thread_rate_update.daemon = True
                self.thread_rate_update.start()

            def watch_threads(self):
                self.thread_check_thread_rate_observation = threading.Thread(target=self.check_threads_status)
                self.thread_check_thread_rate_observation.daemon = True
                self.thread_check_thread_rate_observation.start()

            ### No need to join the threads ###
            # def join_threads(self):
            #     self.thread_rate_observation.join()
            #     self.thread_rate_update.join()
            #     self.t3.join()

        
        try:
            rates_calculation_progress = Rates_calculation_progress()
            node_resources_progress = Node_Resources_Progress()

            # md = Markdown(f"```bash cd /home```")
            # Panel(Console.print(md)), title="[b]Indexing Rate", padding=(1, 2)),


            progress_table = Table.grid(expand=True)
            progress_table.add_row(
                Panel(node_resources_progress.group, title="[b]OS Resources", padding=(1, 2)),
                # Panel(rates_calculation_progress.progress_read_operations, title="[b]IO State", padding=(1, 2)),
                # Panel(progress_fs, title="[b]CPU", border_style="green", padding=(1, 2)),
            )
            progress_table.add_row(
                Panel(rates_calculation_progress.io_group, title="[b]IO State", padding=(1, 2)),
            )

            thread_status_group = Group(
                rates_calculation_progress.progress_threads_status,
                node_resources_progress.progress_node_monitoring_threads_status
            )

            rates_progress_obervation = Table.grid(expand=True)
            rates_progress_obervation.add_row(
                Panel(rates_calculation_progress.progress, title="[b]Observing Data", padding=(1, 2),  width=89),
                Panel(thread_status_group, title="[b]Threads status", width=44),
                )

            rates_progress_table = Table.grid(expand=True)
            rates_progress_table.add_row(
                Panel(rates_calculation_progress.progress_indexing_rate, title="[b]Indexing Rate", padding=(1, 2)),
                Panel(rates_calculation_progress.progress_searching_rate, title="[b]Searching Rate", padding=(1, 2)),
                Panel(rates_calculation_progress.progress_fetch_rate, title="[b]Fetch Rate", padding=(1, 2)),
                )
            rates_progress_table.add_row(
                Panel(rates_calculation_progress.progress_indexing_latency, title="[b]Indexing Latency", padding=(1, 2)),
                Panel(rates_calculation_progress.progress_searching_latency, title="[b]Searching Latency", padding=(1, 2)),
                Panel(rates_calculation_progress.progress_fetch_latency, title="[b]Fetch Latency", padding=(1, 2)),
            )


            rates_progress_table.add_row(
                Panel(node_resources_progress.jvm_group, title="[b]JVM"),
                # Panel(rates_calculation_progress.progress_searching_latency, title="[b]Fefresh Rate", padding=(1, 2)),
                # Panel(rates_calculation_progress.progress_searching_latency, title="[b]? Rate", padding=(1, 2)),

            )

            # rates_progress_table.add_row(
            #     Panel("", title="", padding=(1, 2)),
            #     Panel(rates_calculation_progress.progress_searching_latency, title="[b]Fefresh Latency", padding=(1, 2)),
            #     Panel(rates_calculation_progress.progress_fetch_latency, title="[b]? Latency", padding=(1, 2)),
            # )
            




            threads_status_table = Table.grid(expand=True)
            threads_status_table.add_row(
                Panel(rates_calculation_progress.progress_threads_status, title="[b]Monitoring Threads status", padding=(1, 2)),
                # Panel("", title="[b]not used", padding=(1, 2)),
                # Panel("", title="[b]not used", padding=(1, 2)),
                )

            # rates_progress_table2 = Table.grid(expand=True)
            # rates_progress_table2.add_row(Panel(rates_calculation_progress2.progress, title="[b]Indexing Rate", padding=(1, 2)),
            #                                 # Panel(progress, title="[b]CPU", border_style="green", padding=(1, 2)),
            # )


            # panel_group = Group(
            #     # node_resources_progress.progress_mem_total,
            #     node_resources_progress.progress_mem,
            #     node_resources_progress.progress_fs,
            #     # Panel(node_resources_progress.group, title="[b]Indexing Rate", padding=(1, 2)),
            # )
            layout = make_layout()
            layout["header"].update(Header())
            layout["head"].update(rates_progress_obervation)
            layout["body1"].update(rates_progress_table)
            # layout["body2"].update(rates_progress_table2)
            layout["box1"].update(progress_table)
            # layout["box1"].update(Panel(bar.start()))
            # layout["box1"].update(Panel(progress_table, border_style="yellow"))

            # layout["footer"].update(Panel(rates_progress_obervation))
            # layout["footer"].update(Panel(threads_status_table))

            # layout["box1"].update())

            node_resources_progress.start_threads()
            node_resources_progress.watch_threads()

            rates_calculation_progress.start_threads()
            rates_calculation_progress.watch_threads()



            with Live(layout, auto_refresh=True, screen=True):
                    while True:
                        time.sleep(30)
                    
        except (KeyboardInterrupt):
            rich_print(layout)
            exit(0)
        except (SystemExit) as e:
            exit(1)
        except (opensearchpy.exceptions.AuthorizationException,
                opensearchpy.exceptions.ConnectionTimeout,
                opensearchpy.exceptions.ConnectionTimeout) as e:
            raise SystemExit(f"ERROR -- connection failed with openSearch\n> {e}")
