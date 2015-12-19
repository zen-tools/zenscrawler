import re
import sys
import time
import pycurl
import threading
from io import BytesIO
from bs4 import BeautifulSoup
from queue import Queue
from collections import deque
from urllib.parse import urljoin
from zenscrawler.utils import Singleton


class PageParser(object):
    user_agent = "Mozilla/4.0 (compatible; ZensBot/0.0.1)"
    headers = {}
    worker_task = None
    match_domain = None
    match_content = None

    def __init__(self, task, match_domain, match_content='text/html'):
        assert isinstance(task, Task), \
            "%r is not a Task" % task

        self.worker_task = task
        self.match_domain = match_domain
        self.match_content = match_content

    def __header_function(self, header_line):
        header_line = header_line.decode('iso-8859-1')

        if ':' not in header_line:
            return

        name, value = header_line.split(':', 1)

        name = name.strip().lower()
        value = value.strip()

        self.headers[name] = value

    def parse(self, tag):
        buffer = BytesIO()
        c = pycurl.Curl()
        c.setopt(c.USERAGENT, self.user_agent)
        c.setopt(c.URL, self.worker_task.url_target)
        c.setopt(c.HEADERFUNCTION, self.__header_function)
        c.setopt(c.WRITEDATA, buffer)

        # TODO: load form db url's last modified date
        # and send it in headers request
        # c.setopt(c.HTTPHEADER, ['If-Modified-Since: ... '])

        # TODO: Timeouts should be configurable
        # c.setopt(c.CONNECTTIMEOUT_MS, 5000)
        # c.setopt(c.TIMEOUT_MS, 5000)

        c.setopt(c.FOLLOWLOCATION, True)
        c.setopt(c.SSL_VERIFYPEER, False)
        c.setopt(c.SSL_VERIFYHOST, False)
        c.perform()

        r_url = c.getinfo(c.EFFECTIVE_URL)
        r_code = c.getinfo(c.RESPONSE_CODE)
        # r_time = c.getinfo(c.TOTAL_TIME)
        # r_last_modified = self.headers['last-modified']
        c.close()

        # TODO: Save page loads metrics in db
        # r_url + r_code + r_time + r_last_modified

        # Return if we were redirected to another domain
        if not re.match(self.match_domain, r_url):
            return []

        # There is no sense to parse body when it's not modified
        # or when it's not accessible
        if r_code not in [200, 203, 301, 302]:
            return []

        mime_type = None
        encoding = None
        if 'content-type' in self.headers:
            content_type = self.headers['content-type'].lower()
            match = re.search('([^;]*); charset=(\S+)', content_type)
            if match:
                mime_type = match.group(1)
                encoding = match.group(2)
        if encoding is None:
            encoding = 'iso-8859-1'

        # Check for content-type
        if not mime_type or mime_type != self.match_content:
            return []

        r_body = buffer.getvalue().decode(encoding)

        links_list = list()
        soup = BeautifulSoup(r_body, 'html.parser')
        for link in soup.find_all(tag):
            url = urljoin(r_url, link.get('href'))
            links_list.append(url)

        return links_list


class TasksList(object, metaclass=Singleton):
    tasks = deque()

    def __init__(self, task=None):
        if task:
            self.__insert(task)

    def __len__(self):
        return len(self.tasks)

    def __insert(self, task):
        assert isinstance(task, Task), \
            "%r is not a Task" % task
        self.tasks.append(task)

    def append(self, task):
        self.__insert(task)

    def pop(self):
        return self.tasks.pop()


class Task(object):
    url_target = None
    url_source = None
    lvl_depth = 0

    def __init__(self, url_target, url_source=None, lvl_depth=0):
        if url_source:
            self.url_source = url_source
        else:
            self.url_source = url_target
        self.url_target = url_target
        self.lvl_depth = lvl_depth

    def __str__(self):
        return "[{0}] Source: '{1}', Destination: '{2}'".format(
            self.lvl_depth,
            self.url_source,
            self.url_target
        )


class Worker:
    start_url = None
    domain_mask = None
    blacklist = None
    threads_queue = None
    max_depth = 0
    max_threads = 4
    known_urls = []
    external_urls = []

    def __init__(self, start_url, blacklist, domain_mask, max_depth=2):
        self.start_url = start_url
        self.domain_mask = domain_mask
        self.max_depth = max_depth
        self.blacklist = blacklist
        self.threads_queue = Queue()
        TasksList(Task(url_target=start_url))

    def do_work(self):
        while True:
            task = self.threads_queue.get()
            self.known_urls.append(task.url_target)

            if task.lvl_depth >= self.max_depth:
                self.threads_queue.task_done()
                continue

            time.sleep(.1)
            try:
                mparser = PageParser(task, self.domain_mask)
                all_links = mparser.parse('a')
            except:
                # TODO: print error message to stderr using threading.Lock()
                self.threads_queue.task_done()
                continue

            for link in all_links:
                if not re.match(self.domain_mask, link):
                    self.external_urls.append(
                        Task(link, task.url_target, task.lvl_depth + 1)
                    )
                elif link not in self.known_urls and \
                        not re.match(self.blacklist, link):
                    new_task = Task(
                        link, task.url_target, task.lvl_depth + 1
                    )
                    tasks_list = TasksList()
                    tasks_list.append(new_task)
            self.threads_queue.task_done()

    def find_external_links(self):
        for i in range(self.max_threads):
            t = threading.Thread(target=self.do_work)
            t.daemon = True
            t.start()

        processed_links = 0
        tasks_list = TasksList()
        lock = threading.Lock()
        print("Processing links: ", end='', file=sys.stderr, flush=True)
        while len(tasks_list):
            counter = 0
            while len(tasks_list) > 0 and counter < self.max_threads:
                self.threads_queue.put(tasks_list.pop())
                counter += 1
            processed_links += 1
            if processed_links % 10:
                print(".", end='', file=sys.stderr, flush=True)
            self.threads_queue.join()
        print(file=sys.stderr, flush=True)
        return self.external_urls
