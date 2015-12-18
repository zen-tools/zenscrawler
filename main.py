#!/usr/bin/env python3
from zenscrawler.worker import Worker


worker = Worker(
    start_url='https://www.iis.se/',
    domain_mask='.*iis\.se.*',
    blacklist='(^mailto:)',
    max_depth=2
)

for i in worker.find_external_links():
    print(i)
