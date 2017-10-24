# -*- coding: utf-8 -*-
from __future__ import absolute_import

from collections import OrderedDict

from frontera import Backend
from frontera.core.components import States


class CommonBackend(Backend):
    """
    A simpliest possible backend, performing one-time crawl: if page was crawled once, it will not be crawled again.
    """
    component_name = 'Common Backend'

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    def frontier_start(self):
        self.metadata.frontier_start()
        self.queue.frontier_start()
        self.states.frontier_start()
        self.queue_size = self.queue.count()

    def frontier_stop(self):
        self.metadata.frontier_stop()
        self.queue.frontier_stop()
        self.states.frontier_stop()

    def add_seeds(self, seeds):
        # 为种子设置depth
        for seed in seeds:
            seed.meta[b'depth'] = 0
        # 添加种子到metadata
        self.metadata.add_seeds(seeds)
        # 将种子相关的state数据从持久化存储读到缓存中
        self.states.fetch([seed.meta[b'fingerprint'] for seed in seeds])
        # 设置种子的state，如果缓存中有种子的state数据则设置为此数据，否则设置为默认（即未抓取状态）
        self.states.set_states(seeds)
        # 将种子设置QUEUED状态，放在queue中等待被分配
        self._schedule(seeds)
        # 更新states中对应种子的state
        self.states.update_cache(seeds)

    # 处理一批请求，将没有抓取的的请求设置QUEUED状态，放在queue中等待被分配
    def _schedule(self, requests):
        batch = []
        queue_incr = 0
        for request in requests:
            # 状态为未抓取，错误的会被设置为QUEUED状态
            schedule = True if request.meta[b'state'] in [States.NOT_CRAWLED, States.ERROR, None] else False
            batch.append((request.meta[b'fingerprint'], self._get_score(request), request, schedule))
            if schedule:
                queue_incr += 1
                request.meta[b'state'] = States.QUEUED
        # 将schedule=True的请求写入到queue中，实际上是持久化到后端
        self.queue.schedule(batch)
        # 更新request在metadata中的评分
        self.metadata.update_score(batch)
        self.queue_size += queue_incr

    def _get_score(self, obj):
        return obj.meta.get(b'score', 1.0)

    # 获取一批要抓取的request，排序依据的是score和created_time（具体可配置，查看get_next_requests和_order_by）
    def get_next_requests(self, max_next_requests, **kwargs):
        partitions = kwargs.pop('partitions', [0])  # TODO: Collect from all known partitions
        batch = []
        for partition_id in partitions:
            batch.extend(self.queue.get_next_requests(max_next_requests, partition_id, **kwargs))
        self.queue_size -= len(batch)
        return batch

    def page_crawled(self, response):
        response.meta[b'state'] = States.CRAWLED
        # 更新states中对应request的state
        self.states.update_cache(response)
        # 将response信息更新到metadata中的request中
        self.metadata.page_crawled(response)

    def links_extracted(self, request, links):
        to_fetch = OrderedDict()
        # 设置links的depth
        for link in links:
            to_fetch[link.meta[b'fingerprint']] = link
            link.meta[b'depth'] = request.meta.get(b'depth', 0) + 1
        # 将links相关的state数据从持久化存储读到缓存中
        self.states.fetch(to_fetch.keys())
        # 设置links的state，如果缓存中有links的state数据则设置为此数据，否则设置为默认（即未抓取状态）
        self.states.set_states(links)
        unique_links = to_fetch.values()
        # 将links存储到metadata，可能是update或者create
        self.metadata.links_extracted(request, unique_links)
        # 将links设置QUEUED状态，放在queue中等待被分配
        self._schedule(unique_links)
        # 更新states中对应links的state
        self.states.update_cache(unique_links)

    def request_error(self, request, error):
        request.meta[b'state'] = States.ERROR
        self.metadata.request_error(request, error)
        self.states.update_cache(request)

    def finished(self):
        return self.queue_size == 0
