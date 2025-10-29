#!/usr/bin/env python

import time
import argparse
import logging
import libzfs
from prometheus_client import start_http_server, Summary
from prometheus_client.core import GaugeMetricFamily, REGISTRY
from prometheus_client.registry import Collector

LOG = logging.getLogger(__name__)

class ZFSCollector(Collector):
    def __init__(self, limit, exclude):
        self.exclude = set(exclude or [])
        self.limit   = limit
        self.duration_summary = Summary('zfs_collect_duration', 'Duration needed collecting metrics')

    def collect(self):
        pmetrics = ['size', 'allocated', 'freeing']
        dsmetrics = {
                'used_bytes': {
                    'desc': 'The amount of space consumed by this dataset and all its descendants, in bytes',
                    'getter': lambda d: d.properties['used'].parsed
                    },
                'usedbydataset_bytes': {
                    'desc': 'The amount of space used by this dataset itself, excluding descendants, in bytes',
                    'getter': lambda d: d.properties['usedbydataset'].parsed
                    },
                'available_bytes': {
                    'desc': 'The amount of space available to the dataset and all its children, in bytes',
                    'getter': lambda d: d.properties['available'].parsed
                    },
                'atime_enabled': {
                    'desc': 'Whether access time updates are enabled (1 = on, 0 = off)',
                    'getter': lambda d: 1 if d.properties.get('atime') and d.properties['atime'].parsed else 0,
                    },
                'creation_date': {
                    'desc': 'The time this dataset was created, in epoch',
                    'getter': lambda d: int(d.properties['creation'].rawvalue),
                    },
                'type': {
                    'desc': 'Dataset type: 0=filesystem, 1=volume, 2=snapshot, 3=bookmark',
                    'getter': lambda d: {'filesystem': 0, 'volume': 1, 'snapshot': 2, 'bookmark': 3}.get(d.properties['type'].parsed, -1),
                    }
                }
        volmetrics = {
                'volsize_bytes': {
                    'desc': 'logical size of the volume, in bytes',
                    'getter': lambda v: v.properties['volsize'].parsed
                    },
                }

        pools_metrics    = {}
        datasets_metrics = {}
        volumes_metrics  = {}

        start = time.time()

        pools    = libzfs.ZFS().pools
        datasets = libzfs.ZFS().datasets

        for p in pmetrics:
            pools_metrics[p] = GaugeMetricFamily(
                        'zfs_pool_{}_bytes'.format(p),
                        'ZFS Pool {} in bytes'.format(p),
                        labels=['pool']
                        )

        for d, meta in dsmetrics.items():
            datasets_metrics[d] = GaugeMetricFamily(
                    'zfs_dataset_{}'.format(d),
                    meta['desc'],
                    labels=['pool', 'dataset']
                    )

        for vol, meta in volmetrics.items():
            datasets_metrics[vol] = GaugeMetricFamily(
                    'zfs_dataset_{}'.format(vol),
                    meta['desc'],
                    labels=['pool', 'dataset']
                    )

        for pool in pools:
            for p in pmetrics:
                pools_metrics[p].add_metric([pool.name], pool.properties[p].parsed)

        for ds in datasets:
            depth = len(ds.name.split('/')) - 1
            if depth > self.limit:
                continue
            elif depth == self.limit:
                datasets_metrics['used_bytes'].add_metric([ds.pool.name, ds.name], dsmetrics['used_bytes']['getter'](ds))
            else:
                if ds.type.name == 'VOLUME':
                    for vol, meta in volmetrics.items():
                        datasets_metrics[vol].add_metric([ds.pool.name, ds.name], meta['getter'](ds))
                for d, meta in dsmetrics.items():
                    datasets_metrics[d].add_metric([ds.pool.name, ds.name], meta['getter'](ds))

        duration = time.time() - start
        self.duration_summary.observe(duration)
        for p in pools_metrics.values():
            yield p
        for d in datasets_metrics.values():
            yield d
        for v in volumes_metrics.values():
            yield v

def main(port, limit, addr, exclude):
    stop = False
    REGISTRY.register(ZFSCollector(limit, exclude))
    start_http_server(port, addr=addr)

    while not stop:
        print('still running...')
        time.sleep(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            prog='zfs_exporter.py',
            description='ZFS Exporter for Prometheus',
            add_help=True
            )
    parser.add_argument('-b', '--bind',    default='127.0.0.1', type=str)
    parser.add_argument('-p', '--port',    default=8000,        type=int)
    parser.add_argument('-l', '--limit',   default=2,           type=int)
    parser.add_argument('-x', '--exclude', default=[],          type=str, action='append')
    args = parser.parse_args()
    main(exclude=args.exclude, limit=args.limit, port=args.port, addr=args.bind)
