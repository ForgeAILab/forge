#!/usr/bin/env python3
"""Benchmark usage reads at idle and under ledger writes, sequentially or concurrently.

Only copied/prepared fixtures are written. SQLite admission/immutability guards
remain enabled. The pinned heavy profile is unchanged; estimated fixtures use
perf_fixture's separately named usage-estimated profile.
"""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor
import ctypes
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import sqlite3
import struct
import sys
import threading
import time
import uuid
from perf_bench import distribution
from perf_fixture import Server, LocalClient, PASSWORD, build, validate_database
from perf_usage_fixture import insert, pricing, revision, scale


def rss(pid: int) -> int:
    if sys.platform=='darwin':
        library=ctypes.CDLL('/usr/lib/libproc.dylib')
        buffer=ctypes.create_string_buffer(96)
        if library.proc_pidinfo(pid,4,0,buffer,len(buffer))!=96:
            raise OSError('proc_pidinfo(PROC_PIDTASKINFO) failed')
        return struct.unpack_from('QQ',buffer)[1]
    return int(Path(f'/proc/{pid}/statm').read_text().split()[1])*os.sysconf('SC_PAGE_SIZE')


def prepare(args: argparse.Namespace) -> None:
    if args.out.exists(): raise ValueError('--out must be a new directory')
    if args.estimated:
        build(args.binary.resolve(),args.out,args.port,'usage-estimated')
    else:
        shutil.copytree(args.fixture,args.out)
        with Server(args.binary.resolve(),args.out,args.port,args.out/'usage-migrate.log'): pass
    with sqlite3.connect(args.out/'forge.db') as db:
        db.execute('PRAGMA foreign_keys=ON')
        scale(db,args.scale)
        validate_database(db)
        db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    manifest=json.loads((args.out/'perf-fixture.json').read_text())
    manifest['usage_scale']=args.scale
    manifest['usage_estimated']=args.estimated or manifest.get('usage_estimated',manifest.get('profile')=='usage-estimated')
    (args.out/'perf-fixture.json').write_text(json.dumps(manifest,indent=2)+'\n')
    print(json.dumps({'fixture':str(args.out),'scale':args.scale,'estimated':args.estimated}))


class Writer:
    def __init__(self,path: Path):
        self.db=sqlite3.connect(path,check_same_thread=False)
        self.db.execute('PRAGMA foreign_keys=ON')
        self.db.row_factory=sqlite3.Row
        self.templates=[dict(row) for row in self.db.execute('SELECT * FROM usage_event WHERE rowid IN (SELECT MIN(rowid) FROM usage_event GROUP BY invocation_id) ORDER BY invocation_id')]
        self.sequences={row[0]:row[1]+1 for row in self.db.execute('SELECT invocation_id,MAX(report_sequence) FROM usage_event GROUP BY invocation_id')}
        self.lock=threading.Lock()
        self.ordinal=0
        self.estimated=bool(self.db.execute('SELECT COUNT(*) FROM cost_estimate_revision').fetchone()[0])
        if self.estimated:
            row=self.db.execute('SELECT r.catalog_snapshot_id,r.rate_revision_id,run.preview_id FROM cost_estimate_revision r JOIN cost_estimation_run run ON run.id=r.run_id LIMIT 1').fetchone()
            self.provenance=tuple(row)
        else:
            first=self.templates[0]
            self.provenance=pricing(self.db,first['owner_user_id'],first['project_id'])
            sample=first.copy();identity=str(uuid.uuid4())
            sample.update(id=identity,event_idempotency_key=identity,source_report_id=identity,report_sequence=self.sequences[first['invocation_id']],cost_kind='none',provider_reported_nano_usd=None,coverage_reason_code='missing_rate')
            self.sequences[first['invocation_id']]+=1
            if sample['legacy_source_id'] is not None:sample['legacy_source_id']=identity
            insert(self.db,'usage_event',sample)
            self.reprice_event=sample
        self.db.commit()
    def append(self) -> None:
        with self.lock:
            self.ordinal+=1
            template=self.templates[(self.ordinal-1)%len(self.templates)]
            row=template.copy();identity=str(uuid.uuid4())
            row.update(id=identity,event_idempotency_key=identity,source_report_id=identity,report_sequence=self.sequences[row['invocation_id']])
            self.sequences[row['invocation_id']]+=1
            if row['legacy_source_id'] is not None:row['legacy_source_id']=identity
            insert(self.db,'usage_event',row)
            if self.estimated:revision(self.db,row,self.provenance,self.ordinal)
            if self.ordinal%50==0:
                old=self.templates[(self.ordinal//50)%len(self.templates)] if self.estimated else self.reprice_event
                revision(self.db,old,self.provenance,self.ordinal+100_000)
            self.db.commit()
    def close(self)->None:self.db.close()


def run(args: argparse.Namespace) -> None:
    if args.out.exists():raise ValueError('--out must be a new file')
    manifest=json.loads((args.fixture/'perf-fixture.json').read_text())
    original=hashlib.sha256((args.fixture/'forge.db').read_bytes()).hexdigest()
    routes=[('operations status','/api/v1/operations/status'),('agents','/api/v1/agents?limit=100'),('project agents',f"/api/v1/projects/{manifest['project_id']}/agents")]
    results=[]
    root=args.out.parent/(args.out.stem+'-runs')
    root.mkdir(parents=True,exist_ok=False)
    for name,path in routes:
        data=root/name.replace(' ','-')/'data'
        shutil.copytree(args.fixture,data)
        log=data.parent/'server.log'
        # Writes are set up before starting the measured process, so the first
        # measured request is also the index's first read after startup.
        writer=Writer(data/'forge.db')
        with Server(args.binary.resolve(),data,args.port,log,log_filter="warn,services::usage_projection::ledger_index=info") as server:
            clients=[LocalClient(args.port) for _ in range(args.clients)]
            token=clients[0].json('/api/v1/auth/login',{'email':manifest['email'],'password':PASSWORD})['access_token']
            for client in clients:client.token=token
            assert server.process is not None
            before=rss(server.process.pid)
            start=time.perf_counter_ns();status,_,_=clients[0].request(path);cold=(time.perf_counter_ns()-start)/1e6
            if status!=200:raise ValueError(f'{name} cold HTTP {status}')
            after_cold=rss(server.process.pid)
            messages=re.sub(r"\x1b\[[0-9;]*m","",log.read_text())
            built=re.findall(r"usage index built.*?charge=(\d+)",messages)
            discarded=re.findall(r"usage index discarded.*?charge=(\d+)",messages)
            charge=int(built[-1]) if built else 0 if discarded else None
            rejected=int(discarded[-1]) if discarded else None
            for client in clients:
                for _ in range(10):client.request(path)
            for active in (False,True):
                phases=threading.Barrier(args.clients) if active and args.clients>1 else None
                def worker(client: LocalClient)->list[float]:
                    elapsed=[]
                    client.connection.timeout=180
                    if client.connection.sock is not None:
                        client.connection.sock.settimeout(180)
                    for _ in range(args.requests):
                        if active:writer.append()
                        # Each client commits one event between request rounds.
                        # All four then read concurrently, so timings measure
                        # the changed-ledger read rather than write-lock wait.
                        if phases:phases.wait(timeout=180)
                        start=time.perf_counter_ns();status,_,_=client.request(path);elapsed.append((time.perf_counter_ns()-start)/1e6)
                        if status!=200:raise ValueError(f'{name} HTTP {status}')
                        if phases:phases.wait(timeout=180)
                    return elapsed
                with ThreadPoolExecutor(max_workers=args.clients) as pool:
                    elapsed=[value for sample in pool.map(worker,clients) for value in sample]
                result={'scenario':name,'clients':args.clients,'active':active,'requests':len(elapsed),'latency_ms':distribution(elapsed),'cold_ms':round(cold,3),'rss_before_bytes':before,'rss_after_cold_bytes':after_cold,'index_charged_bytes':charge,'index_rejected_charge_bytes':rejected,'rss_after_bytes':rss(server.process.pid)}
                results.append(result);print(json.dumps(result),flush=True)
            for client in clients:client.close()
        writer.close()
    assert hashlib.sha256((args.fixture/'forge.db').read_bytes()).hexdigest()==original
    with sqlite3.connect(f'file:{args.fixture}/forge.db?mode=ro',uri=True) as db:
        counts={table:db.execute('SELECT COUNT(*) FROM '+table).fetchone()[0] for table in ('execution','usage_invocation','usage_event','cost_estimate_revision')}
    report={'binary':str(args.binary),'fixture':str(args.fixture),'counts':counts,'scale':manifest.get('usage_scale',1),'estimated':manifest.get('usage_estimated',manifest.get('profile')=='usage-estimated'),'writes':'Raw SQL in copied fixtures through all production guards; one event per request, rotating invocations; estimated events get an applied revision, and an old event is repriced every 50 writes. Writes excluded from read timings; four-client rounds commit one event per client before concurrent reads.','results':results}
    args.out.write_text(json.dumps(report,indent=2)+'\n')


def main()->None:
    parser=argparse.ArgumentParser(description=__doc__)
    commands=parser.add_subparsers(dest='command',required=True)
    for command in ('prepare','run'):
        sub=commands.add_parser(command)
        sub.add_argument('--binary',type=Path,required=True);sub.add_argument('--fixture',type=Path,required=True);sub.add_argument('--out',type=Path,required=True);sub.add_argument('--port',type=int,default=18237)
        if command=='prepare':sub.add_argument('--scale',type=int,default=1);sub.add_argument('--estimated',action='store_true')
        else:sub.add_argument('--requests',type=int,default=100);sub.add_argument('--clients',type=int,choices=(1,4),default=1)
    args=parser.parse_args()
    (prepare if args.command=='prepare' else run)(args)

if __name__=='__main__':main()
