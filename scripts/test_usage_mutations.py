#!/usr/bin/env python3
"""Run usage-index fault injection in an isolated source copy and target directory.

No files or build artifacts in the source checkout are mutated. The scratch
root must be below /Volumes/Data/tmp; dependencies are used offline.
"""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time

MUTATIONS=[
 ('M1','skip changed invocations','while !cold && position < new.invocation_revision {','while false && position < new.invocation_revision {'),
 ('M2','ignore estimate revisions','while !cold && position < new.estimate_rowid {','while false && position < new.estimate_rowid {'),
 ('M3','reprice without subtracting old contribution','Self::Event(event, sign) => state.event(*event, sign),','Self::Event(event, sign) => if sign < 0 { Ok(()) } else { state.event(*event, sign) },'),
 ('M4a','omit foreign-Agent attribution','.filter(|agent| Some(agent) != owner.as_ref());','.filter(|agent| Some(agent) != owner.as_ref()).filter(|_| false);'),
 ('M4b','foreign Agent receives all invocation events','.map(|e| e.attempt(self.lifecycle, self.telemetry))','.map(|_| self.events.attempt(self.lifecycle, self.telemetry))'),
 ('M5','event id sorts before source/attempt/invocation', 'struct SourceOrder {\n    source: Arc<str>,\n    ordinal: i64,\n    invocation: Arc<str>,\n    occurred: Arc<str>,\n    event: Arc<str>,\n}', 'struct SourceOrder {\n    event: Arc<str>,\n    source: Arc<str>,\n    ordinal: i64,\n    invocation: Arc<str>,\n    occurred: Arc<str>,\n}'),
 ('M5b','first provenance winner instead of last','.last_key_value()','.first_key_value()'),
 ('M6','double execution duration','duration: duration.map(|value| (value * DURATION_SCALE) as i128),','duration: duration.map(|value| (value * DURATION_SCALE * 2.0) as i128),'),
 ('M7','inquiry counted as chat','DbUsageSurface::MainInquiry => 2,','DbUsageSurface::MainInquiry => 1,'),
 ('M8','skip updated executions','while !cold && position <= new.execution_revision {','while false && position <= new.execution_revision {'),
 ('M9','ignore cascade deletion generation','.is_none_or(|old| old.deletion_generation != observed.deletion_generation)','.is_none()'),
 ('M11','unsettled counted as unmetered','i64::from(!metered && !pending && !unsettled),','i64::from(!metered && !pending),'),
 ('M12','unsettled retains priced tokens','let priced = if unsettled { [0; 4] } else { self.priced };','let priced = self.priced;'),
 ('M13','running execution without invocation loses pending coverage','t.active_domains = i64::from(self.domain == Some(true));','t.active_domains = 0;'),
 ('M14','reason run count equals attempt count','r.runs = i64::from(r.attempts > 0);','r.runs = r.attempts;'),
 ('M15','new events use old estimate watermark','for event in effective_events(connection, events, new.estimate_rowid).await? {','for event in effective_events(connection, events, old.estimate_rowid).await? {'),
 ('M16','revision delta also folds newly appended events','.bind(position).bind(new.estimate_rowid).bind(old.event_rowid).fetch_all','.bind(position).bind(new.estimate_rowid).bind(new.event_rowid).fetch_all'),
 ('M18','ownership move leaves old Agent domain run','            if old.owner.is_some() {\n                self.run_change((old.owner, old.run), None, None, Some(None))?;\n            }\n        } else {','        } else {'),
]


def mutation_sources(root: Path) -> dict[Path, str]:
    file = root/'crates/services/src/usage_projection/ledger_index.rs'
    files = [file, *sorted(file.with_suffix('').rglob('*.rs'))]
    return {path: path.read_text() for path in files}


def mutation_site(originals: dict[Path, str], identity: str, old: str) -> tuple[Path, int]:
    # Faults target production code; existing test assertions can contain the
    # same text (for example M5b's last_key_value call).
    sites = {path: text.count(old) for path, text in originals.items()
             if not path.stem.endswith('tests')}
    matches = sum(sites.values())
    expected = 2 if identity == 'M15' else 1
    if matches != expected:
        raise ValueError(f'{identity}: expected {expected} mutation site(s), found {matches}')
    holders = [path for path, count in sites.items() if count]
    if len(holders) != 1:
        # Base replaced the first site in one file; sites split across files
        # would make "first" depend on file order.
        raise ValueError(f'{identity}: mutation sites span {len(holders)} files')
    return holders[0], matches


def main()->None:
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--scratch',type=Path)
    p.add_argument('--dry-run',action='store_true',help='check mutation site counts without copying or running cargo')
    p.add_argument('--only',nargs='*')
    p.add_argument('--resume',action='store_true')
    p.add_argument('--no-debug-info',action='store_true')
    args=p.parse_args()
    source=Path(__file__).resolve().parents[1]
    if args.dry_run:
        originals = mutation_sources(source)
        errors = []
        for identity, _, old, _ in MUTATIONS:
            if args.only and identity not in args.only:continue
            try:
                file, matches = mutation_site(originals, identity, old)
            except ValueError as error:
                print(error)
                errors.append(str(error))
                continue
            print(f'{identity}: {matches} site(s); first in {file.relative_to(source)}')
            test_matches = sum(text.count(old) for path, text in originals.items()
                               if path.stem.endswith('tests'))
            if test_matches:print(f'  excluded {test_matches} occurrence(s) in test files')
        if errors:raise SystemExit(1)
        return
    if args.scratch is None:p.error('--scratch is required unless --dry-run is supplied')
    scratch=args.scratch.resolve()
    if not scratch.is_relative_to('/Volumes/Data/tmp') or scratch==source:
        raise ValueError('scratch must be a separate directory under /Volumes/Data/tmp')
    if scratch.exists() and not args.resume:raise ValueError('scratch must be a new directory unless --resume is supplied')
    root=scratch/'source';root.mkdir(parents=True,exist_ok=args.resume)
    names=subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard','-z'],cwd=source).split(b'\0')
    for name in names:
        if not name:continue
        path=source/os.fsdecode(name)
        if not path.is_file():continue
        destination=root/path.relative_to(source);destination.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(path,destination)
    target=root/'target'
    env=dict(os.environ,TMPDIR='/Volumes/Data/tmp',CARGO_TARGET_DIR=str(target),FORGE_SKIP_WEB_BUILD='1',RUSTC_WRAPPER='/usr/bin/env')
    if target.resolve()==(source/'target').resolve():raise ValueError('source target must never be used')
    if args.no_debug_info:
        env.update(CARGO_PROFILE_TEST_DEBUG='0',CARGO_PROFILE_DEV_DEBUG='0')
    originals=mutation_sources(root)
    # Warm the isolated target and establish that the copied test gate passes.
    command=['cargo','test','--offline','-p','services','--lib','usage_projection::ledger_index::tests::','--','--test-threads','4']
    with (scratch/'baseline.log').open('w') as log:
        result=subprocess.run(command,cwd=root,env=env,stdout=log,stderr=subprocess.STDOUT)
    if result.returncode:raise RuntimeError('copied baseline failed; see baseline.log')
    results=[]
    for identity,description,old,new in MUTATIONS:
        if args.only and identity not in args.only:continue
        file, _ = mutation_site(originals, identity, old)
        original = originals[file]
        file.write_text(original.replace(old,new,1));start=time.perf_counter()
        try:
            with (scratch/(identity+'.log')).open('w') as log:
                result=subprocess.run(command,cwd=root,env=env,stdout=log,stderr=subprocess.STDOUT)
            output=(scratch/(identity+'.log')).read_text()
            failed=re.findall(r'^test (\S+) \.\.\. FAILED$',output,re.M)
            compiled='error[' not in output and 'could not compile' not in output
            row={'id':identity,'mutation':description,'compiled':compiled,'caught':bool(failed),'failed_tests':failed,'seconds':round(time.perf_counter()-start,2)}
            results.append(row);print(json.dumps(row),flush=True)
            (scratch/'results.json').write_text(json.dumps(results,indent=2)+'\n')
        finally:file.write_text(original)
    if not all(row['compiled'] and row['caught'] for row in results):raise RuntimeError('a mutation survived or did not compile')

if __name__=='__main__':main()
