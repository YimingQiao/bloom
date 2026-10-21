#!/usr/bin/env python3
"""Same-binary differential correctness and native-runner benchmark for L/C."""
import argparse, collections, csv, gzip, hashlib, io, json, math, os, platform, random, re, statistics, subprocess, time
from pathlib import Path
ROOT=Path('/home/yiming/projects/bloom-key-lineage')
DATA=Path('/home/yiming/projects/bloomsea')
THEME=Path(__file__).resolve().parents[1]
ENV={k:v for k,v in os.environ.items() if not k.startswith('BLOOM_')}
VARIANTS=('duckdb','legacy','lc')

def settings(v, log=False):
    return ("SET threads=1; SET memory_limit='16GB'; SET bloom_memory_limit='4GB';\n"
            "SET bloom_sample_mode='instant'; SET bloom_sample_seed=2;\n"
            f"SET enable_bloom={'false' if v=='duckdb' else 'true'}; "
            f"SET bloom_key_lineage={'true' if v=='lc' else 'false'}; "
            f"SET bloom_log_transfer_steps={'true' if log else 'false'};\n")

def invoke(cmd, sql=None, timeout=180):
    return subprocess.run([str(x) for x in cmd], input=sql, text=True, capture_output=True,
                          env=ENV, cwd=ROOT, timeout=timeout)

def cli(sql, db=':memory:', timeout=180):
    return invoke([ROOT/'build/release/duckdb',*(['-readonly'] if str(db)!=':memory:' else []),'-batch','-bail','-csv','-noheader',
                   '-nullvalue','__LC_NULL__',db],sql,timeout)

def bag(stdout):
    return collections.Counter(tuple(row) for row in csv.reader(io.StringIO(stdout)))

def equivalent(a,b):
    if a==b: return True
    # Parallel aggregate/reordering can change last floating bits; require
    # identical row multiplicity and all other cells, with a tight tolerance.
    aa=sorted(a.elements()); bb=sorted(b.elements())
    if len(aa)!=len(bb): return False
    for ar,br in zip(aa,bb):
        if len(ar)!=len(br): return False
        for x,y in zip(ar,br):
            if x==y: continue
            if re.fullmatch(r'[+-]?\d+',x) and re.fullmatch(r'[+-]?\d+',y): return False
            try:
                if math.isclose(float(x),float(y),rel_tol=1e-10,abs_tol=1e-10): continue
            except ValueError: pass
            return False
    return True

def metrics(log):
    builds=re.findall(r'\[Bloom-FilterBuildTiming\] rows=(\d+) specs=(\d+)',log)
    compact=re.findall(r'\[Bloom-Compact\] input=(\d+) output=(\d+)',log)
    initial=re.findall(r'\[Materialize\] rows=(\d+)',log)
    return dict(builds=sum(int(x[1]) for x in builds),build_batches=len(builds),
                build_input_rows=sum(int(x[0]) for x in builds),
                materialized_rows=sum(map(int,initial))+sum(int(x[1]) for x in compact),
                compact_input_rows=sum(int(x[0]) for x in compact),
                compact_output_rows=sum(int(x[1]) for x in compact),
                transfers=log.count('[Bloom-Transfer]'),key_commits=log.count('[Bloom-KeyLineage]'),
                memory_stops=log.count('[Bloom-Memory] transfer stopped'))

def save(path,value): path.write_text(json.dumps(value,indent=2)+'\n')
def hashfile(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''): h.update(block)
    return h.hexdigest()

def cases():
    setup=''
    for name,size in [('a',100),('b',180),('c',240),('d',300),('e',400)]:
        setup+=f'CREATE TABLE {name} AS SELECT (i%31)::INTEGER x, (i%17)::INTEGER y, (i%11)::INTEGER z, i id FROM range({size}) t(i);\n'
    graphs={
      'chain_cross_key':'a JOIN b ON a.x=b.x JOIN c ON b.y=c.y JOIN d ON c.z=d.z',
      'star':'a JOIN b ON a.x=b.x JOIN c ON a.y=c.y JOIN d ON a.z=d.z',
      'key_inclusion':'a JOIN b ON a.x=b.x AND a.y=b.y JOIN c ON b.x=c.x JOIN d ON c.y=d.y',
      'overlapping_composite':'a JOIN b ON a.x=b.x AND a.y=b.y JOIN c ON b.y=c.y AND b.z=c.z JOIN d ON c.x=d.x',
      'five_table':'a JOIN b ON a.x=b.x JOIN c ON b.x=c.x AND b.y=c.y JOIN d ON c.z=d.z JOIN e ON d.y=e.y',
      'cycle':'a JOIN b ON a.x=b.x JOIN c ON b.y=c.y AND a.z=c.z',
      'same_key':'a JOIN b ON a.x=b.x JOIN c ON b.x=c.x',
      'self_join':'a JOIN a b ON a.x=b.y JOIN c ON b.z=c.z',
    }
    out=[]
    for name,graph in graphs.items():
        out.append((name,setup,f'SELECT a.id,b.id,c.id FROM {graph} WHERE a.id<7 AND c.id<30'))
        out.append((name+'_null',setup+'UPDATE a SET x=NULL WHERE id%3=0; UPDATE b SET y=NULL WHERE id%4=0;',f'SELECT a.id,b.id,c.id FROM {graph} WHERE a.id<7 AND c.id<30'))
    out+= [('empty',setup+'DELETE FROM a;', 'SELECT a.id,b.id,c.id FROM a JOIN b ON a.x=b.x JOIN c ON b.y=c.y'),
           ('no_shrink',setup,'SELECT a.id FROM a JOIN b ON a.x=b.x JOIN c ON b.x=c.x WHERE a.id<2'),
           ('cte_alias',setup,'WITH t AS MATERIALIZED (SELECT * FROM a WHERE id<40) SELECT p.id,q.id FROM t p JOIN t q ON p.x=q.y JOIN c ON q.z=c.z WHERE c.id<5'),
           ('outer_guard',setup,'SELECT a.id,b.id FROM a LEFT JOIN b ON a.x=b.x AND a.y=b.y WHERE a.id<5')]
    rng=random.Random(21)
    for n in range(50):
        sql=''
        nt=3+n%3
        for t in range(nt):
            rows=[tuple(rng.choice([None,*range(5)]) for _ in range(3)) for _ in range(12)]
            vals=','.join('('+','.join('NULL' if x is None else str(x) for x in row)+')' for row in rows+rows[:2])
            sql+=f'CREATE TABLE t{t}(x INTEGER,y INTEGER,z INTEGER); INSERT INTO t{t} VALUES {vals};\n'
        graph='t0'
        for t in range(1,nt):
            p=rng.randrange(t); keys=rng.sample(['x','y','z'],1+n%2)
            cond=' AND '.join(f't{p}.{k}=t{t}.{k}' for k in keys)
            graph+=f' JOIN t{t} ON {cond}'
        out.append((f'random_{n}',sql,f'SELECT t0.x,t0.y,t{nt-1}.z FROM {graph}'))
    return out

def check_fixtures(out):
    rows=[]; folder=out/'fixtures'; folder.mkdir(exist_ok=True)
    for name,setup,query in cases():
        baseline=None
        for v in VARIANTS:
            sql=settings('duckdb')+setup+settings(v,True)+query+';\n'
            r=cli(sql)
            with gzip.open(folder/f'{name}_{v}.log.gz','wt') as f:f.write(r.stdout+'\n'+r.stderr)
            (folder/f'{name}.sql').write_text(setup+query+';\n')
            if r.returncode: raise RuntimeError(f'{name}/{v}: {r.stderr[-2000:]}')
            actual=bag(r.stdout)
            if baseline is None: baseline=actual
            if not equivalent(baseline,actual): raise RuntimeError(f'Bag mismatch {name}/{v}')
            rows.append(dict(case=name,variant=v,result_rows=sum(actual.values()),**metrics(r.stderr)))
    save(out/'fixture-results.json',rows)
    print(f'PASS {len(cases())} fixtures x 3 variants',flush=True)

def workloads():
    stats=DATA/'.bench_cache/stats_ceb/assets/670cb8d4bf4cbfa32f94fdf17f33973d3fd67d1b/queries'
    return [('JOB',DATA/'.bench_cache/data/imdb.duckdb',DATA/'duckdb/benchmark/imdb_plan_cost/queries'),
            ('STATS-CEB',DATA/'.bench_cache/data/stats_ceb.duckdb',stats),
            ('TPC-H-SF1',DATA/'.bench_cache/data/tpch_sf1.duckdb',DATA/'duckdb/extension/tpch/dbgen/queries')]

def summarize(out,records):
    summaries=[]
    for bench,_,_ in workloads():
        rr=[r for r in records if r['benchmark']==bench]
        for v in ('legacy','lc'):
            xx=[r for r in rr if r['variant']==v and r.get('seconds')]
            summaries.append(dict(benchmark=bench,variant=v,queries=len(xx),
                sum_median_s=sum(r['median_s'] for r in xx),
                **{key:sum(r[key] for r in xx) for key in ['builds','materialized_rows','build_input_rows','compact_input_rows','transfers','memory_stops']}))
    save(out/'summary.json',summaries)

def benchmarks(args,out):
    path=out/'records.json'; records=json.loads(path.read_text()) if path.exists() else []
    done={(r['benchmark'],r['query'],r['variant']) for r in records}
    todo=[]
    for bench,db,directory in workloads():
        if args.benchmark and bench not in args.benchmark: continue
        for q in sorted(directory.glob('*.sql')):
            if args.query and q.stem not in args.query: continue
            todo.append((bench,db,q))
    random.Random(21).shuffle(todo)
    failure_path=out/'failures.json'
    failures=json.loads(failure_path.read_text()) if failure_path.exists() else []
    def failure(bench,query,variant,phase,message):
        failures.append(dict(benchmark=bench,query=query,variant=variant,phase=phase,message=message))
        save(failure_path,failures)
        print(f'INCOMPLETE {bench}/{query}/{variant}/{phase}: {message[:160]}',flush=True)
    native=out/'native'; logs=out/'logs'; logs.mkdir(exist_ok=True)
    for bench,db,q in todo:
        if all((bench,q.stem,v) in done for v in ('legacy','lc')): continue
        query=q.read_text().strip(); label=bench+'_'+q.stem
        try:
            ref=cli(settings('duckdb')+query,db,args.timeout)
        except subprocess.TimeoutExpired:
            failure(bench,q.stem,'duckdb','reference',f'timeout {args.timeout}s')
            continue
        if ref.returncode:
            failure(bench,q.stem,'duckdb','reference',ref.stderr)
            continue
        expected=bag(ref.stdout)
        with gzip.open(logs/f'{label}_duckdb.log.gz','wt') as f:f.write(ref.stdout+'\n'+ref.stderr)
        variants=['legacy','lc']; random.Random(label).shuffle(variants)
        for v in variants:
            if (bench,q.stem,v) in done: continue
            start=time.time()
            try:
                r=cli(settings(v,True)+query,db,args.timeout)
            except subprocess.TimeoutExpired:
                failure(bench,q.stem,v,'diagnostic',f'timeout {args.timeout}s')
                continue
            with gzip.open(logs/f'{label}_{v}.log.gz','wt') as f:f.write(r.stdout+'\n'+r.stderr)
            if r.returncode or not equivalent(expected,bag(r.stdout)):
                raise RuntimeError(f'Correctness failed {label}/{v}, exit={r.returncode}: {r.stderr[-1500:]}')
            root=native/v; bd=root/'benchmark'/bench; bd.mkdir(parents=True,exist_ok=True)
            name=bd/(q.stem+'.benchmark')
            name.write_text(f'name {label}\ngroup {bench}\n\nrequire bloom\n\nstorage transient\n\ninit\n'
                +f"ATTACH '{db.resolve()}' AS bench (READ_ONLY);\nUSE bench;\n"+settings(v)
                +'\nrun\n'+query+'\n')
            times=logs/f'{label}_{v}.times'
            cmd=[ROOT/'build/release/benchmark/benchmark_runner','--root-dir',root,
                 f'benchmark/{bench}/{q.stem}\\.benchmark','--threads=1','--timed-runs',str(args.runs),f'--out={times}']
            try:
                timed=invoke(cmd,timeout=args.timeout*(args.runs+1)+10)
            except subprocess.TimeoutExpired:
                failure(bench,q.stem,v,'timing','native runner total timeout')
                continue
            with gzip.open(logs/f'{label}_{v}.timing.log.gz','wt') as f:f.write(timed.stdout+'\n'+timed.stderr)
            try:
                durations=[float(x) for x in times.read_text().splitlines()] if times.exists() else []
            except ValueError:
                failure(bench,q.stem,v,'timing',timed.stderr[-2000:])
                continue
            if timed.returncode or len(durations)!=args.runs or any(not math.isfinite(x) for x in durations):
                failure(bench,q.stem,v,'timing',timed.stderr[-2000:])
                continue
            record=dict(benchmark=bench,query=q.stem,variant=v,query_sha256=hashlib.sha256(query.encode()).hexdigest(),
                correct=True,seconds=durations,median_s=statistics.median(durations),**metrics(r.stderr))
            records.append(record); save(path,records); summarize(out,records)
            print(f'{label}/{v}: {record["median_s"]:.6f}s, builds={record["builds"]}, materialized={record["materialized_rows"]}',flush=True)
    summarize(out,records)

def main():
    global ROOT
    ap=argparse.ArgumentParser();ap.add_argument('--root',type=Path,default=ROOT)
    ap.add_argument('--out',type=Path,required=True);ap.add_argument('--runs',type=int,default=3)
    ap.add_argument('--timeout',type=int,default=180);ap.add_argument('--fixtures',action='store_true')
    ap.add_argument('--benchmark',nargs='*');ap.add_argument('--query',nargs='*');ap.add_argument('--only-fixtures',action='store_true')
    args=ap.parse_args();ROOT=args.root.resolve();out=args.out.resolve();out.mkdir(parents=True,exist_ok=True)
    if (out/'manifest.json').exists():
        previous=json.loads((out/'manifest.json').read_text())
        if previous['binary_sha256']!=hashfile(ROOT/'build/release/duckdb') or previous['runner_sha256']!=hashfile(ROOT/'build/release/benchmark/benchmark_runner') or previous['timed_runs']!=args.runs:
            raise RuntimeError('Refusing to mix different binaries or repetition counts in one run directory')
    if not (out/'manifest.json').exists():
        save(out/'manifest.json',dict(root=str(ROOT),base=invoke(['git','rev-parse','HEAD']).stdout.strip(),
            diff_sha256=hashlib.sha256(invoke(['git','diff']).stdout.encode()).hexdigest(),
            binary_sha256=hashfile(ROOT/'build/release/duckdb'),runner_sha256=hashfile(ROOT/'build/release/benchmark/benchmark_runner'),
            platform=platform.platform(),threads=1,timed_runs=args.runs,warmups=1,
            settings={v:settings(v) for v in VARIANTS},
            databases=[dict(benchmark=b,path=str(p.resolve()),size=p.stat().st_size,mtime_ns=p.stat().st_mtime_ns) for b,p,q in workloads()]))
        (out/'source.patch').write_text(invoke(['git','diff']).stdout)
    if args.fixtures or args.only_fixtures: check_fixtures(out)
    if not args.only_fixtures: benchmarks(args,out)
if __name__=='__main__':main()
