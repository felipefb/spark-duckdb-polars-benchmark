"""Reproducible local benchmark. Python 3.11+, Java 17+ for Spark.
Generate: python benchmark.py generate --rows 1000000
Run:      python benchmark.py run --repeats 3 --threads 4
Validate: python benchmark.py validate
"""
import argparse, csv, json, os, platform, statistics, subprocess, sys, time
from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parent
DATA = ROOT/'data'; OUT = ROOT/'results'
QUERIES = ('q1','q2','q3','q4')
SQL = {
'q1': "SELECT store_id, status, COUNT(*) AS n, SUM(amount_cents) AS total_cents FROM sales WHERE day_id >= 180 AND status = 'paid' GROUP BY 1,2",
'q2': "SELECT p.category, COUNT(*) AS n, SUM(s.amount_cents) AS total_cents FROM sales s JOIN products p ON s.product_id=p.product_id WHERE s.status <> 'cancelled' GROUP BY 1",
'q3': "SELECT customer_id, order_id, day_id, amount_cents FROM (SELECT customer_id,order_id,day_id,amount_cents,ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY day_id DESC,order_id DESC) AS rn FROM sales) t WHERE rn=1",
'q4': "SELECT customer_id, COUNT(*) AS n, SUM(amount_cents) AS total_cents FROM sales GROUP BY 1"
}
COLUMNS = {'q1':['store_id','status','n','total_cents'],'q2':['category','n','total_cents'],'q3':['customer_id','order_id','day_id','amount_cents'],'q4':['customer_id','n','total_cents']}

def generate(rows):
    DATA.mkdir(exist_ok=True)
    con=duckdb.connect()
    # No random(): arithmetic deterministic across machines; exact integer currency.
    con.execute("COPY (SELECT i::BIGINT AS product_id, ('category_' || CAST(i%20 AS VARCHAR)) AS category FROM range(1,1001) t(i)) TO '"+(DATA/'products.parquet').as_posix()+"' (FORMAT PARQUET)")
    con.execute("COPY (SELECT i::BIGINT AS order_id, (i%GREATEST(1,CAST(?/10 AS BIGINT))+1)::BIGINT AS customer_id, (i%100+1)::INTEGER AS store_id, (i%1000+1)::BIGINT AS product_id, (i%365)::INTEGER AS day_id, CASE WHEN i%11=0 THEN 'cancelled' WHEN i%7=0 THEN 'pending' ELSE 'paid' END AS status, ((i*37)%100000+100)::BIGINT AS amount_cents FROM range(1,?+1) t(i)) TO '"+(DATA/'sales.parquet').as_posix()+"' (FORMAT PARQUET, ROW_GROUP_SIZE 122880)",[rows,rows])
    (DATA/'manifest.json').write_text(json.dumps({'rows':rows,'generator':'deterministic modular arithmetic','currency':'integer cents','products':1000},indent=2))
    print('Generated',rows,'sales rows')

def engine_query(engine,query,threads):
    if engine=='duckdb':
        con=duckdb.connect()
        con.execute(f'SET threads={threads}')
        con.execute(f"CREATE VIEW sales AS SELECT * FROM read_parquet('{DATA/'sales.parquet'}')")
        con.execute(f"CREATE VIEW products AS SELECT * FROM read_parquet('{DATA/'products.parquet'}')")
        return con.execute(SQL[query]).fetchall()
    if engine=='spark':
        from pyspark.sql import SparkSession
        spark=(SparkSession.builder.master(f'local[{threads}]').appName('engine-bench').config('spark.sql.shuffle.partitions',str(threads)).config('spark.ui.enabled','false').getOrCreate())
        spark.sparkContext.setLogLevel('ERROR')
        spark.read.parquet(str(DATA/'sales.parquet')).createOrReplaceTempView('sales')
        spark.read.parquet(str(DATA/'products.parquet')).createOrReplaceTempView('products')
        result=[tuple(r) for r in spark.sql(SQL[query]).collect()]
        spark.stop()
        return result
    if engine=='polars':
        import polars as pl
        s=pl.scan_parquet(str(DATA/'sales.parquet'))
        p=pl.scan_parquet(str(DATA/'products.parquet'))
        if query=='q1':
            x=s.filter((pl.col('day_id')>=180)&(pl.col('status')=='paid')).group_by(['store_id','status']).agg(pl.len().alias('n'),pl.col('amount_cents').sum().alias('total_cents'))
        elif query=='q2':
            x=s.filter(pl.col('status')!='cancelled').join(p,on='product_id').group_by('category').agg(pl.len().alias('n'),pl.col('amount_cents').sum().alias('total_cents'))
        elif query=='q3':
            # Deterministic tie-breaking matches ROW_NUMBER day_id DESC, order_id DESC.
            x=s.sort(['customer_id','day_id','order_id'],descending=[False,True,True]).unique(subset=['customer_id'],keep='first',maintain_order=True).select(COLUMNS[query])
        else:
            x=s.group_by('customer_id').agg(pl.len().alias('n'),pl.col('amount_cents').sum().alias('total_cents'))
        return list(x.select(COLUMNS[query]).collect().iter_rows())
    raise ValueError(engine)

def output_path(engine,query): return OUT/'outputs'/f'{engine}_{query}.csv'
def write_output(engine,query,rows):
    path=output_path(engine,query);path.parent.mkdir(parents=True,exist_ok=True)
    # Exact integer/string results; sorted so output is order-independent.
    with path.open('w',newline='') as f:
        writer=csv.writer(f);writer.writerow(COLUMNS[query]);writer.writerows(sorted(rows,key=lambda row:tuple(str(v) for v in row)))

def worker(engine,query,threads):
    start=time.perf_counter();rows=engine_query(engine,query,threads);elapsed=time.perf_counter()-start
    write_output(engine,query,rows)
    print(json.dumps({'engine':engine,'query':query,'seconds':elapsed,'output_rows':len(rows)}))

def measure(engine,query,threads,repeat):
    import psutil
    cmd=[sys.executable,str(Path(__file__).resolve()),'worker','--engine',engine,'--query',query,'--threads',str(threads)]
    env=os.environ.copy();env['POLARS_MAX_THREADS']=str(threads)
    t0=time.perf_counter();p=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,env=env)
    peak=0
    while p.poll() is None:
        try:
            proc=psutil.Process(p.pid)
            children=[proc]+proc.children(recursive=True)
            rss=sum(x.memory_info().rss for x in children if x.is_running())
            peak=max(peak,rss)
        except (psutil.Error,ProcessLookupError): pass
        time.sleep(.05)
    stdout,stderr=p.communicate();wall=time.perf_counter()-t0
    record={'engine':engine,'query':query,'repeat':repeat,'wall_seconds':round(wall,4),'peak_process_tree_rss_mb':round(peak/1048576,2),'status':'PASS' if p.returncode==0 else 'ERROR'}
    if p.returncode==0:
        record.update(json.loads([l for l in stdout.strip().splitlines() if l.startswith('{')][-1]))
    else:record['error']=stderr[-2000:]
    print(record);return record

def run(repeats,threads):
    if not (DATA/'sales.parquet').exists():raise SystemExit('Run generate first')
    OUT.mkdir(exist_ok=True)
    meta={'python':sys.version,'platform':platform.platform(),'cpu_count':os.cpu_count(),'threads':threads,'repeats':repeats,'versions':{}}
    import polars,pyspark,psutil
    meta['versions']={'duckdb':duckdb.__version__,'polars':polars.__version__,'pyspark':pyspark.__version__,'psutil':psutil.__version__}
    (OUT/'environment.json').write_text(json.dumps(meta,indent=2))
    records=[]
    for query in QUERIES:
        for rep in range(repeats):
            for engine in (['duckdb','polars','spark'][rep%3:]+['duckdb','polars','spark'][:rep%3]):
                records.append(measure(engine,query,threads,rep))
    with (OUT/'runs.csv').open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=['engine','query','repeat','wall_seconds','peak_process_tree_rss_mb','status','seconds','output_rows','error']);writer.writeheader();writer.writerows(records)
    validate()

def canonical(path):
    with path.open(newline='') as f:
        r=csv.DictReader(f)
        return sorted(tuple(row[c] for c in r.fieldnames) for row in r)

def validate():
    checks=[]
    for q in QUERIES:
        reference=output_path('duckdb',q)
        if not reference.exists():raise SystemExit('Reference missing: '+str(reference))
        expected=canonical(reference)
        for engine in ['duckdb','polars','spark']:
            path=output_path(engine,q)
            ok=path.exists() and canonical(path)==expected
            checks.append({'query':q,'engine':engine,'correct':ok,'reference_rows':len(expected)})
    # Independent mathematical invariants, not just agreement with DuckDB.
    con=duckdb.connect()
    sales=str(DATA/'sales.parquet'); products=str(DATA/'products.parquet')
    n=con.execute('SELECT COUNT(*) FROM read_parquet(?)',[sales]).fetchone()[0]
    expected_n=json.loads((DATA/'manifest.json').read_text())['rows']
    invariants={'input_count':n==expected_n,'q3_unique_customers':len(canonical(output_path('duckdb','q3')))==con.execute('SELECT COUNT(DISTINCT customer_id) FROM read_parquet(?)',[sales]).fetchone()[0], 'q4_sum_counts':sum(int(x[1]) for x in canonical(output_path('duckdb','q4')))==n,'q2_join_count':sum(int(x[1]) for x in canonical(output_path('duckdb','q2')))==con.execute("SELECT COUNT(*) FROM read_parquet(?) WHERE status <> 'cancelled'",[sales]).fetchone()[0]}
    report={'engine_equivalence':checks,'invariants':invariants,'all_pass':all(x['correct'] for x in checks) and all(invariants.values())}
    OUT.mkdir(exist_ok=True);(OUT/'validation.json').write_text(json.dumps(report,indent=2))
    print(json.dumps(report,indent=2))
    if not report['all_pass']:raise SystemExit('VALIDATION FAILED: do not rank engines')

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('action',choices=['generate','run','worker','validate']);parser.add_argument('--rows',type=int,default=1000000);parser.add_argument('--repeats',type=int,default=3);parser.add_argument('--threads',type=int,default=4);parser.add_argument('--engine',choices=['duckdb','polars','spark']);parser.add_argument('--query',choices=QUERIES);args=parser.parse_args()
    if args.action=='generate':generate(args.rows)
    elif args.action=='run':run(args.repeats,args.threads)
    elif args.action=='worker':worker(args.engine,args.query,args.threads)
    else:validate()
