import json
from pathlib import Path
from collections import defaultdict,Counter
from bisect import bisect_left
from decimal import Decimal as D
p=Path('Z_ORB_ONE/analysis_strategy/analysis_json_cache/analysis_strategy_broken_each_stock_special_short_api_cache_xxxxxxxx.json')
data=json.loads(p.read_text(encoding='utf-8'))
daily={s:{r['date'][:10]:r for r in rs} for s,rs in data['day_candles_by_symbol'].items()}
minutes={}; dates=set()
for s,rs in data['minute_raw_by_symbol'].items():
 groups=defaultdict(dict)
 for r in rs:
  dt=r['date']; t=dt[11:19]
  if '09:00:00'<=t<='13:30:00':groups[dt[:10]][dt]=r;dates.add(dt[:10])
 minutes[s]={dt:sorted(v.values(),key=lambda r:r['date']) for dt,v in groups.items()}
for rs in daily.values():dates.update(rs)
dates=sorted(dates)
rows=[]; skip=Counter();sources=Counter();first_times=Counter()
for s,groups in minutes.items():
 for date,bars in sorted(groups.items()):
  i=bisect_left(dates,date)
  if i==0:skip['no_previous_date']+=1;continue
  prevdate=dates[i-1]; prev=daily.get(s,{}).get(prevdate);source='daily'
  if prev is None:
   prevbars=groups.get(prevdate,[])
   prev=next((b for b in reversed(prevbars) if b['date'][11:19]=='13:30:00'),None);source='minute_1330'
  if prev is None or not prev['close']:skip['no_previous_close']+=1;continue
  sources[source]+=1
  first=bars[0];f=D(str(first['close']));pc=D(str(prev['close']));change=(f/pc-1)*100
  group='up_gt_3' if f>pc*D('1.03') else 'down_lt_minus3' if f<pc*D('0.97') else 'other'
  first_times[first['date'][11:19]]+=1
  row=dict(symbol=s,date=date,previous_date=prevdate,previous_close=float(pc),previous_close_source=source,first_time=first['date'],first_close=float(f),first_change_pct=float(change),group=group)
  for target in ['09:30','10:00','10:30','11:00','12:30']:
   b=next((b for b in bars if b['date'][11:16]>=target),None)
   row[target]=None if b is None else dict(time=b['date'],close=b['close'],relative_to_first='above' if D(str(b['close']))>f else 'below' if D(str(b['close']))<f else 'equal',shifted=b['date'][11:16]!=target)
  rows.append(row)
summary={}
for group in ['up_gt_3','down_lt_minus3']:
 rr=[r for r in rows if r['group']==group]; out={'samples':len(rr),'symbols':len(set(r['symbol'] for r in rr))}
 for t in ['09:30','10:00','10:30','11:00','12:30']:
  c=Counter(r[t]['relative_to_first'] if r[t] else 'missing' for r in rr);c['shifted']=sum(bool(r[t] and r[t]['shifted']) for r in rr);out[t]=dict(c)
 want='above' if group=='up_gt_3' else 'below'
 eligible=[r for r in rr if r['09:30'] and r['12:30']]
 out['both_valid']=len(eligible);out['both_direction']=sum(all(r[t]['relative_to_first']==want for t in ['09:30','12:30']) for r in eligible)
 summary[group]=out
report=dict(source=str(p),date_range=[min(r['date'] for r in rows),max(r['date'] for r in rows)],total_symbol_days=sum(len(g) for g in minutes.values()),valid_symbol_days=len(rows),skipped=dict(skip),previous_close_sources=dict(sources),first_times=dict(first_times),summary=summary,rows=rows)
Path('analysis_output/first_bar_3pct_analysis.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
print(json.dumps({k:v for k,v in report.items() if k!='rows'},ensure_ascii=True,indent=2))

