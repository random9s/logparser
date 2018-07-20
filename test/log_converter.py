from threading import Thread
from time import sleep
import sys, time, json, urlparse, csv, datetime, gzip
import pygeoip

gi = pygeoip.GeoIP('/mnt/md0/GeoLiteCity.dat')

fname = str(sys.argv[1])
rate = 0
total = 0
quit = False
cache = dict()

def rateThread(a):
  global rate
  time.sleep(5)
  zero_count=0
  while not quit:
    if rate == 0:
      zero_count += 1
    else:
      zero_count = 0
    if zero_count == 5:
      return
    print(rate, total)
    rate = 0
    time.sleep(1)


def lineProccessor2(jmsg):
  global cache
  output = dict()



  ## SPLIT JSON LINE
  jmsg = json.loads(jmsg.split('SDK ~ 0]')[1])


  if 'event' in jmsg:
    if len(jmsg['event']) > 1:
      for e in jmsg['event']:
        if jmsg['event'][e] != '':
          output['event_'+str(e).lower()] = jmsg['event'][e]
    del jmsg['event']




  if 'REQUEST_URI' in jmsg:
    tmp = jmsg['REQUEST_URI'].split('?')
    output['request_uri'] = tmp[0]
    uri = urlparse.parse_qsl(tmp[1])
    for u in uri:
      output['uri_' + str(u[0]).lower()] = u[1]
    del jmsg['REQUEST_URI']



  if 'uri_key' in output:
    del output['uri_key']



  for key in jmsg:
    output[str(key).lower()] = jmsg[key]
  if 'event_ts' in output:
    output['event_ts'] = datetime.datetime.fromtimestamp(float(output['event_ts'])/1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')

//ParseIP ...



  if 'request_time_float' in output:
    output['request_time_float'] = datetime.datetime.fromtimestamp(float(output['request_time_float'])/1.0).strftime('%Y-%m-%d %H:%M:%S.%f')



  if 'uri_d' in output:
    output['uri_did'] = output['uri_d']
    del output['uri_d']
  if 'uri_dt' in output:
    output['uri_dm'] = output['uri_dt']
    del output['uri_dt']
  if 'uri_v' in output:
    output['uri_sv'] = output['uri_v']
    del output['uri_v']
  if 'uri_p' in output:
    output['uri_fv'] = output['uri_p']
    del output['uri_p']
  if 'uri_t' in output:
    output['uri_ts'] = output['uri_t']
    del output['uri_t']


  if 'event_key' in output:
    output['event_n'] = output['event_key']
    del output['event_key']



  if 'remote_addr' in output:
    if output['remote_addr'] in cache:
      output['geo_country'] = cache[output['remote_addr']][0]
      output['geo_city'] = cache[output['remote_addr']][1]
    else:
      match = gi.record_by_addr(output['remote_addr'])
      try:
        output['geo_country'] = match['country_name'].encode('utf-8')
      except:
        output['geo_country'] = None
      try:
        output['geo_city'] = match['city'].encode('utf-8')
      except:
        output['geo_city'] = None
      cache[output['remote_addr']] = [output['geo_country'], output['geo_city']]
  return output

thread = Thread(target = rateThread, args = (10, ))
thread.start()

fields = ['event_fc', 'http_user_agent', 'event_ori', 'event_uid', 'event_ord', 'uri_p', 'uri_did', 'uri_v', 'event_lc', 'uri_n', 'event_lf', 'uri_l', 'event_dr', 'event_sp', 'uri_tz', 'event_st', 'remote_addr', 'uri_av', 'event_rid', 'uri_access_token', 'uri_an', 'uri_app', 'request_time_float', 'event_res', 'uri_ov', 'uri_os', 'event_typ', 'uri_kv', 'event_ct', 'uri_sv', 'client_id', 'request_uri', 'event_vs', 'event_ps', 'event_ts', 'event_n', 'event_m', 'event_tc', 'uri_dm', 'uri_fv', 'event_tg', 'event_sn', 'uri_q', 'uri_appkey', 'uri_tmp', 'uri_length', 'uri_pretty', 'uri_uid', 'uri_title', 'uri_category', 'uri_id', 'event_sc', 'uri_f', 'geo_country', 'geo_city']

prev_out_fname = ''

with gzip.open(fname) as f:
  for line in f:
    fline = lineProccessor2(line)
    if total == 0:
      prev_out_fname = 'sdk-log-'+str(fline['request_time_float'].split(' ')[0].replace('-', '.'))+'.csv.gz'
      out = gzip.open(prev_out_fname, 'a')
    out_fname = 'sdk-log-'+str(fline['request_time_float'].split(' ')[0].replace('-', '.'))+'.csv.gz'
    if out_fname != prev_out_fname:
      out.close()
      out = gzip.open(out_fname, 'a')
      prev_out_fname = out_fname
    writer = csv.DictWriter(out, fieldnames=fields)
    rate += 1
    total += 1
    try:
      writer.writerow(fline)
    except ValueError as e:
      errfile = open('fields_not_found.txt', 'a')
      errfile.write(str(e))
      errfile.write(str(fname))
      errfile.write(str(fline))
      errfile.write('\n')
      errfile.close()
out.close()
quit = True
