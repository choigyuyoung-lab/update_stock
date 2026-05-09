Traceback (most recent call last):
  File "/opt/hostedtoolcache/Python/3.10.20/x64/lib/python3.10/site-packages/requests/models.py", line 978, in json
    return complexjson.loads(self.text, **kwargs)
  File "/opt/hostedtoolcache/Python/3.10.20/x64/lib/python3.10/json/__init__.py", line 346, in loads
    return _default_decoder.decode(s)
  File "/opt/hostedtoolcache/Python/3.10.20/x64/lib/python3.10/json/decoder.py", line 337, in decode
    obj, end = self.raw_decode(s, idx=_w(s, 0).end())
  File "/opt/hostedtoolcache/Python/3.10.20/x64/lib/python3.10/json/decoder.py", line 355, in raw_decode
    raise JSONDecodeError("Expecting value", s, err.value) from None
json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0)
During handling of the above exception, another exception occurred:
Traceback (most recent call last):
  File "/home/runner/work/update_stock/update_stock/update_master_db_kr.py", line 101, in <module>
    main()
  File "/home/runner/work/update_stock/update_stock/update_master_db_kr.py", line 51, in main
    token = get_kis_token()
  File "/home/runner/work/update_stock/update_stock/update_master_db_kr.py", line 23, in get_kis_token
    return res.json().get('access_token')
  File "/opt/hostedtoolcache/Python/3.10.20/x64/lib/python3.10/site-packages/requests/models.py", line 982, in json
    raise RequestsJSONDecodeError(e.msg, e.doc, e.pos)
requests.exceptions.JSONDecodeError: Expecting value: line 1 column 1 (char 0)
Error: Process completed with exit code 1.
