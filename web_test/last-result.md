# web test results (v2 — dashboard)

| check | status | ms | type | result |
|---|---|---|---|---|
| landing page | 200 PASS | 577 | text/html | <!DOCTYPE html> <html lang="en"> <head> <meta charset="UTF-8"> <meta name="viewport" content="width=device-width, initial-scale=1.0"> <meta name="them |
| owner dashboard page | 200 PASS | 563 | text/html | <!doctype html> <html lang="en" data-theme="onyx"> <head> <meta charset="utf-8"> <meta name="viewport" content="width=device-width,initial-scale=1,vie |
| dashboard page script-UA blocked | 403 PASS | 195 | text/plain | forbidden |
| heartbeat /_ping | 404 FAIL | 300 | application/json | {"detail":"Not Found"} |
| state without session | 401 PASS | 437 | application/json | {"error": "unauthorized"} |
| action without session | 401 PASS | 283 | application/json | {"error": "unauthorized"} |
| login script-UA blocked | 403 PASS | 151 | application/json | {"error": "forbidden"} |
| login wrong pass | 401 PASS | 848 | application/json | {"error": "wrong password"} |
| login right pass (datacenter shield) | 200 PASS | 846 | application/json | {"ok": true} |
| file manager page | 200 PASS | 361 | text/html | <!DOCTYPE html> <html lang="en"> <head> <meta charset="UTF-8"> <meta name="viewport" content="width=device-width, initial-scale=1.0"> <meta name="them |
| pin api no gid | 200 PASS | 473 | application/json | {"files":[],"engine":"","error":"GID is missing","message":"GID not specified"} |
| qbit ui valid pass | 200 PASS | 510 | text/html | <!DOCTYPE html> <html lang="C"> <head> <meta charset="UTF-8" /> <meta http-equiv="X-UA-Compatible" content="IE=10" /> <meta name="application-name" co |
| qbit api valid pass | 200 PASS | 233 | text/plain | v4.4.1 |
| qbit wrong pass | 403 PASS | 264 | text/html | <h1>403: Unauthorized access</h1> |
| nzb ui valid pass (known broken) | 500 PASS | 381 | text/html | <h1>500: Internal server error</h1> |
| nzb wrong pass | 403 PASS | 244 | text/html | <h1>403: Unauthorized access</h1> |
| stream bogus token | 404 PASS | 432 | text/plain | unknown link |
| download bogus token | 404 PASS | 371 | text/plain | unknown link |
| unknown page 404 | 404 PASS | 251 | application/json | {"detail":"Not Found"} |

**18 pass / 1 fail / 0 error**
